import boto3
import time
import requests
import boto3.dynamodb.conditions

s3 = boto3.resource("s3")
client1 = boto3.client("s3")
url = "https://s3-us-west-2.amazonaws.com/css490/input.txt"
bucket_name = 'programfourestorage'
key = 'input.txt'
dbName = 'programfourestoragetable'
my_list = []

def read_data_upload_s3():
    r = requests.get(url,stream=True)

    session = boto3.Session()
    s3Obj = session.resource('s3')

    bucket = s3Obj.Bucket(bucket_name)
    bucket.upload_fileobj(r.raw, key, ExtraArgs = {'ACL':'public-read'})

def create_db():
    dbServ = boto3.resource('dynamodb')
    table = dbServ.create_table(
        TableName = dbName,
        KeySchema=[
            {
                'AttributeName':'firstName',
                'KeyType':'HASH'
            },
            {
                'AttributeName':'lastName',
                'KeyType':'RANGE'
            }
        ],
        AttributeDefinitions=[{
                'AttributeName': 'firstName',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'lastName',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits':5,
            'WriteCapacityUnits':5
        }
    )

    print("Table Status: ", table.table_status )

def checkAndAddToDb(currLine):
    currLine.strip()

    valuesWord = currLine.split()
    firstName = valuesWord[0]
    lastName = valuesWord[1]
    otherString = ""

    for word in valuesWord[2:]:
        otherString += word + " "

    db_client = boto3.client('dynamodb')
    reponse = db_client.list_tables()

    if dbName not in reponse["TableNames"]:
        create_db()

    clientObj = boto3.resource('dynamodb')
    table = clientObj.Table('programfourestoragetable')
    tempVar = table.table_status
    while len(str(tempVar)) is not len("ACTIVE"):
        #print(str(tempVar))
        time.sleep(3)
        print("Waiting for table to get created")
        table = clientObj.Table('programfourestoragetable')
        tempVar = table.table_status


    table.put_item(
        Item={
            'firstName':firstName,
            'lastName':lastName,
            'otherString':otherString
        }
    )

def update_dynamoDb():
    #content = s3.Object(bucket_name, key).get()
    #body = content['Body']

    fileObj = client1.get_object(Bucket=bucket_name, Key = key)
    fileData = fileObj['Body'].read()

    contents = fileData.decode('utf-8')

    '''
    tableKeys = []
    tableKeys.append("firstName")
    tableKeys.append("lastName")

    for word in contents.split():
        if len(word.split('=')) > 1 :
            keyValue = word.split('=')
            tableKeys.append(str(keyValue[0]))

    my_list = list(set(tableKeys))
    '''

    for line in contents.splitlines():
        checkAndAddToDb(line)


"""
    Recursive function to take a backup of all subdirectories and
    their contents into the bucket of users choice. 
    It not only takes a backup at each time but first verifies that 
    the same file has not already been backed up
"""
def load_data():

    read_data_upload_s3()
    update_dynamoDb()


def clear_data():
    dynamodb = boto3.client('dynamodb')
    dynDb = boto3.resource('dynamodb')
    table_list = dynamodb.list_tables()['TableNames']
    table = dynDb.Table('programfourestoragetable')

    if dbName in table_list:
        table.delete()
        time.sleep(5)

    obj = s3.Object(bucket_name, key)
    obj2 = boto3.client('s3')
    obj_list = obj2.list_objects(Bucket=bucket_name)['Contents']

    if obj in obj_list:
        obj.delete()
        time.sleep(3)

    print("Successfully deleted the data or it was never there")

def queryData(q1,q2):
    dynamodb = boto3.client('dynamodb')
    tempDb= boto3.resource('dynamodb')
    table = tempDb.Table('programfourestoragetable')
    response = []

    if dbName not in dynamodb.list_tables()['TableNames']:
        print("DAtabase table does not exist. Load data first")
    else:
        if (q1 is None or len(q1) is 0) and (q2 is None and len(q2) is 0):
            print("FirstName and lastname values null or empty. Try again")
        elif (q1 is not None and len(q1) > 0) and (q2 is not None and len(q2) > 0):
            print("Querying database right now")
            response = table.query(KeyConditionExpression = boto3.dynamodb.conditions.Key('firstName').eq(str(q1)) & boto3.dynamodb.conditions.Key('lastName').eq(str(q2)))
        elif q2 is not None and len(q2) > 0:
            print("Querying database right now with lastName only")
            response = table.query(KeyConditionExpression = boto3.dynamodb.conditions.Key('lastName').eq(str(q2)))
        elif q1 is not None and len(q1) > 0:
            print("Querying database right now with firstName only")
            response = table.query(KeyConditionExpression = boto3.dynamodb.conditions.Key('firstName').eq(str(q1)))

    for i in response['Items']:
        print(i['firstName'], " ", i['lastName'], " ", i['otherString'])

"""
    The method validates the arguments passed in to the program
    It takes an argument of the current directory that needs to be backed
    It then recursively backs up all subdirectories and files in the bucket of 
    choice
    
    Assumption: the boto3 has been setup by anyone using the code. Refer to the
    steps in the BuildFile to setup Boto3 and configure it with your AWS account
    using the access key and secret access key
"""
def main():
    load_data()
    #clear_data()
    #clear_data()

    q1 = "Ray"
    q2 = ""
    queryData(q1,q2)

    clear_data()




if __name__ == "__main__":
    main()
