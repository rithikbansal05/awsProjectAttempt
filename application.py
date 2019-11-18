import os
import boto3
import time
import urllib
import requests
import boto3.dynamodb.conditions
from botocore.exceptions import ClientError
from flask import Flask, render_template, request
from boto3.dynamodb.conditions import Key, Attr

s3 = boto3.resource("s3")
client1 = boto3.client("s3")
url = "https://s3-us-west-2.amazonaws.com/css490/input.txt"
bucket_name = 'programfourestorage'
key = 'input.txt'
dbName = 'programfourestoragetable'
my_list = []
data = []

application = Flask(__name__, template_folder='template')
application.debug = True

@application.route('/',methods=['GET'])
def hello():
    application.logger.info("home page working")
    return render_template('home.html')

@application.route('/update',methods=['POST'])
def LoadData():
    application.logger.info("load data method ")
    load_data()
    return render_template('home.html')

@application.route('/clear',methods=['POST'])
def ClearFunc():
    application.logger.info("clearing data")
    clear_data()
    return render_template('home.html')

@application.route('/query',methods=['POST'])
def loaddat():

    firstName = str(request.form['first'])
    lastName = str(request.form['last'])
    application.logger.info("The first name and last name are" +firstName + " " + lastName)
    qData = queryData(firstName,lastName)
    application.logger.info(str(qData))
    if qData != [] and qData != None:
        application.logger.info("got results. Uploading")
        return render_template("home.html", data=qData)
    elif qData == None or qData == []:
        return render_template("home.html", data="No users match query results")

def read_data_upload_s3():
    application.logger.info("adding file to s3")

    global url
    global fname
    fname = url[url.rfind("/") + 1:]
    location = os.getcwd() + "/" + url[url.rfind("/") + 1:]
    data = urllib.urlopen(url)
    datatowrite = data.read()
    with open(location, 'wb') as f:
        f.write(datatowrite)

    #r = requests.get(url, stream=True)
    #s3.Bucket('css490').download_file(key,key)
    #s3.meta.client.download_file('css490',key, key)
    application.logger.info("downloaded file")
    client1.upload_file(key,bucket_name,key, ExtraArgs={'ACL':'public-read'})
    application.logger.info("uploaded file to s3")
    #session = boto3.Session()
    #s3Obj = session.resource('s3')

    #bucket = s3Obj.Bucket(bucket_name)
    #bucket.upload_fileobj(r.raw, key, ExtraArgs={'ACL': 'public-read'})

def create_db():
    dbServ = boto3.resource('dynamodb', region_name='us-west-2')
    try:
        table = dbServ.create_table(
            TableName=dbName,
            KeySchema=[
                {
                    'AttributeName': 'firstName',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'lastName',
                    'KeyType': 'RANGE'
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
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
    except ClientError as e:
        print("table already exisits")
        return

    print("Table Status: ", table.table_status)
    application.logger.info("Table created")


def checkAndAddToDb(currLine):
    application.logger.info("contents of " + str(len(currLine)))

    firstName = currLine[0]
    lastName = currLine[1]
    otherString = ""

    for word in currLine[2:]:
        otherString += word + " "

    db_client = boto3.client('dynamodb', region_name='us-west-2')
    reponse = db_client.list_tables()

    if dbName not in reponse["TableNames"]:
        create_db()

    clientObj = boto3.resource('dynamodb', region_name='us-west-2')
    table = clientObj.Table('programfourestoragetable')
    tempVar = table.table_status
    while len(str(tempVar)) is not len("ACTIVE"):
        # print(str(tempVar))
        time.sleep(3)
        print("Waiting for table to get created")
        table = clientObj.Table('programfourestoragetable')
        tempVar = table.table_status
    try:
        table.put_item(
            Item={
                'firstName': firstName,
                'lastName': lastName,
                'otherString': otherString
            }
        )
        application.logger.info("data added")
    except ClientError as e:
        return 0



def update_dynamoDb():
    # content = s3.Object(bucket_name, key).get()
    # body = content['Body']

    fileObj = client1.get_object(Bucket=bucket_name, Key=key)
    fileData = fileObj['Body'].read()

    contents = []
    for line in urllib.urlopen(url):
        contents.append(line.split())

    for obj in contents:
        checkAndAddToDb(obj)

    '''
    tableKeys = []
    tableKeys.append("firstName")
    tableKeys.append("lastName")

    for word in contents.split():
        if len(word.split('=')) > 1 :
            keyValue = word.split('=')
            tableKeys.append(str(keyValue[0]))

    my_list = list(set(tableKeys))

    for line in contents.splitlines():
        checkAndAddToDb(line)
    '''

def load_data():
    application.logger.info("inside load_data")
    read_data_upload_s3()

    create_db()

    update_dynamoDb()


def clear_data():
    global table
    global dynDb
    application.logger.info("Clearing data now")
    #dynamodb = boto3.client('dynamodb')
    try:
        dynDb = boto3.resource('dynamodb', region_name='us-west-2')
    except ClientError as e:
        print("error message")
        return 0
    #table_list = dynamodb.list_tables()['TableNames']

    try:
        table = dynDb.Table(dbName)
    except ClientError as e:
        print("Not table")
        return

    try:
        table.delete()
    except ClientError  as e:
        print("Table not there")
        return 0

    try:
        s3.Object(bucket_name, key).delete()
        time.sleep(3)
    except ClientError as e:
        print("file not found")
        return 0

    '''
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
    '''

def queryData(q1, q2):
    application.logger.info(q1 + " " + q2)
    dynamodb = boto3.client('dynamodb', region_name='us-west-2')
    tempDb = boto3.resource('dynamodb', region_name='us-west-2')
    table = tempDb.Table('programfourestoragetable')
    response = []
    try:
        if dbName not in dynamodb.list_tables()['TableNames']:
            print("DAtabase table does not exist. Load data first")
        else:
            if (q1 is None or len(q1) is 0) and (q2 is None or len(q2) is 0):
                print("FirstName and lastname values null or empty. Try again")
            elif (q1 is not None and len(q1) > 0) and (q2 is not None and len(q2) > 0):
                print("Querying database right now")
                response = table.scan(FilterExpression=Key('firstName').eq(q1) & Attr('lastName').eq(q2))
            elif q2 is not None and len(q2) > 0:
                print("Querying database right now with lastName only")
                response = table.scan(FilterExpression=Attr('lastName').eq(q2))
                #response = table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key('lastName').eq(str(q2)))
            elif q1 is not None and len(q1) > 0:
                print("Querying database right now with firstName only")
                response = table.scan(FilterExpression=Key('firstName').eq(q1))
                #response = table.query(KeyConditionExpression=boto3.dynamodb.conditions.Key('firstName').eq(str(q1)))

        application.logger.info(str(len(response['Items'])))
        return response['Items']
        #for i in response['Items']:
        #    temp2 = []
        #    application.logger.info(i['firstName'] +  " " + i['lastName']+" " + i['otherString'])
        #    temp2.append(i['firstName']).append(i['lastName']).append(i['otherString'])
        #    data.append(temp2)
        #    return data
    except ClientError as e:
        retVal = []
        retVal.append("Table does not exist")
        return retVal

if __name__ == "__main__":
    application.run()