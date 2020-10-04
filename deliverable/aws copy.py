# Author Name: Jie Zhou
# AWS Cloud Storage Homework

import boto3
import csv

# set the credentials, credentials are removed for safety purpose
s3 = boto3.resource('s3')

# try upload a file to the bucket
try:
    s3.create_bucket(Bucket='zhoujiefirstbucket2', CreateBucketConfiguration={
        'LocationConstraint': 'us-west-2'})
except:
    print("this may already exist")

# create a bucket
bucket = s3.Bucket("zhoujiefirstbucket2")
bucket.Acl().put(ACL='public-read') # setting the access to public

body = open('file.txt', 'rb')
# print(body)  # debug print
o = s3.Object('zhoujiefirstbucket2', 'test').put(Body=body)
s3.Object('zhoujiefirstbucket2', 'test').Acl().put(ACL='public-read')

# create a dynamodb, credentials are removed for safety purpose
dyndb = boto3.resource('dynamodb', region_name='us-west-2')

# creating a table
# itemid, experimentid, description, date, url
try:
    table = dyndb.create_table(
        TableName='Experiment',
        KeySchema=[
            {
                'AttributeName': 'itemid',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'experimentid',
                'KeyType': 'RANGE'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'itemid',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'experimentid',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5}
    )
except:
    # if there is an exception, the table may already exist.
    table = dyndb.Table("Experiment")
# wait for the table to be created if so...
table.meta.client.get_waiter('table_exists').wait(TableName='Experiment')
# print(table) # debug print

with open('/Users/jay/Desktop/2020 Fall/CS-1660/Assignments/CloudStorageHomework/experimentData.csv', 'r') as csvfile:
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')

    for item in csvf:
        print(item)
        addr = '/Users/jay/Desktop/2020 Fall/CS-1660/Assignments/CloudStorageHomework/' + item[3]
        body = open(addr, 'rb')
        s3.Object('zhoujiefirstbucket2', item[3]).put(Body=body)
        md = s3.Object('zhoujiefirstbucket2', item[3]).Acl().put(ACL='public-read')
        url = "https://zhoujiefirstbucket2.s3-us-west-2.amazonaws.com/" + item[3]
        metadata_item = {'itemid': item[0], 'experimentid': item[1], 'description': item[4], 'date': item[2],
                         'url': url}
        try:
            table.put_item(Item=metadata_item)
        except:
            print("item may already be there or another failure")

# try query the database
response = table.get_item(
    Key={
        'itemid': 'data1',
        'experimentid': '2'
    }
)
item = response['Item']
print("Below is the query result:")
print(item)
# print(response) # debug print