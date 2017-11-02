import pymysql
import boto3
import json
import decimal
from botocore.exceptions import ClientError

# MySQL connection
conn = pymysql.connect(host='%MYSQL_HOST%', user='%MYSQL_USER%', password='%MYSQL_PASSWORD%', db='%MYSQL_DB%', charset='utf8')

# Cursor from connection
curs = conn.cursor()

# Execute query
sql = "select id, created_at, email, profile_image, nickname from users"
curs.execute(sql)

# Fetch data
records = []
for row in curs:
    record = {
        'id': row[0],
        'created_at': row[1]
        'email': row[2],
        'info': {
            'profile_image': row[3],
            'nickname': row[4]
        }
    }
    print(record)
    records.append(record)

# Close connection
conn.close()


# Print JSON
print(json.dumps(records, ensure_ascii=False, indent="\t") )
# Write a JSON file
# with open('data_users.json', 'w', encoding="utf-8") as make_file:
#    json.dump(records, make_file, ensure_ascii=False, indent="\t")

# DynamoDB connection
dynamodb = boto3.resource('dynamodb', region_name='%AWS_REGION%')

# Create user table
tableUsers = dynamodb.Table('users')

try:
    tableUsers.delete()
except ClientError as ce:
    if ce.response['Error']['Code'] == 'ResourceNotFoundException':
        print ("tableUsers does not exist.")
    else:
        print ("Unknown exception occurred while querying for the tableUsers")

tableUsers = dynamodb.create_table(
    TableName='users',
    KeySchema=[
        {
            'AttributeName': 'id',
            'KeyType': 'HASH'  #Partition key
        },
        {
            'AttributeName': 'created_at',
            'KeyType': 'RANGE'  #Sort key
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'created_at',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'email',
            'AttributeType': 'S'
        }
    ],
    GlobalSecondaryIndexes=[
        {
            "IndexName": "email-index",
            "KeySchema": [
                {
                    "AttributeName": "email",
                    "KeyType": "HASH"
                }
            ],
            "Projection": {
                "ProjectionType": "KEYS_ONLY"
            },
            "ProvisionedThroughput": {
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
)
print("Table users status:", tableUsers.table_status)

# Put data
for user in records:
   print("put user:", user['id'], user['created_at'], user['email'], user['info'])
   tableUsers.put_item(
       Item={
           'id': user['id'],
           'created_at': user['created_at'],
           'email': user['email'],
           'info': user['info']
       }
   )
