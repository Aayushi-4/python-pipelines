import boto3
import pandas as pd
from extract import extract_document
def connect_to_dynamo():
    dynamodb=boto3.resource('dynamodb',region_name='us-east-1')
    table=dynamodb.Table('customerActivity')
    
    return table
def load_to_dynamo(df,table):
    for index,rows in df.iterrows():
        item ={col:str(rows[col])for col in df.columns}

        table.put_item(Item=item)
    print("data loaded into dynamodb")