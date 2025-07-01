import boto3
import pandas as pd
def connect_to_dynamo():
    dynamodb=boto3.resource('dynamodb',region_name='us-east-1')

    table=dynamodb.Table('customerActivity')
    return table

def extract_item(table):
    response=table.scan()

    items=response['Items']
    df=pd.DataFrame(items)
    print(df.head())
    print('extracted')
    return df
