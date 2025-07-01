import json
import configparser
from pymongo import MongoClient
import pandas as pd
#connect to mongodb
def connect():
    connection_string = "mongodb://localhost:27017/"
    client = MongoClient(connection_string)
    db = client['question_11']
    collection = db['unstructured']
    print("python-mongodb success")
    
    #loading into mongodb
    with open('unstructured.json') as file:
        file_data=json.load(file)
    if isinstance(file_data,list):
        collection.insert_many(file_data)
    else:
        collection.insert_one(file_data)
    print("loaded to mongodb")
    return collection

# extracting data from mongodb
def extract_table():
    collection=connect()
    data = list(collection.find())
    df = pd.DataFrame(data)
    print(df.head())
    return df

#df=extract()
#print(df.head())