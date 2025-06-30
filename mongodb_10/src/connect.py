import json
from pymongo import MongoClient
import pandas as pd
# Connect to MongoDB
connection_string = "mongodb://localhost:27017/"
client = MongoClient(connection_string)
db = client['question_10']
collection = db['project']
print("python-mongodb success")
"""
# Load JSON file
with open("project.json", "r") as file:
    data = json.load(file)
# Insert data into MongoDB
if isinstance(data, list):
    collection.insert_many(data)  # For a list of documents
else:
    collection.insert_one(data)  # For a single document
"""
print("JSON file imported successfully!")

# extracting data from mongodb
data = list(collection.find())
df = pd.DataFrame(data)
print(df.head())

#sql server connect
import configparser
import pyodbc
import urllib
from sqlalchemy import create_engine
def read_config(file_path=r"C:\Users\Aayushi\Documents\tasks\mongodb\config.ini"):
    config = configparser.ConfigParser()
    config.read(file_path)
    return config
 
def get_sqlalchemy_engine(config):
    sql_config = config['sqlserver']
 
    conn_str = urllib.parse.quote_plus(
        f"DRIVER={sql_config['driver']};"
        f"SERVER={sql_config['server']};"
        f"DATABASE={sql_config['database']};"
        f"UID={sql_config['username']};"
        f"PWD={sql_config['password']}"
    )
 
    return create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}")

config=read_config()
# Convert ObjectId to string
df['_id'] = df['_id'].astype(str)
df=df.explode("technologies")
engine = get_sqlalchemy_engine(config)
df.to_sql('mongo_data',con=engine,if_exists='replace', index=False)
print("loading is completed")
 