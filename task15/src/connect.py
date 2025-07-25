#connect.py
import boto3
import configparser
import pyodbc
import os
from dotenv import load_dotenv
 # Load variables from .env
def load_aws_config(env_file=r'C:\Users\Aayushi\Documents\tasks\task15\config\config.env'):
    load_dotenv(env_file)  
 
    config = {
        'access_key': os.getenv('AWS_ACCESS_KEY_ID'),
        'secret_key': os.getenv('AWS_SECRET_ACCESS_KEY'),
        'region': os.getenv('AWS_REGION'),
        'bucket': os.getenv('BUCKET_NAME')
    }
 
    if not all(config.values()):
        raise ValueError("Missing one or more AWS configuration variables")
 
    return config
 
def connect_to_s3():
    aws = load_aws_config()
 
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws['access_key'],
        aws_secret_access_key=aws['secret_key'],
        region_name=aws['region']
    )
 
    print(f"Connected to S3 bucket: {aws['bucket']}")
    return s3, aws['bucket']
 
 
def read_config(file_path=r'C:\Users\Aayushi\Documents\tasks\task15\config\config.ini'):
    config = configparser.ConfigParser()
    config.read(file_path)
    print(f"Reading from: {file_path}")
    print("Sections loaded:", config.sections())
    return config
 
 
def get_db_connection(config):
    sql_config = config['sqlserver']
 
    conn_str = (
        f"DRIVER={sql_config['driver']};"
        f"SERVER={sql_config['server']};"
        f"DATABASE={sql_config['database']};"
        f"UID={sql_config['username']};"
        f"PWD={sql_config['password']}"
    )
 
    return pyodbc.connect(conn_str)
 