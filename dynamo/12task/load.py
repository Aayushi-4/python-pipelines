#sql sever connect
import configparser
import urllib.parse
import pyodbc
import urllib
from sqlalchemy import create_engine
from transform import transform_data
from extract1 import connect_to_dynamo
import pandas as pd

def read_config(file_path=r'C:\Users\Aayushi\Documents\tasks\dynamo\config.ini'):
    config=configparser.ConfigParser()
    config.read(file_path)
    return config

def connect_to_sql(config):
    sql_config=config['sqlserver']

    con_str=urllib.parse.quote_plus(
        f"DRIVER={sql_config['driver']};"
        f"SERVER={sql_config['server']};"
        f"DATABASE={sql_config['database']};"
        f"UID={sql_config['username']};"
        f"PWD={sql_config['password']}"
    )

    return create_engine(f"mssql+pyodbc:///?odbc_connect={con_str}")

def load_to_sql():
    df=transform_data(connect_to_dynamo())
    config=read_config()
    engine=connect_to_sql(config)

    df.to_sql('dynamo',con=engine,if_exists='replace',index=False)
    print("loaded succesfully")