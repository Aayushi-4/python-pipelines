#sql server connect
import configparser
import pyodbc
import urllib
from sqlalchemy import create_engine
from transform import transform_data
import pandas as pd
def read_config(file_path=r'C:\Users\Aayushi\Documents\tasks\mongo_11\config\config.ini'):
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


def load_to_sql():
    df = transform_data()
    config = read_config()
    engine = get_sqlalchemy_engine(config)
 
    if '_id' in df.columns:
        df['_id'] = df['_id'].astype(str)
 
    df.to_sql('project_combined', con=engine, if_exists='replace', index=False)
    print(" All project data loaded into one table.")
 