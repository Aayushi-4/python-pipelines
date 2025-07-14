import urllib.parse
from sqlalchemy import create_engine
import configparser
import urllib.parse
import pyodbc
import pandas as pd

def read_config(file_path=r'C:\Users\Aayushi\Documents\tasks\task13\config.ini'):
    config=configparser.ConfigParser()
    config.read(file_path)
    return config

def get_sqlalchamey_engine(config):
    sql_config=config['sqlserver']
    conn=(
        f"driver={sql_config['driver']};"
        f"server={sql_config['server']};"
        f"database={sql_config['database']};"
        f"uid={sql_config['username']};"
        f"pwd={sql_config['password']}"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={conn}")