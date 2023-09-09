from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from dotenv import load_dotenv
import pymysql

import pandas as pd
import json
import io
import boto3
import os


# Getting API Keys
config_content = open('config.json');
config = json.load(config_content);

#Setting Access Keys
access_key = config['access_key'];
secret_access_key = config['secret_access_key'];

# Getting SQL DB details
load_dotenv();

user = os.environ.get("NAME");
password = os.environ.get("PASSWORD");
server = os.environ.get("HOST_NAME");
db = os.environ.get("MYSQL_DATABASE");
port = os.environ.get("MYSQL_PORT");


def extract():
    try:
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{server}:{port}/{db}')
        
        Session = scoped_session(sessionmaker(bind=engine))
        
        sec = Session()
        
        src_tables = sec.execute(text(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'world' """))
    
        
        for tbl in src_tables:
            # query and load save data to dataframe
            df = pd.read_sql_query(f'select * FROM {tbl[0]}', engine)
            load(df, tbl[0])
            
    except Exception as e:
        print("Data extract error: " + str(e))

def load():   
    
if __name__ == "__main__":
    extract();