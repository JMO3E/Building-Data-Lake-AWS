from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from dotenv import load_dotenv

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

# Extract data from MYSQL
def extract():
    try:
        engine = create_engine(f'mysql+pymysql://{user}:{password}@{server}:{port}/{db}')
        
        Session = scoped_session(sessionmaker(bind=engine))
        
        sec = Session()
        
        db_content = sec.execute(text(""" SELECT table_name FROM information_schema.tables WHERE table_schema = 'world' """))
           
        for table in db_content:
            df = pd.read_sql_query(f'select * FROM {table[0]}', engine)
            #load data
            load(df, table[0])
            
    except Exception as error:
        print("Data extract error: " + str(error));

# Load Data to AWS S3
def load(df, table):  
    try:
        print(f'Importing {len(df)} rows from table {table}');
        
        # Save to S3
        s3_bucket = 'my-weather-data-bucket-18634';
        upload_file_key = 'public/' + str(table) + f"/{str(table)}";
        file_path = upload_file_key + ".csv";
        
        s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key, region_name='us-east-1');
        
        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False);
            
            response = s3_client.put_object(
                Bucket=s3_bucket, Key=file_path, Body=csv_buffer.getvalue()
            )
            
            status = response.get("ResponseMetaData", {}).get("HTTPStatusCode");
            
            if status == 200:
                print(f"Succesful S3 response. Status = {status}");
            else:
                print(f"Unuccesful S3 response. Status = {status}");
                       
    except Exception as error:
        print("Data load error: " + str(error));
        
if __name__ == "__main__":
    try:
        extract();
    except Exception as error:
        print("Error while extracting the data: " + str(error));