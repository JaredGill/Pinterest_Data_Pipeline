import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import yaml
from datetime import datetime

# This class likely serves as a connector to interact with an AWS database.
class AWSDBConnector:

    def __init__(self, creds: dict) -> None:
        self.creds = creds
        self.HOST = self.creds['RDS_HOST']
        self.USER = self.creds['RDS_USER']
        self.PASSWORD = self.creds['RDS_PASSWORD']
        self.DATABASE = self.creds['RDS_DATABASE']
        self.PORT = self.creds['RDS_PORT']
        
    def create_db_connector(self):
        '''The function creates a database connector using SQLAlchemy to connect to a MySQL database.
        
        Returns
        -------
            The function `create_db_connector` is returning the SQLAlchemy engine object that is created
        using the connection string for a MySQL database.
        
        '''
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine
    
def load_yaml(path):
    with open(path, 'r') as file:
        data = yaml.safe_load(file)
    return data

def convert_datetime_to_string(data):
    for key, value in data.items():
        if isinstance(value, datetime):
            data[key] = value.isoformat()
    return data


def stream_to_kafka(
        invoke_url, 
        topic_name, 
        data):
    
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}

    payload = json.dumps({
        "records": [
            {"value": data}
            ]
        })
    
    #print(payload)
    full_invoke_url = f"{invoke_url}/topics/{topic_name}"
    #print(full_invoke_url)
    response = requests.request("POST", full_invoke_url, headers=headers, data=payload)

    if response.status_code == 200:
        print(f"Data successfully sent to topic {topic_name}")
    else:
        print(f"Failed to send data to topic {topic_name}: {response.status_code} - {response.text}")


def run_infinite_post_data_loop(engine, api_details):
    api_invoke_url = api_details['invoke_url']
    pin_topic = api_details['pin_topic']
    geo_topic = api_details['geo_topic']
    user_topic = api_details['user_topic']

    for i in range(500):
        print(i)
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)

        with engine.connect() as connection:

            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
                cleaned_pin_result = convert_datetime_to_string(pin_result)  
                print('Pin row:')
                stream_to_kafka(api_invoke_url, pin_topic, cleaned_pin_result)

            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
                
            for row in geo_selected_row:
                geo_result = dict(row._mapping)
                cleaned_geo_result = convert_datetime_to_string(geo_result)  
                print('Geo row:')
                stream_to_kafka(api_invoke_url, geo_topic, cleaned_geo_result)

            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
                
            for row in user_selected_row:
                user_result = dict(row._mapping)
                cleaned_user_result = convert_datetime_to_string(user_result)
                print('User row')
                stream_to_kafka(api_invoke_url, user_topic, cleaned_user_result)
        
        

if __name__ == "__main__":
    pinterest_rds_creds = load_yaml('pinterest_data_rds.yaml')
    #print(pinterest_rds_creds)
    api_creds = load_yaml('API_invoke_url.yaml')

    new_connector = AWSDBConnector(pinterest_rds_creds)
    engine = new_connector.create_db_connector()
    run_infinite_post_data_loop(engine, api_creds)
