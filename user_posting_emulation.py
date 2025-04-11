import boto3
import json
import random
import requests
import sqlalchemy
import yaml
from datetime import datetime
from time import sleep
from multiprocessing import Process
from sqlalchemy import text


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
    
    def get_random_row_from_table(self, table_name, row_idx, conn):
        query = text(f"SELECT * FROM {table_name} LIMIT {row_idx}, 1")
        selected_row = conn.execute(query)
            
        for row in selected_row:
            result = dict(row._mapping)
        return result

def load_yaml(path):
    with open(path, 'r') as file:
        data = yaml.safe_load(file)
    return data

def convert_datetime_to_string(data):
    for key, value in data.items():
        if isinstance(value, datetime):
            data[key] = value.isoformat()
    return data


def create_kafka_payload(data):
    return json.dumps({
        "records": [
            {"value": data}
            ]
        })

def create_kinesis_payload(stream_name, data, partition_key):
    return json.dumps({
        "StreamName": stream_name, 
        "Data": data, 
        "PartitionKey": partition_key
        })

def send_request(request_type, full_invoke_url, headers, payload, stream_name):
    response = requests.request(request_type,
                                full_invoke_url,
                                headers=headers,
                                data=payload,
                                timeout=60
                                )
    
    if response.status_code == 200:
        print(f"Data successfully sent to {stream_name}")
    else:
        print(f"Failed to send data to {stream_name}: {response.status_code} - {response.text}")


def stream_to_kafka(invoke_url, topic_name, data):
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    payload = create_kafka_payload(data)
    full_invoke_url = f"{invoke_url}/topics/{topic_name}"
    send_request("POST", full_invoke_url, headers, payload, f"Kafka topic {topic_name}")


def stream_to_kinesis(invoke_url, stream_name, data, partition_key):
    headers = {'Content-Type': 'application/json'}
    full_invoke_url = f"{invoke_url}/streams/{stream_name}/record"
    print()
    print(full_invoke_url)
    print()
    payload = create_kinesis_payload(stream_name, data, partition_key)
    send_request("PUT", full_invoke_url, headers, payload, f"Kinesis stream {stream_name}")


def run_infinite_post_data_loop(db_conn_instance, kafka_api_details, kinesis_api_details):
    kafka_invoke_url = kafka_api_details['invoke_url']
    pin_topic = kafka_api_details['pin_topic']
    geo_topic = kafka_api_details['geo_topic']
    user_topic = kafka_api_details['user_topic']
    
    kinesis_invoke_url = kinesis_api_details['invoke_url']
    kinesis_stream_name = kinesis_api_details['stream_name']
    pin_key = kinesis_api_details["partition_keys"]["pin"]
    geo_key = kinesis_api_details["partition_keys"]["geo"]
    user_key = kinesis_api_details["partition_keys"]["user"]

    engine = db_conn_instance.create_db_connector()
    
    for i in range(500):
        print(i)
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)

        with engine.connect() as connection:

            pin_result = db_conn_instance.get_random_row_from_table(
                'pinterest_data',
                random_row,
                connection
            )
            cleaned_pin_result = convert_datetime_to_string(pin_result)
            print('Pin row:')
            print('Kinesis')
            stream_to_kinesis(kinesis_invoke_url, kinesis_stream_name, cleaned_pin_result, partition_key=pin_key)
            print('Kafka')
            stream_to_kafka(kafka_invoke_url, pin_topic, cleaned_pin_result)
            
            geo_result = db_conn_instance.get_random_row_from_table(
                'geolocation_data',
                random_row,
                connection
                )
            cleaned_geo_result = convert_datetime_to_string(geo_result)
            print('Geo row:')
            print('Kinesis')
            stream_to_kinesis(
                kinesis_invoke_url, 
                kinesis_stream_name, 
                cleaned_geo_result, 
                partition_key=geo_key
                )
            print('Kafka')
            stream_to_kafka(kafka_invoke_url, geo_topic, cleaned_geo_result)
            
            user_result = db_conn_instance.get_random_row_from_table(
                'user_data',
                random_row,
                connection
                )
            cleaned_user_result = convert_datetime_to_string(user_result)
            print('User row:')
            print('Kinesis')
            stream_to_kinesis(kinesis_invoke_url, kinesis_stream_name, cleaned_user_result, partition_key=user_key)
            print('Kafka')
            stream_to_kafka(kafka_invoke_url, user_topic, cleaned_user_result)
        
        

if __name__ == "__main__":
    pinterest_rds_creds = load_yaml('pinterest_data_rds.yaml')
    #print(pinterest_rds_creds)
    kafka_api_creds = load_yaml('API_invoke_url.yaml')
    kinesis_api_creds = load_yaml('kinesis_api.yaml')

    new_connector = AWSDBConnector(pinterest_rds_creds)

    run_infinite_post_data_loop(new_connector, kafka_api_creds, kinesis_api_creds)
