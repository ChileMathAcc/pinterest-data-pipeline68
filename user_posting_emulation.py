import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
import yaml
import pymysql


class AWSDBConnector:
    '''
    Creates a connector to a Pinterest database
    Attributes: database credentails
    '''

    def __init__(self):
        '''
        Intializes an instanst with the desired attributes
        '''
        creds = AWSDBConnector.read_db_creds('db_creds.yaml')
        self.HOST = creds['HOST']
        self.USER = creds['USER']
        self.PASSWORD = creds['PASSWORD']
        self.DATABASE = creds['DATABASE']
        self.PORT = creds['PORT']
    
    def read_db_creds(yaml_file):
        '''
        Opens a yaml document with the credentails for the database
        Params: path to yaml file
        Returns: Dictionary with database credentials
        '''
        
        #Opens a yaml file (read mode) and loads its data
        with open(yaml_file, 'r') as d:
            db_creds = yaml.safe_load(d)
            
        return db_creds
    
    def post_to_kafka(result: list, headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}):
        '''
        Posts data to kafka topics on an EC2 server
        Params: result - a list of the form [topic, json payload],
                headers - API header
        Return: Status code
        '''
        
        topic = result[0]
        
        #Modifies the api url with the appropriate topic
        invoke_url = f"https://qrtrf2bgl0.execute-api.us-east-1.amazonaws.com/dev/topics/0affe94cc7d3.{topic}"
        
        #Creates the payload to be post from a dictionary
        payload = json.dumps({
            "records":
                [{"value": result[1]}]}, default=str)
        
        #Sends the post request to the api
        response = requests.request("POST", invoke_url, headers=headers, data=payload)
        
        return response.status_code
            
    def create_db_connector(self):
        '''
        Creates the connection engine
        '''
        
        #Uses the atributes of this Class to construct an engine
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4",
                                          pool_pre_ping = True)
        return engine


new_connector = AWSDBConnector()


def infinite_post_data_kafka():
    '''
    Continously pulls information from the Pinterest database emulation
    '''
    
    #Continous executes the code inside
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            
            #Create and executes a sql query to retrieve a random row of data
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            #Converts that row to a dictionary
            for row in pin_selected_row:
                pin_result = dict(row._mapping)

            #Create and executes a sql query to retrieve a random row of data
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            #Converts that row to a dictionary
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            #Create and executes a sql query to retrieve a random row of data
            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            #Converts that row to a dictionary
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            print(f'Pinterest post data: {pin_result}')
            #Uses the post to api method to send the Pinterest data
            status_code = AWSDBConnector.post_to_kafka(result= ['pin', pin_result])
            
            # Check if records are being sent through
            if status_code != 200:
                print(f"Terminating infinite loop. API error - status code: {status_code}")
                break
            
            print(f'Geographical data: {geo_result}')
            #Uses the post to api method to send the Pinterest data
            AWSDBConnector.post_to_kafka(result= ['geo', geo_result])
            
            print(f'User data: {user_result}')
            #Uses the post to api method to send the Pinterest data
            AWSDBConnector.post_to_kafka(result= ['user', user_result])


if __name__ == "__main__":
    infinite_post_data_kafka()
    print('Working')