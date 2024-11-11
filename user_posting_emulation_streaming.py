import requests
import json
from sqlalchemy import text
import random
from time import sleep
import user_posting_emulation as upe

class KinesisConnector:
    '''
    A class with methods for Kinesis Data Streams
    Attributes: None
    '''
    
    def record_to_Kinesis(result: list, headers = {'Content-Type': 'application/json'}):
        '''
        Posts data to Kinesis Data Streams
        Params: result - a list of the form [topic, json payload],
                headers - API header
        Prints: Status code
        '''
        
        stream_name = f"streaming-0affe94cc7d3-{result[0]}"
        
        #Modifies the api url with the appropriate topic
        invoke_url = f"https://qrtrf2bgl0.execute-api.us-east-1.amazonaws.com/feat/streams/{stream_name}/record"
        
        #Creates the payload to be post from a dictionary
        payload = json.dumps({
            "StreamName": stream_name,
            "Data": result[1],
            "PartitionKey": "partition-0"
            }, default=str)
        
        #Sends the post request to the api
        response = requests.request("PUT", invoke_url, headers=headers, data=payload)
        
        print(f'Status code: {response.status_code}')
    
    def list_streams():
        '''
        Prints a dictionary of all the kinesis streams and their summaries
        '''
        
        response = requests.request("GET", "https://qrtrf2bgl0.execute-api.us-east-1.amazonaws.com/feat/streams")
        
        print(json.dumps(json.loads(response.content.decode('utf8')), indent=4))
    
    def create_stream(stream_name:str):
        '''
        Create a kinesis stream with the give input
        Params: str
        '''
        
        response = requests.request("POST", f"https://qrtrf2bgl0.execute-api.us-east-1.amazonaws.com/feat/streams/{stream_name}")
        
    def describe_stream(stream_name:str):
        '''
        Prints a description of the kinesis stream with the give input
        Params: str 
        '''
        
        response = requests.request("GET", f"https://qrtrf2bgl0.execute-api.us-east-1.amazonaws.com/feat/streams/{stream_name}")
        
        print(json.dumps(json.loads(response.content.decode('utf8')), indent=2))
        
    def delete_stream(stream_name:str):
        '''
        Deletes a kinesis stream with the give input
        Params: str
        '''
        
        response = requests.request("DELETE", f"https://qrtrf2bgl0.execute-api.us-east-1.amazonaws.com/feat/streams/{stream_name}")


Connector = upe.AWSDBConnector()

def infinite_post_data_kinesis():
    '''
    Continously pulls information from the Pinterest database emulation
    '''
    
    #Continous executes the code inside
    while True:
        
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = Connector.create_db_connector()

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
            #Uses the put method for the kinesis api to send the Pinterest post data
            KinesisConnector.record_to_Kinesis(result= ['pin', pin_result])
            
            print(f'Geographical data: {geo_result}')
            #Uses the put method for the kinesis api to send the Pinterest post data
            KinesisConnector.record_to_Kinesis(result= ['geo', geo_result])
            
            print(f'User data: {user_result}')
            #Uses the put method for the kinesis api to send the Pinterest post data
            KinesisConnector.record_to_Kinesis(result= ['user', user_result])


if __name__ == "__main__":
    infinite_post_data_kinesis()
    print('Working')