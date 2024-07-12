import requests
import json
import pandas as pd
from datetime import datetime

class SolarEdgeExtractor(object):
    def __init__(self, config:dict):
        self.site_id = config["SITE_ID"]
        self.api_key = config["API_KEY"]
        self.component_list_api = f'https://monitoringapi.solaredge.com/equipment/{self.site_id}/list?api_key={self.api_key}'
        self.inverter_data_api = f'https://monitoringapi.solaredge.com/equipment/{self.site_id}/'
        self.site_details_api = f'https://monitoringapi.solaredge.com/site/{self.site_id}/details?api_key={self.api_key}'

    def get_componet_list(self) -> dict:
        try:
            res = requests.get(self.component_list_api)
            if(res.ok):
                data = json.loads(res.content)
                return data['reporters']['list']
            else:
                raise Exception(f"API response not OK: {res.status_code}")
        except requests.exceptions.RequestException as e:
            raise e

    
    def get_inverter_data(self,
                          serial_number:str,
                          start_time:datetime,
                          end_time:datetime) -> list:
    
        time_format = "%Y-%m-%d %H:%M:%S"
        start_time = datetime.strftime(start_time, time_format).replace(' ', '%20')
        end_time = datetime.strftime(end_time, time_format).replace(' ', '%20')

        api_call = self.inverter_data_api + f'{serial_number}/data?'
        api_call = api_call + f"startTime={start_time}&endTime={end_time}"
        api_call = api_call + f"&api_key={self.api_key}"

        try:
            res = requests.get(api_call)
            if(res.ok):
                data = json.loads(res.content)
                return data['data']['telemetries']
            else:
                raise Exception(f"API response not OK: {res.status_code}")
        except requests.exceptions.RequestException as e:
            raise e
    
    def get_site_details(self) -> dict:
        try:
            res = requests.get(self.site_details_api)
            if(res.ok):
                data = json.loads(res.content)
                return data['details']
            else:
                raise Exception(f"API response not OK: {res.status_code}")
        except requests.exceptions.RequestException as e:
            raise e
    


