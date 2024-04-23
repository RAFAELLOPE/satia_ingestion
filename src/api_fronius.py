import requests
import json
import pandas as pd
from datetime import datetime
import os


class FroniusExtractor(object):
    def __init__(self, config:dict, pv_system_id:str):
        self.api_value = config["API_VALUE"]
        self.api_key = config["API_KEY"]
        self.header = {'AccessKeyId': self.api_key,
                       'AccessKeyValue': self.api_value}
        self.pv_system = pv_system_id

        self.api_list_pv_systems = f'https://api.solarweb.com/swqapi/pvsystems-list'
        self.api_list_devices = f'https://api.solarweb.com/swqapi/pvsystems/{self.pv_system}/devices-list'
        self.api_dev_historical = f'https://api.solarweb.com/swqapi/pvsystems/{self.pv_system}/'

    
    def transform_list_pv_systems(self, data:dict) -> pd.DataFrame:
        pass

    def transform_component_list(self, data:dict) -> pd.DataFrame:
        pass

    def transform_device_data(self, data:dict) -> pd.DataFrame:
       pass

    def get_pv_system_list(self) -> pd.DataFrame:
        try:
            res = requests.get(self.api_list_pv_systems, headers=self.header)
            if (res.ok):
                data = json.loads(res.content)
                df = self.transform_list_pv_systems(data)
            else:
                raise Exception(f"API response not OK: {res.status_code}")
        except requests.exceptions.RequestException as e:
            raise e
        return df


    def get_componet_list(self) -> pd.DataFrame:
        try:
            res = requests.get(self.api_list_devices, headers=self.header)
            if(res.ok):
                data = json.loads(res.content)
                df = self.transform_component_list(data)
            else:
                raise Exception(f"API response not OK: {res.status_code}")
        except requests.exceptions.RequestException as e:
            raise e
        return df

    
    def get_device_data(self,
                        device_id:str,
                        start_time:datetime,
                        end_time:datetime) -> pd.DataFrame:
    
        time_format = "%Y-%m-%d %H:%M:%S"
        start_time = datetime.strftime(start_time, time_format).replace(' ', '%20')
        end_time = datetime.strftime(end_time, time_format).replace(' ', '%20')

        api_call = self.api_dev_historical + f'devices/{device_id}/histdata?'
        api_call = api_call + f"from={start_time}&to={end_time}"

        try:
            res = requests.get(api_call, headers=self.header)
            if(res.ok):
                data = json.loads(res.content)
                df = self.transform_device_data(data)
            else:
                raise Exception(f"API response not OK: {res.status_code}")
        except requests.exceptions.RequestException as e:
            raise e
        return df
    


