import requests
import json
from typing import List
import pandas as pd
from pandas import json_normalize
from datetime import datetime

class HuaweiiExtractor(object):
    def __init__(self, config:dict, intl:str = 'eu5'):
        self.user = config["USER"]
        self.password = config["PASSWORD"]
        self.api = f'https://{intl}.fusionsolar.huawei.com/thirdData/'
        self.header = {"Content-Type": "application/json"}
        self.token = None


    def log_in(self):
        data = {"userName":self.user,
                "systemCode": self.password}
        try:
            res = requests.post(self.api + 'login', 
                                headers=self.header, 
                                json=data)
            res_data = json.loads(res.content)
            if res_data['success'] == True:
                self.token = res.headers["xsrf-token"]
        except requests.exceptions.RequestException as e:
            raise e
        return

    def get_device_list(self, plants:list) -> list:
        devices = []
        try:
            header = {"XSRF-TOKEN": self.token}
            body = {'stationCodes': ','.join(plants)}
            res = requests.post(self.api + 'getDevList',
                                headers=header,
                                json=body)
            res_data = json.loads(res.content)
            if res_data['success'] == True:
                devices =  res_data['data']
        except requests.exceptions.RequestException as e:
            raise e
        return devices

    def get_plant_list(self) -> list:
        plants = []
        try:
            header = {"XSRF-TOKEN": self.token}
            body = {'pageNo': 1}
            res = requests.post(self.api + 'stations',
                                headers=header,
                                json=body)
            res_data = json.loads(res.content)
            if res_data['success'] == True:
                remaining_pages = res_data['data']['pageCount'] - 1
                plants += res_data['data']['list']
                for i in range(remaining_pages):
                    body = {'pageNo': i}
                    res = requests.post(self.plant_list,
                                        headers=header,
                                        json=body)
                    res_data = json.loads(res.content)
                    plants += res_data['data']['list']
        except requests.exceptions.RequestException as e:
            raise e
        return plants
    
    def get_device_data(self, devices:List[str], start_time:int, end_time:int) -> dict:
        try:
            header = {"XSRF-TOKEN": self.token}
            body = {"devIds": ",".join(devices),
                    "devTypeId":1,
                    "startTime": start_time,
                    "endTime": end_time}
                
            res = requests.post(self.api + 'getDevHistoryKpi',
                                headers=header,
                                json=body)
                
            res_data = json.loads(res.content)
            if res_data['success'] == True:
                return res_data['data']
        except requests.exceptions.RequestException as e:
            raise e
    
    def get_device_data_as_df(self, 
                              devices:List[str], 
                              start_time:int, 
                              end_time:int):
        dev_data = self.get_device_data(devices=devices,
                                        start_time=start_time,
                                        end_time=end_time)
        
        meta = []
        for i in range(len(dev_data)):
            cols = ['devId','sn', 'collectTime'] + [f'dataItemMap__{c}' for c in dev_data[i]['dataItemMap'].keys()]
            meta = list(set(meta).union(set(cols)))

        meta = [c.split('__') for c in meta]
        df_data_dev = json_normalize(data=dev_data,
                                     meta=meta)
        
        df_data_dev.columns = [c.replace('.', '_') for c in df_data_dev.columns]
        df_data_dev['datetime'] = df_data_dev['collectTime'].apply(lambda x: datetime.fromtimestamp(int(x)/1000))
        
        return df_data_dev
    
    
    




