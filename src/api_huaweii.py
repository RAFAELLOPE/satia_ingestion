import requests
import json
import os
import pandas as pd

class HuaweiiExtractor(object):
    def __init__(self, config:dict, intl:str = 'eu5'):
        self.user = config["USER"]
        self.password = config["PASSWORD"]
        self.login = f'https://{intl}.fusionsolar.huawei.com/thirdData/login'
        self.logout = f'https://{intl}.fusionsolar.huawei.com/thirdData/logout'
        self.plant_list = f'https://{intl}.fusionsolar.huawei.com/thirdData/stations'
        self.device_list = f'https://{intl}.fusionsolar.huawei.com/thirdData/getDevList'
        self.header = {"Content-Type": "application/json"}
        self.token = None


    def get_device_list(self, plants:list) -> list:
        data = {"userName":self.user,
                "systemCode": self.password}
        devices = []
        try:
            res = requests.post(self.login, 
                                headers=self.header, 
                                json=data)
            res_data = json.loads(res.content)
            if res_data['success'] == True:
                self.token = res.headers["xsrf-token"]
                header = {"XSRF-TOKEN": self.token}
                body = {'stationCodes': ','.join(plants)}
                res = requests.post(self.device_list,
                                    headers=header,
                                    json=body)
                res_data = json.loads(res.content)
                if res_data['success'] == True:
                    devices =  res_data['data']
            res = requests.post(self.logout,
                                headers=header,
                                json={'xsrfToken' : self.token})
        except requests.exceptions.RequestException as e:
            raise e
        return devices

    def get_plant_list(self) -> list:
        data = {"userName":self.user,
                "systemCode": self.password}
        plants = []
        try:
            res = requests.post(self.login, 
                                headers=self.header, 
                                json=data)
            res_data = json.loads(res.content)
            if res_data['success'] == True:
                self.token = res.headers["xsrf-token"]
                header = {"XSRF-TOKEN": self.token}
                body = {'pageNo': 1}
                res = requests.post(self.plant_list,
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
            res = requests.post(self.logout,
                                headers=header,
                                json={'xsrfToken' : self.token})
        except requests.exceptions.RequestException as e:
            raise e
        return plants

with open(os.path.abspath(os.path.join(os.getcwd(), 'config.json'))) as f:
    config = json.load(f)
    extractor = HuaweiiExtractor(config["HUAWEII"])
    plants = extractor.get_plant_list()
    df_plants = pd.DataFrame(plants)
    plantCodes = df_plants.plantCode.unique()
    devices = extractor.get_device_list(plantCodes)
    df_devices = pd.DataFrame(devices)


