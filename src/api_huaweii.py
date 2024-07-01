import requests
import json
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import List

class HuaweiiExtractor(object):
    def __init__(self, config:dict, intl:str = 'eu5'):
        self.user = config["USER"]
        self.password = config["PASSWORD"]
        self.login = f'https://{intl}.fusionsolar.huawei.com/thirdData/login'
        self.logout = f'https://{intl}.fusionsolar.huawei.com/thirdData/logout'
        self.plant_list = f'https://{intl}.fusionsolar.huawei.com/thirdData/stations'
        self.device_list = f'https://{intl}.fusionsolar.huawei.com/thirdData/getDevList'
        self.device_data = f'https://{intl}.fusionsolar.huawei.com/thirdData/getDevHistoryKpi'
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
    
    def get_device_data(self, devices:List[str], start_time:int, end_time:int) -> dict:
        data = {"userName":self.user,
                "systemCode": self.password}
        dev_data = []
        try:
            res = requests.post(self.login, 
                                headers=self.header, 
                                json=data)
            res_data = json.loads(res.content)
            if res_data['success'] == True:
                self.token = res.headers["xsrf-token"]
                header = {"XSRF-TOKEN": self.token}
                body = {"devIds": ",".join(devices),
                        "devTypeId":1,
                        "startTime": start_time,
                        "endTime": end_time}
                
                res = requests.post(self.device_data,
                                    headers=header,
                                    json=body)
                res_data = json.loads(res.content)
                if res_data['success'] == True:
                    return res_data
            res = requests.post(self.logout,
                                headers=header,
                                json={'xsrfToken' : self.token})
        except requests.exceptions.RequestException as e:
            raise e
        return



with open(os.path.abspath(os.path.join(os.getcwd(), 'config.json'))) as f:
    config = json.load(f)
    extractor = HuaweiiExtractor(config["HUAWEII"])
    plants = extractor.get_plant_list()
    df_plants = pd.DataFrame(plants)
    plantCodes = df_plants.plantCode.unique()
    devices = extractor.get_device_list(plantCodes)
    df_devices = pd.DataFrame(devices)
    df_devices = df_devices[df_devices.devTypeId == 1]
    df_devices = df_devices.drop(['latitude', 'longitude'], axis=1)
    df_devices.rename(columns={'stationCode':'plantName'}, inplace=True)
    df = pd.merge(df_devices, df_plants, on='plantName', how='inner')
    
    for pl in range(len(df_plants)):
        start_date = df_plants.loc[pl, 'gridConnectionDate']
        plant = df_plants.loc[pl, 'plantCode']
        start_date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S%z")
        end_date = start_date + timedelta(days=3)
        devices = list(df[df['plantCode'] == plant]['id'])
        
        while end_date <= datetime.now():
            start_time = int(start_date.timestamp() * 1000)
            end_time = int(end_date.timestamp() * 1000)
            dev_data = extractor.get_device_data(devices, start_time, end_time)

            df_data = json.json_normalize(dev_data['data'],
                                          meta=['devId',
                                                'sn', 
                                                'collectTime', 
                                                ['dataItemMap', 'pv26_i'],
                                                ['dataItemMap', 'pv2_u'],
                                                ['dataItemMap', 'pv28_i'],
                                                ['dataItemMap', 'pv4_u'],
                                                ['dataItemMap', 'pv22_i'],
                                                ['dataItemMap', 'power_factor'],
                                                ['dataItemMap', 'pv6_u'],
                                                ['dataItemMap', 'pv24_i'],
                                                ['dataItemMap', 'mppt_total_cap'],
                                                ['dataItemMap', 'pv8_u'],
                                                ['dataItemMap', 'pv22_u'],
                                                ['dataItemMap', 'open_time'],
                                                ['dataItemMap', 'a_i'],
                                                ['dataItemMap', 'pv24_u'],
                                                ['dataItemMap', 'c_i'],
                                                ['dataItemMap', 'mppt_9_cap'],
                                                ['dataItemMap', 'pv20_u'],
                                                ['dataItemMap', 'pv19_u'],
                                                ['dataItemMap', 'pv15_u'],
                                                ['dataItemMap', 'a_u'],
                                                ['dataItemMap', 'reactive_power'],
                                                ['dataItemMap', 'pv17_u'],
                                                ['dataItemMap', 'c_u'],
                                                ['dataItemMap', 'mppt_8_cap'],
                                                ['dataItemMap', 'pv20_i'],
                                                ['dataItemMap', 'pv15_i'],
                                                ['dataItemMap', 'efficiency'],
                                                ['dataItemMap', 'pv17_i'],
                                                ['dataItemMap', 'pv11_i'],
                                                ['dataItemMap', 'pv13_i'],
                                                ['dataItemMap', 'pv11_u'],
                                                ['dataItemMap', 'mppt_power'],
                                                ['dataItemMap', 'pv13_u'],
                                                ['dataItemMap', 'close_time'],
                                                ['dataItemMap', 'pv19_i'],
                                                ['dataItemMap', 'mppt_7_cap'],
                                                ['dataItemMap', 'mppt_5_cap'],
                                                ['dataItemMap', 'pv27_u'],
                                                ['dataItemMap', 'pv2_i'],
                                                ['dataItemMap', 'active_power'],
                                                ['dataItemMap', 'pv4_i'],
                                                ['dataItemMap', 'pv6_i'],
                                                ['dataItemMap', 'pv8_i'],
                                                ['dataItemMap', 'mppt_6_cap'],
                                                ['dataItemMap', 'pv27_i'],
                                                ['dataItemMap', 'pv1_u'],
                                                ['dataItemMap', 'pv3_u'],
                                                ['dataItemMap', 'pv23_i'],
                                                ['dataItemMap', 'pv5_u'],
                                                ['dataItemMap', 'pv25_i'],
                                                ['dataItemMap', 'pv7_u'],
                                                ['dataItemMap', 'pv23_u'],
                                                ['dataItemMap', 'inverter_state'],
                                                ['dataItemMap', 'pv9_u'],
                                                ['dataItemMap', 'pv25_u'],
                                                ['dataItemMap', 'total_cap'],
                                                ['dataItemMap', 'b_i'],
                                                ['dataItemMap', 'mppt_3_cap'],
                                                ['dataItemMap', 'pv21_u'],
                                                ['dataItemMap', 'mppt_10_cap'],
                                                ['dataItemMap', 'pv16_u'],
                                                ['dataItemMap', 'pv18_u'],
                                                ['dataItemMap', 'temperature'],
                                                ['dataItemMap', 'bc_u'],
                                                ['dataItemMap', 'b_u'],
                                                ['dataItemMap', 'pv21_i'],
                                                ['dataItemMap', 'elec_freq'],
                                                ['dataItemMap', 'mppt_4_cap'],
                                                ['dataItemMap', 'pv16_i'],
                                                ['dataItemMap', 'pv18_i'],
                                                ['dataItemMap', 'day_cap'],
                                                ['dataItemMap', 'pv12_i'],
                                                ['dataItemMap', 'pv14_i'],
                                                ['dataItemMap', 'pv12_u'],
                                                ['dataItemMap', 'pv14_u'],
                                                ['dataItemMap', 'mppt_1_cap'],
                                                ['dataItemMap', 'pv10_u'],
                                                ['dataItemMap', 'pv1_i'],
                                                ['dataItemMap', 'pv26_u'],
                                                ['dataItemMap', 'pv3_i'],
                                                ['dataItemMap', 'pv28_u'],
                                                ['dataItemMap', 'mppt_2_cap'],
                                                ['dataItemMap', 'pv5_i'],
                                                ['dataItemMap', 'ab_u'],
                                                ['dataItemMap', 'ca_u'],
                                                ['dataItemMap', 'pv7_i'],
                                                ['dataItemMap', 'pv10_i'],
                                                ['dataItemMap', 'pv9_i']
                                                ])


            start_date = end_date
            end_date = start_date + timedelta(days=3)




