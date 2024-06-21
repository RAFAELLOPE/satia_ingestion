import requests
import json
import pandas as pd
from datetime import datetime


class FroniusExtractor(object):
    def __init__(self, config:dict):
        self.api_value = config["API_VALUE"]
        self.api_key = config["API_KEY"]
        self.header = {'AccessKeyId': self.api_key,
                       'AccessKeyValue': self.api_value}

        self.api_list_pv_systems = f'https://api.solarweb.com/swqapi/pvsystems-list'
        self.api_list_devices = f'https://api.solarweb.com/swqapi/pvsystems'
        self.api_dev_historical = f'https://api.solarweb.com/swqapi/pvsystems'

    
    def transform_list_pv_systems(self, data:dict) -> dict:
        data_dict = {'pvSystemIds':[]}
        for i in range(data['links']['totalItemsCount']):
            data_dict['pvSystemIds'].append(data['pvSystemIds'][i])
        return data_dict

    def transform_component_list(self, data:dict) -> dict:
        data_dict = {'deviceIds':[]}
        for i in range(data['links']['totalItemsCount']):
            data_dict['deviceIds'].append(data['deviceIds'][i])
        return data_dict

    def transform_device_data(self, data_org:dict) -> pd.DataFrame:
        data = data_org["data"]
        df_result = pd.DataFrame()

        for i in range(len(data)):
            date_time = data[i]['logDateTime'].replace('T', ' ').replace('Z', '')
            long_dur = data[i]['logDuration']
            channels = data[i]['channels']

            df_channels = pd.DataFrame(channels)
            df_channels_t = df_channels[['channelName', 'value']].T
            df_channels_t = df_channels_t.drop('channelName')\
                                        .reset_index(drop=True)\
                                        .rename_axis(None, axis=1)
            df_channels_t.columns = df_channels['channelName']
            df_channels_t['datetime'] = date_time
            if 'EnergyExported' in df_channels_t.columns:
                df_channels_t['total_active_power'] = df_channels_t['EnergyExported'] * 3600 / long_dur
            else:
                df_channels_t['total_active_power'] = None

            #Fill missing cols
            if i == 0:
                df_result = df_channels_t
            else:
                res_cols = set(df_result.columns)
                chn_cols = set(df_channels_t.columns)
                diff_cols = list(res_cols.difference(chn_cols)) + \
                            list(chn_cols.difference(res_cols))
                for c in diff_cols:
                    if c not in df_channels_t.columns:
                        df_channels_t[c] = None
                    elif c not in df_result.columns:
                        df_result[c] = None
                
                df_channels_t  = df_channels_t[df_result.columns]
                df_result = pd.concat([df_result, df_channels_t])

        df_result.reset_index(drop=True, inplace=True)
        return df_result


    def transform_list_pv_systems_details(self, data:dict) -> dict:
        data_dict = {'pvSystemId': data['pvSystemId'] if 'pvSystemId' in data.keys() else None,
                     'name': data['name'] if 'name' in data.keys() else None,
                     'country': data['address']['country'] if 'country' in data['address'].keys() else None,
                     'zipCode': data['address']['zipCode'] if 'zipCode' in data['address'].keys() else None,
                     'city': data['address']['city'] if 'city' in data['address'].keys() else None,
                     'state': data['address']['state'] if 'state' in data['address'].keys() else None,
                     'peakPower': data['peakPower'] if 'peakPower' in data.keys() else None,
                     'installationDate': data['installationDate'] if 'installationDate' in data.keys() else None,
                     'timeZone': data['timeZone'] if 'timeZone' in data.keys() else None}
        
        return data_dict
        

    def transform_device_details(self, data:dict) -> dict:
        data_dict = {'deviceType' : data['deviceType'] if 'deviceType' in data.keys() else None,
                     'deviceId': data['deviceId'] if 'deviceId' in data.keys() else None,
                     'deviceName': data['deviceName'] if 'deviceName' in data.keys() else None,
                     'deviceManufacturer': data['deviceManufacturer'] if 'deviceManufacturer' in data.keys() else None,
                     'deviceCategory': data['deviceCategory'] if 'deviceCategory' in data.keys() else None,
                     'deviceLocation': data['deviceLocation'] if 'deviceLocation' in data.keys() else None,
                     'deviceTypeDetails': data['deviceTypeDetails'] if 'deviceTypeDetails' in data.keys() else None,
                     'serialNumber': data['serialNumber'] if 'serialNumber' in data.keys() else None,
                     'numberPhases': data['numberPhases'] if 'numberPhases' in data.keys() else None,
                     'peakPower': sum([c if c!=None else 0 for c in data['peakPower'].values()]),
                     'nominalAcPower': data['nominalAcPower'] if 'nominalAcPower' in data.keys() else None}
        return data_dict

    def get_pv_system_details(self, pv_system_id:str) -> dict:
        api_call = self.api_list_devices + f'/{pv_system_id}'
        try:
            res = requests.get(api_call, headers=self.header)
            if (res.ok):
                data = json.loads(res.content)
                df = self.transform_list_pv_systems_details(data)
            else:
                raise Exception(f"API response not OK: {res.status_code}")
        except requests.exceptions.RequestException as e:
            raise e
        return df

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


    def get_componet_list(self, pv_system_id:str) -> pd.DataFrame:
        api_call = self.api_list_devices + f'/{pv_system_id}/devices-list'
        try:
            res = requests.get(api_call, headers=self.header)
            if(res.ok):
                data = json.loads(res.content)
                df = self.transform_component_list(data)
            else:
                raise Exception(f"API response not OK: {res.status_code}")
        except requests.exceptions.RequestException as e:
            raise e
        return df


    def get_pv_systems_and_components(self) -> pd.DataFrame:
        components = []
        df_pv = self.get_pv_system_list()
        for pv in set(df_pv['pvSystemIds']):
            df_dev = self.get_componet_list(pv_system_id=pv)
            df_dev = pd.DataFrame(df_dev)
            df_dev['pvSystemIds'] = pv
            components.append(df_dev)
        
        df_r = pd.concat(components)
        df_r.drop_duplicates(inplace=True)
        return df_r
    

    def get_device_details(self,
                           pv_system_id:str,
                           device_id:str):
        api_call = self.api_list_devices + f'/{pv_system_id}/devices/{device_id}'
        try:
            res = requests.get(api_call, headers=self.header)
            if(res.ok):
                data = json.loads(res.content)
                df = self.transform_device_details(data)
            else:
                raise Exception(f"API response not OK: {res.status_code}")
        except requests.exceptions.RequestException as e:
            raise e
        return df

    def get_device_data(self,
                        pv_system_id:str,
                        device_id:str,
                        start_time:datetime,
                        end_time:datetime) -> pd.DataFrame:
    
        time_format = "%Y-%m-%d %H:%M:%S"
        start_time = datetime.strftime(start_time, time_format).replace(' ', 'T')
        end_time = datetime.strftime(end_time, time_format).replace(' ', 'T')

        api_call = self.api_dev_historical + f'/{pv_system_id}/devices/{device_id}/histdata?'
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
    


