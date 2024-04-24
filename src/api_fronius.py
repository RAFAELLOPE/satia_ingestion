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

    
    def transform_list_pv_systems(self, data:dict) -> pd.DataFrame:
        data_dict = {'pvSystemIds':[]}
        for i in range(data['links']['totalItemsCount']):
            data_dict['pvSystemIds'].append(data['pvSystemIds'][i])
        df = pd.DataFrame(data_dict)
        return df

    def transform_component_list(self, data:dict) -> pd.DataFrame:
        data_dict = {'deviceIds':[]}
        for i in range(data['links']['totalItemsCount']):
            data_dict['deviceIds'].append(data['deviceIds'][i])
        df = pd.DataFrame(data_dict)
        return df

    def transform_device_data(self, data_org:dict) -> pd.DataFrame:
       data = data_org["data"]
       data_dict ={"datetime":[],
                   "total_energy":[],
                   "total_active_power":[],
                   "dc_voltage":[],
                   "dc_current":[],
                   "dc_energy":[],
                   "power_limit_perc":[],
                   "internal_temp":[],
                   "inverter_mode":[],
                   "operation_mode":[],
                   "ac_current_L1":[],
                   "ac_voltage_L1":[],
                   "ac_current_L2":[],
                   "ac_voltage_L2":[],
                   "ac_current_L3":[],
                   "ac_voltage_L3":[],
                   "apparent_power":[],
                   "reactive_power":[],
                   "power_factor":[]}
    
       for i in range(len(data)):
           long_dur = data[i]['logDuration']
           channels = data[i]['channels']
           total_energy = [c['value'] for c in channels if c['channelName'] == 'EnergyExported'][0]
           power_limit_perc = [c['value'] for c in channels if c['channelName'] == 'StandardizedPower'][0]
           total_active_power = total_energy * 3600 / long_dur
           ac_voltage_L1 = [c['value'] for c in channels if c['channelName'] == 'VoltageA'][0]
           ac_voltage_L2 = [c['value'] for c in channels if c['channelName'] == 'VoltageB'][0]
           ac_voltage_L3 = [c['value'] for c in channels if c['channelName'] == 'VoltageC'][0]
           ac_current_L1 = [c['value'] for c in channels if c['channelName'] == 'CurrentA'][0]
           ac_current_L2 = [c['value'] for c in channels if c['channelName'] == 'CurrentB'][0]
           ac_current_L3 = [c['value'] for c in channels if c['channelName'] == 'CurrentC'][0]
           dc_voltage_1 = [c['value'] for c in channels if c['channelName'] == 'VoltageDC1'][0]
           dc_voltage_2 = [c['value'] for c in channels if c['channelName'] == 'VoltageDC2'][0]
           dc_current_1 = [c['value'] for c in channels if c['channelName'] == 'CurrentDC1'][0]
           dc_current_2 = [c['value'] for c in channels if c['channelName'] == 'CurrentDC2'][0]
           reactive_power = [c['value'] for c in channels if c['channelName'] == 'ReactivePower'][0]
           apparent_power = [c['value'] for c in channels if c['channelName'] == 'ApparentPower'][0]
           power_factor = [c['value'] for c in channels if c['channelName'] == 'ApparentPower'][0]
           energy_dc1 = [c['value'] for c in channels if c['channelName'] == 'EnergyDC1'][0]
           energy_dc2 = [c['value'] for c in channels if c['channelName'] == 'EnergyDC2'][0]

           # Append data
           data_dict['datetime'].append(data[i]['logDateTime'])
           data_dict['total_energy'].append(total_energy)
           data_dict['total_active_power'].append(total_active_power)
           data_dict['dc_voltage'].append(dc_voltage_1 + dc_voltage_2)
           data_dict['dc_current'].append(dc_current_1 + dc_current_2)
           data_dict['dc_energy'].append(energy_dc1 + energy_dc2)
           data_dict['power_limit_perc'].append(power_limit_perc)
           data_dict['internal_temp'].append(None)
           data_dict['inverter_mode'].append(None)
           data_dict['operation_mode'].append(None)
           data_dict["ac_current_L1"].append(ac_current_L1)
           data_dict["ac_voltage_L1"].append(ac_voltage_L1)
           data_dict["ac_current_L2"].append(ac_current_L2)
           data_dict["ac_voltage_L2"].append(ac_voltage_L2)
           data_dict["ac_current_L3"].append(ac_current_L3)
           data_dict["ac_voltage_L3"].append(ac_voltage_L3)
           data_dict["apparent_power"].append(apparent_power)
           data_dict["reactive_power"].append(reactive_power)
           data_dict["power_factor"].append(power_factor)
       
       df_r = pd.DataFrame(data_dict)
       return df_r

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
        for pv in df_pv['pvSystemIds'].unique():
            df_dev = self.get_componet_list(pv_system_id=pv)
            df_dev['pvSystemIds'] = pv
            components.append(df_dev)
        
        df_r = pd.concat(components)
        df_r.drop_duplicates(inplace=True)
        return df_r
    
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
    


