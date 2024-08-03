import requests
import json
import pandas as pd
from pandas import json_normalize
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
    
    def get_site_details_as_df(self) -> pd.DataFrame:
        site_details = self.get_site_details()
        df_site_details = json_normalize(data=site_details, 
                                         meta=['id', 
                                               'name',
                                               'accountId',
                                               'peakPower',
                                               'lastUpdateTime',
                                               'installationDate',
                                               'ptoDate',
                                               'notes',
                                               'type',
                                               ['location', 'country'],
                                               ['location', 'city'],
                                               ['location', 'address'],
                                               ['location', 'zip'],
                                               ['location', 'timeZone'],
                                               ['location', 'countryCode'],
                                               ['primaryModule', 'manufacturerName'],
                                               ['primaryModule', 'modelName'],
                                               ['primaryModule', 'maximumPower'],
                                               ['primaryModule', 'temperatureCoef']])
        
        df_site_details.columns = [c.replace('.', '_') for c in df_site_details.columns]
        df_site_details.rename(columns={'name':'site_name', 'id':'site_id'}, inplace=True)
        return df_site_details
    
    def get_inverter_data_as_df(self, 
                                serial_number:str,
                                start_time:datetime,
                                end_time:datetime) -> pd.DataFrame:
        
        inv_data = self.get_inverter_data(serial_number=serial_number,
                                          start_time=start_time,
                                          end_time=end_time)
        df_inv_data = json_normalize(data=inv_data,
                                             meta=['date',
                                                   'totalActivePower',
                                                   'dcVoltage',
                                                   'powerLimit',
                                                   'totalEnergy',
                                                   'temperature',
                                                   'operationMode',
                                                   ['L1Data', 'acCurrent'],
                                                   ['L1Data', 'acVoltage'],
                                                   ['L1Data', 'acFrequency'],
                                                   ['L1Data', 'apparentPower'],
                                                   ['L1Data', 'activePower'],
                                                   ['L1Data', 'reactivePower'],
                                                   ['L1Data', 'cosPhi'],
                                                   ['L2Data', 'acCurrent'],
                                                   ['L2Data', 'acVoltage'],
                                                   ['L2Data', 'acFrequency'],
                                                   ['L2Data', 'apparentPower'],
                                                   ['L2Data', 'activePower'],
                                                   ['L2Data', 'reactivePower'],
                                                   ['L2Data', 'cosPhi'],
                                                   ['L3Data', 'acCurrent'],
                                                   ['L3Data', 'acVoltage'],
                                                   ['L3Data', 'acFrequency'],
                                                   ['L3Data', 'apparentPower'],
                                                   ['L3Data', 'activePower'],
                                                   ['L3Data', 'reactivePower'],
                                                   ['L3Data', 'cosPhi'], ])
                
        df_inv_data.columns = [c.replace('.', '_') for c in df_inv_data.columns]
        df_inv_data.rename(columns={'date':'datetime'}, inplace=True)
        return df_inv_data


