import requests
import json
import pandas as pd
from datetime import datetime
import os


class SolarEdgeExtractor(object):
    def __init__(self, config:dict):
        self.site_id = config["SITE_ID"]
        self.api_key = config["API_KEY"]
        self.component_list_api = f'https://monitoringapi.solaredge.com/equipment/{self.site_id}/list?api_key={self.api_key}'
        self.inverter_data_api = f'https://monitoringapi.solaredge.com/equipment/{self.site_id}/'

    
    def transform_component_list(self, data:dict) -> pd.DataFrame:
        df_dict = {'name':[],
                  'manufacturer':[],
                  'model':[],
                  'serialNumber':[],
                  'kWpDC':[]}

        for i in range(data['reporters']['count']):
            df_dict['name'].append(data['reporters']['list'][i]['name'])
            df_dict['manufacturer'].append(data['reporters']['list'][i]['manufacturer'])
            df_dict['model'].append(data['reporters']['list'][i]['model'])
            df_dict['serialNumber'].append(data['reporters']['list'][i]['serialNumber'])
            df_dict['kWpDC'].append(data['reporters']['list'][i]['kWpDC'])
    
        df = pd.DataFrame(df_dict)
        return df

    def transform_inverter_data(self, data:dict) -> pd.DataFrame:
        data = data['data']
        df_dict = {"datetime":[],
                  "total_active_power":[],
                  "dc_voltage":[],
                  "power_limit_perc":[],
                  "total_energy":[],
                  "internal_temp":[],
                  "inverter_mode":[],
                  "operation_mode":[],
                  "vL1To2":[],
                  "vL2To3":[],
                  "vL3To1":[]}
    
        for l in ["L1", "L2", "L3"]:
            df_dict[f"ac_current_{l}"] = []
            df_dict[f"ac_voltage_{l}"] = []
            df_dict[f"ac_frequency_{l}"] = []
            df_dict[f"apparent_power_{l}"] = []
            df_dict[f"active_power_{l}"] = []
            df_dict[f"reactive_power_{l}"] = []
            df_dict[f"cos_phi_{l}"] = []

        for i in range(data['count']):
            df_dict["datetime"].append(data['telemetries'][i]['date'])
            df_dict["total_active_power"].append(data['telemetries'][i]['totalActivePower'])
            df_dict["dc_voltage"].append(data['telemetries'][i]['dcVoltage'])
            df_dict["power_limit_perc"].append(data['telemetries'][i]['powerLimit'])
            df_dict["total_energy"].append(data['telemetries'][i]['totalEnergy'])
            df_dict["internal_temp"].append(data['telemetries'][i]['temperature'])
            df_dict["inverter_mode"].append(data['telemetries'][i]['inverterMode'])
            df_dict["operation_mode"].append(data['telemetries'][i]['operationMode'])
            df_dict["vL1To2"].append(data['telemetries'][i]['vL1To2'])
            df_dict["vL2To3"].append(data['telemetries'][i]['vL2To3'])
            df_dict["vL3To1"].append(data['telemetries'][i]['vL3To1'])
            for j, l in enumerate(["L1Data", "L2Data", "L3Data"]):
                df_dict[f"ac_current_L{j+1}"].append(data['telemetries'][i][l]['acCurrent'])
                df_dict[f"ac_voltage_L{j+1}"].append(data['telemetries'][i][l]['acVoltage'])
                df_dict[f"ac_frequency_L{j+1}"].append(data['telemetries'][i][l]['acFrequency'])
                df_dict[f"apparent_power_L{j+1}"].append(data['telemetries'][i][l]['apparentPower'])
                df_dict[f"active_power_L{j+1}"].append(data['telemetries'][i][l]['activePower'])
                df_dict[f"reactive_power_L{j+1}"].append(data['telemetries'][i][l]['reactivePower'])
                df_dict[f"cos_phi_L{j+1}"].append(data['telemetries'][i][l]['cosPhi'])

        df = pd.DataFrame(df_dict)
        return df


    def get_componet_list(self) -> pd.DataFrame:
        try:
            res = requests.get(self.component_list_api)
            if(res.ok):
                data = json.loads(res.content)
                df = self.transform_component_list(data)
            else:
                raise Exception(f"API response not OK: {res.status_code}")
        except requests.exceptions.RequestException as e:
            raise e
        return df

    
    def get_inverter_data(self,
                          serial_number:str,
                          start_time:datetime,
                          end_time:datetime) -> pd.DataFrame:
    
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
                df = self.transform_inverter_data(data)
            else:
                raise Exception(f"API response not OK: {res.status_code}")
        except requests.exceptions.RequestException as e:
            raise e
        return df
    


