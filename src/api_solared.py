import requests
import json
import pandas as pd
from datetime import datetime


    
def transform_component_list(data:dict) -> pd.DataFrame:
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


def get_componet_list(api_key:str, site_id:str) -> pd.DataFrame:
    api_call = f"https://monitoringapi.solaredge.com/equipment/{site_id}/list?"
    api_call = api_call + f"api_key={api_key}"

    try:
        res = requests.get(api_call)
        if(res.ok):
            data = json.loads(res.content)
            df = transform_component_list(data)
        else:
            raise Exception(f"API response not OK: {res.status_code}")
    except requests.exceptions.RequestException as e:
        raise e
    
    return df

def transform_inverter_data(data:dict):
    pass
    
def get_inverter_data(api_key:str,
                      site_id:str,
                      serial_number:str,
                      start_time:datetime,
                      end_time:datetime) -> pd.DataFrame:
    
    time_format = "%Y-%m-%d%20%H:%M:%S"
    start_time = datetime.strftime(start_time, time_format)
    end_time = datetime.strftime(end_time, time_format)

    api_call = f"https://monitoringapi.solaredge.com/equipment/{site_id}/{serial_number}/data?"
    api_call = api_call + f"startTime={start_time}&endTime={end_time}"
    api_call = api_call + f"&api_key={api_key}"

    try:
        res = requests.get(api_call)
        if(res.ok):
            data = json.loads(res.content)
            df = transform_inverter_data(data)
        else:
            raise Exception(f"API response not OK: {res.status_code}")
    except requests.exceptions.RequestException as e:
        raise e
    
    return df