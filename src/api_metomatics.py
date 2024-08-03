import requests
import json
from pandas import json_normalize
from datetime import datetime, timedelta
import pandas as pd

class MeteoExtractor(object):
    def __init__(self, config) -> None:
        self.api_key = config['API_KEY']
        self.flexi_base = 'https://www.meteosource.com/api/v1/flexi/'
    

    def get_coordinates(self, 
                        place:str, 
                        timezone:str = 'Europe/Madrid') -> dict:
        endpoint = self.flexi_base + f'find_places'
        payload = {'language' : 'en',
                   'key': self.api_key,
                   'text': place}
        try:
            res = requests.get(endpoint, params=payload)
            data = json.loads(res.content)
            for i in range(len(data)):
                if data[i]['timezone'] == timezone:
                    return data[i]
        except requests.exceptions.RequestException as e:
            raise e



    def get_hist_data(self, 
                      lat:str, 
                      lon:str, 
                      date:str, 
                      timezone:str = 'Europe/Madrid') -> pd.DataFrame:
        endpoint = self.flexi_base + f'time_machine'
        payload = {'lat' : lat,
                   'lon' : lon,
                   'date': date,
                   'timezone': timezone,
                   'units': 'metric',
                   'language': 'en',
                   'key': self.api_key}

        try:
            res = requests.get(endpoint, params=payload)
            data = json.loads(res.content)
            return data['data']
        except requests.exceptions.RequestException as e:
            raise e
        
    def get_wheather_data(self, 
                         start_date:str, 
                         end_date:str,
                         timezone:str = 'Europe/Madrid',
                         lon:str = None,
                         lat:str = None,
                         place:str = None) -> pd.DataFrame:
        
        if (lon == None) or (lat == None):
            try:
                coordinates = self.get_coordinates(place=place, 
                                                   timezone=timezone)
                lon = coordinates['lon']
                lat = coordinates['lat']
            except Exception as e:
                coordinates = self.get_coordinates(place='Granada', 
                                                   timezone=timezone)
                lon = coordinates['lon']
                lat = coordinates['lat']
        
        df_w = pd.DataFrame()

        while start_date <= end_date:
            date = datetime.strftime(start_date, "%Y-%m-%d")
            data = self.get_hist_data(lat=lat, 
                                      lon=lon, 
                                      date=date, 
                                      timezone=timezone)
            df = json_normalize(data=data,
                        meta=['date', 
                              'weather', 
                              'summary', 
                              'icon', 
                              'temperature',
                              'feels_like',
                              'wind_chill',
                              'soil_temperature',
                              'dew_point',
                              'surface_temperature',
                              ['wind', 'speed'],
                              ['wind', 'gusts'],
                              ['wind', 'angle'],
                              ['wind', 'dir'],
                              ['cloud_cover', 'total'],
                              ['cloud_cover', 'low'],
                              ['cloud_cover', 'middle'],
                              ['cloud_cover', 'high'],
                              'pressure',
                              ['precipitation', 'total'],
                              ['precipitation', 'type'],
                              'cape',
                              'evaporation',
                              'irradiance',
                              'ozone',
                              'humidity'])
            df.columns = [c.replace('.', '_') for c in df.columns]
            df_w = pd.concat([df_w, df])
            start_date += timedelta(days=1)
        df_w.rename(columns={'date':'datetime'}, inplace=True)
        df_w['datetime'] = df_w['datetime'].apply(lambda x: x.replace('T', ' '))
        return df_w



