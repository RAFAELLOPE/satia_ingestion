import requests
import json
from argparse import ArgumentParser
import os
from pandas import json_normalize

class MeteoExtractor(object):
    def __init__(self, config) -> None:
        self.api_key = config['API_KEY']
        self.flexi_base = 'https://www.meteosource.com/api/v1/flexi/'
    

    def get_coordinates(self, place:str, timezone:str = 'Europe/Madrid') -> dict:
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
                      timezone:str = 'Europe/Madrid') -> list:
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

def main(args):
    with open(args.config_file) as f:
        config = json.load(f)
    
    meteo_credentials = config["METEOSOURCE"]
    meteo_extractor = MeteoExtractor(meteo_credentials)
    coordinates = meteo_extractor.get_coordinates(place='Granada')
    lon = coordinates['lon']
    lat = coordinates['lat']
    data = meteo_extractor.get_hist_data(lat=lat, lon=lon, date='2024-01-01')
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
    
    df.to_csv(os.path.join(os.getcwd(), 'data', 'Meto', 'meto_data_Granada.csv'),index=False)
    return



if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('--config_file',
                        type=str,
                        required=False,
                        default=os.path.join(os.getcwd(), 'config.json'))
    
    args = parser.parse_args()
    main(args)   

