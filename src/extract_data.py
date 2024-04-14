import json
from argparse import ArgumentParser
import os
import sys
from datetime import datetime, timedelta
sys.path.insert(0, os.getcwd())

from src.api_solared import *


def store_inverter_data(sites:dict,
                        satia_usr:str,
                        satia_pwd:str,
                        start_time:datetime = None,
                        end_time:datetime = None):

    for site in sites.keys():
        site_id = sites[site]["SITE_ID"]
        api_key = sites[site]["API_KEY"]
        df_components = get_componet_list(api_key=api_key, 
                                          site_id=site_id)
        for j in range(len(df_components)):
            serial_number = df_components.loc[j, 'serialNumber']
            if start_time == None:
                end_time = datetime.now()
                start_time = end_time - timedelta(days=5)

            df_inv_data = get_inverter_data(api_key=api_key,
                                                site_id=site_id,
                                                serial_number=serial_number,
                                                start_time=start_time,
                                                end_time=end_time)
            df_inv_data.to_csv(os.path.join(os.getcwd(), 'data', f'inverter_details_{serial_number}.csv'))

    return df_inv_data


def store_component_list(sites:dict, 
                         satia_usr:str, 
                         satia_pwd:str)-> pd.DataFrame:
    for site in sites.keys():
        site_id = sites[site]["SITE_ID"]
        api_key = sites[site]["API_KEY"]
        df_components = get_componet_list(api_key=api_key, 
                                          site_id=site_id)
    
    return df_components

def main(args: ArgumentParser) -> None:
    with open(args.config_file) as f:
        config = json.load(f)
    sites = config["SITES"]
    satia_db_user = config["SATIADB"]["USER"]
    satia_db_pwd = config["SATIADB"]["PASSWORD"]

    res_inf = store_inverter_data(sites=sites,
                                  satia_usr = satia_db_user,
                                  satia_pwd =satia_db_pwd)

    # res_comp = store_component_list(sites=sites,
    #                                 satia_usr=satia_db_user,
    #                                 satia_pwd=satia_db_pwd)
    

    

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('--config_file',
                        type=str,
                        required=False,
                        default=os.path.join(os.getcwd(), 'config.json'))
    args = parser.parse_args()
    main(args)
