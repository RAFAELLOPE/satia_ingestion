import json
from argparse import ArgumentParser
import os
import sys
from datetime import datetime, timedelta
sys.path.insert(0, os.getcwd())

from src.api_solared import *
from src.api_fronius import *


def store_solaredge_inverter_data(sites:dict,
                                  satia_usr:str,
                                  satia_pwd:str,
                                  start_time:datetime = None,
                                  end_time:datetime = None):

    for site in sites.keys():
        solaredge_extr = SolarEdgeExtractor(site)
        df_components = solaredge_extr.get_componet_list()
        for j in range(len(df_components)):
            serial_number = df_components.loc[j, 'serialNumber']
            if start_time == None:
                end_time = datetime.now()
                start_time = end_time - timedelta(days=5)

            df_inv_data = solaredge_extr.get_inverter_data(serial_number=serial_number,
                                                           start_time=start_time,
                                                           end_time=end_time)
            df_inv_data.to_csv(os.path.join(os.getcwd(), 'data', f'inverter_details_{serial_number}.csv'))

    return df_inv_data


def store_fronius_inverter_data(sites:dict,
                                satia_usr:str,
                                satia_pwd:str,
                                start_time:datetime = None,
                                end_time:datetime = None):
    
    fronius_ext = FroniusExtractor(sites)
    df_pvs = fronius_ext.get_pv_systems_and_components()
    for s, d in zip(df_pvs['pvSystemIds'], df_pvs['deviceIds']):
        if start_time == None:
            end_time = datetime.now()
            start_time = end_time - timedelta(days=1)
            df_inv_data = fronius_ext.get_device_data(pv_system_id = s,
                                                      device_id = d,
                                                      start_time = start_time,
                                                      end_time=end_time)
            
        df_inv_data.to_csv(os.path.join(os.getcwd(), 
                                        'data', 
                                        'Fronius_data', 
                                        f'inverter_details_{s}_{d}.csv'))





def main(args: ArgumentParser) -> None:
    with open(args.config_file) as f:
        config = json.load(f)
    #sites = config["SITES"]
    satia_db_user = config["SATIADB"]["USER"]
    satia_db_pwd = config["SATIADB"]["PASSWORD"]

    # res_se = store_solaredge_inverter_data(sites=config["SOLAREDGE"],
    #                                        satia_usr = satia_db_user,
    #                                        satia_pwd =satia_db_pwd)

    res_fr = store_fronius_inverter_data(sites=config["FRONIUS"],
                                         satia_usr=satia_db_user,
                                         satia_pwd=satia_db_pwd)
    

    

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('--config_file',
                        type=str,
                        required=False,
                        default=os.path.join(os.getcwd(), 'config.json'))
    args = parser.parse_args()
    main(args)
