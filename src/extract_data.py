import json
from argparse import ArgumentParser
import os
import sys
from datetime import datetime, timedelta
from supabase import create_client, Client
sys.path.insert(0, os.getcwd())

from src.api_solared import *
from src.api_fronius import *


def store_solaredge_inverter_data(sites:dict,
                                  satia_url:str,
                                  satia_key:str,
                                  start_time:datetime = None,
                                  end_time:datetime = None):

    supa_client = create_client(satia_url, satia_key)
    acc_id = supa_client.table('account')\
                        .eq('acc_name', 'dummy1')\
                        .select('account_id')\
                        .execute()
    
    for site in sites.keys():
        solaredge_extr = SolarEdgeExtractor(sites[site])
        site_details = solaredge_extr.get_site_details()
        site_details['account_id'] = acc_id
        installation_date = site_details['installation_date']
        
        res = supa_client.table("site")\
                         .insert(site_details)\
                         .execute()
        
        site_id = res['data']['id']
        components = solaredge_extr.get_componet_list()
        components['site_id'] = site_id

        res_comp = supa_client.table("component")\
                              .insert(components)\
                              .execute()

        comp_id = res_comp['data']['id']

        for j in range(len(components)):
            serial_number = components['serialNumber'][j]
            
            start_time = installation_date
            while start_time <= datetime.now():
                end_time = start_time + timedelta(days=5)
                inv_data = solaredge_extr.get_inverter_data(serial_number=serial_number,
                                                               start_time=start_time,
                                                               end_time=end_time)
            
                inv_data['component_id'] = comp_id

                res_comp = supa_client.table("inverter_ts")\
                                      .insert(inv_data)\
                                      .execute()

                start_time = end_time + timedelta(days=1)

                df_inv_data = pd.DataFrame(inv_data)

                storage_path = os.path.join(os.getcwd(), 
                                            'data',
                                            'SolarEdge_data',
                                            f'{site_id}')
                if os.path.exists(storage_path):
                    df_inv_data.to_csv(os.path.join(storage_path, f'inverter_details_{serial_number}.csv'), 
                                       index=False)
                else:
                    os.makedirs(storage_path)
                    df_inv_data.to_csv(os.path.join(storage_path, f'inverter_details_{serial_number}.csv'), 
                                       index=False)


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
    satia_db_url = config["SATIADB"]["URL"]
    satia_db_key = config["SATIADB"]["KEY"]

    res_se = store_solaredge_inverter_data(sites=config["SOLAREDGE"],
                                           satia_url = satia_db_url,
                                           satia_key =satia_db_key)

    # res_fr = store_fronius_inverter_data(sites=config["FRONIUS"],
    #                                      satia_usr=satia_db_user,
    #                                      satia_pwd=satia_db_pwd)
    

    

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument('--config_file',
                        type=str,
                        required=False,
                        default=os.path.join(os.getcwd(), 'config.json'))
    args = parser.parse_args()
    main(args)
