import json
from argparse import ArgumentParser
import os
import sys
from datetime import datetime, timedelta
import logging
sys.path.insert(0, os.getcwd())

from src.api_solared import *
from src.api_fronius import *


def store_solaredge_inverter_data_to_S3(sites:dict,
                                        aws_access_key_id,
                                        aws_secret_key,
                                        start_time:datetime = None,
                                        end_time:datetime = None):

    for site in sites.keys():
        solaredge_extr = SolarEdgeExtractor(sites[site])
        try:
            site_details = solaredge_extr.get_site_details()
        except Exception as e:
            logging.error('Failed calling SolarEdge API get_site_details method')
            raise e
        
        installation_date = datetime.strptime(site_details['installation_date'], '%Y-%m-%d')
        site_id = site_details['site_id']
        df_site_details = pd.DataFrame([site_details])
        df_site_details.rename(columns={'name':'site_name'}, inplace=True)

        try:
            components = solaredge_extr.get_componet_list()
        except Exception as e:
            logging.error('Failed calling SolarEdge API get_component_list method')
            raise e
        
        df_components = pd.DataFrame(components)
        df_components['site_id'] = site_id
        df_components.rename(columns={'serialNumber' : 'component_id',
                                      'name':'component_name'}, inplace=True)

        for j in range(len(df_components)):
            serial_number = df_components.loc[j,'component_id']
    
            start_time = installation_date
            while start_time <= datetime.now():
                end_time = start_time + timedelta(days=5)

                try:
                    inv_data = solaredge_extr.get_inverter_data(serial_number=serial_number,
                                                                start_time=start_time,
                                                                 end_time=end_time)
                    
                    logging.info(f"Extracted SolarEdge API get_inverter_data method for serial_number={serial_number}, start_time={start_time}, end_time={end_time}")
                except Exception as e:
                    logging.error(f"Failed calling SolarEdge API get_inverter_data method for serial_number={serial_number}, start_time={start_time}, end_time={end_time}")
                    continue
            
                df_inv_data = pd.DataFrame(inv_data)
                df_inv_data['component_id'] = serial_number

                df_inv_data = pd.merge(df_inv_data, df_components, on='component_id', how='inner')
                df_inv_data = pd.merge(df_inv_data, df_site_details, on='site_id', how='inner')

                idx_cols = ['datetime'] + list(df_site_details.columns) + [c for c in df_components.columns if c!='site_id']
                data_cols = [c for c in df_inv_data.columns if c not in idx_cols]

                df_inv_data = df_inv_data[idx_cols + data_cols].drop_duplicates()

                try:
                    
                    df_inv_data.to_csv(f"s3://prod-satia-raw-data/{site}/inverter_details_{start_time}_{serial_number}.csv",
                                       index=False,
                                       storage_options={"key" : aws_access_key_id,
                                                        "secret": aws_secret_key})
                    
                    logging.info(f"Data stored into S3 for site={site}, serial_number={serial_number}, start_time={start_time}, end_time={end_time}")
                
                except Exception as e:
                    logging.error(f"Couldn't store inverter data into S3 for serial_number={serial_number}, start_time={start_time}, end_time={end_time}")
                    raise(e)

                start_time = end_time
                
    return df_inv_data



def store_fronius_inverter_data_to_S3(sites:dict,
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

    res_se = store_solaredge_inverter_data_to_S3(sites=config["SOLAREDGE"],
                                                 aws_secret_key=config["AWS_SECRET_ACCESS_KEY"],
                                                 aws_access_key_id=config["AWS_ACCESS_KEY_ID"])

    

    

if __name__ == "__main__":
    logging.basicConfig(filename=os.path.join(os.getcwd(), 'logs', 'upload.log'),
                        filemode='w', 
                        format='%(name)s - %(levelname)s - %(message)s')
    parser = ArgumentParser()
    parser.add_argument('--config_file',
                        type=str,
                        required=False,
                        default=os.path.join(os.getcwd(), 'config.json'))
    args = parser.parse_args()
    main(args)
