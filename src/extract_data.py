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
                    
                    logging.info(f"Extracted SolarEdge API get_inverter_data method for site={site}, serial_number={serial_number}, start_time={start_time}, end_time={end_time}")
                except Exception as e:
                    logging.error(f"Failed calling SolarEdge API get_inverter_data method for site={site}, serial_number={serial_number}, start_time={start_time}, end_time={end_time}")
                    logging.error(str(e))
                    continue
            
                df_inv_data = pd.DataFrame(inv_data)
                df_inv_data['component_id'] = serial_number

                df_inv_data = pd.merge(df_inv_data, df_components, on='component_id', how='inner')
                df_inv_data = pd.merge(df_inv_data, df_site_details, on='site_id', how='inner')

                idx_cols = ['datetime'] + list(df_site_details.columns) + [c for c in df_components.columns if c!='site_id']
                data_cols = [c for c in df_inv_data.columns if c not in idx_cols]

                df_inv_data = df_inv_data[idx_cols + data_cols].drop_duplicates()

                try:
                    if len(df_inv_data) >  0:
                        df_inv_data.to_csv(f"s3://prod-satia-raw-data/{site}/inverter_details_{start_time}_{serial_number}.csv",
                                           index=False,
                                           storage_options={"key" : aws_access_key_id,
                                                            "secret": aws_secret_key})
                    
                        logging.info(f"Data stored into S3 for site={site}, serial_number={serial_number}, start_time={start_time}, end_time={end_time}")
                    else:
                        logging.warning(f"No data retrieved for site={site}, serial_number={serial_number}, start_time={start_time}, end_time={end_time}")
                
                except Exception as e:
                    logging.error(f"Couldn't store inverter data into S3 for site={site}, serial_number={serial_number}, start_time={start_time}, end_time={end_time}")
                    raise(e)

                start_time = end_time
                
    return

def equalize_fronius_dataframes(list_df:list) -> list:
    final_dfs = []
    cols = []
    for df in list_df:
        cols += list(df.columns)
    tot_cols = list(set(cols))
    for df in list_df:
        cols_df = set(df.columns)
        diff_cols = list(set(tot_cols).difference(cols_df))
        if len(diff_cols) > 0:
            for cd in diff_cols:
                df[cd] = None
        final_dfs.append(df[tot_cols])
    return final_dfs


def store_fronius_inverter_data_to_S3(sites:dict,
                                      aws_secret_key:str,
                                      aws_access_key_id: str,
                                      start_time:datetime = None,
                                      end_time:datetime = None):
    
    fronius_ext = FroniusExtractor(sites)
    try:
        df_pvs = fronius_ext.get_pv_systems_and_components()
    except Exception as e:
        logging.error('Failed calling Fronius API get_pv_systems_and_components')
        raise e
    for s, d in zip(df_pvs['pvSystemIds'], df_pvs['deviceIds']):
        # Get PV System details
        try:
            pvs_details = fronius_ext.get_pv_system_details(pv_system_id=s)
            installation_date = datetime.strptime(pvs_details['installationDate'].replace('T', ' ').replace('Z', ''), 
                                                  '%Y-%m-%d %H:%M:%S')
            df_pvs_details = pd.DataFrame([pvs_details])
            logging.info(f'Successfully extracted pv system details for pv_system={s}')
        except Exception as e:
            logging.error(f'Failed calling Fronius API get_pv_system_details method for pv_system={s}')
            logging.error(str(e))

        # Get Device details
        try:
            device_details = fronius_ext.get_device_details(pv_system_id=s, device_id=d)
            df_dev_details = pd.DataFrame([device_details])
            df_dev_details.rename(columns={'peakPower' : 'device_peakPower'}, inplace=True)
            logging.info(f'Successfully extracted pv device details for pv_system={s} and device_id={d}')
        except Exception as e:
            logging.error(f'Failed calling Fronius API get_device_details method for pv_system={s} and device_id={d}')

        start_time = installation_date
        days = 0
        inv_data_list = []
        while start_time <= datetime.now():
            if days <= 7:
                end_time = start_time + timedelta(days=1)
                try:
                    df_inv_data = fronius_ext.get_device_data(pv_system_id = s,
                                                              device_id = d,
                                                              start_time = start_time,
                                                              end_time=end_time)
                    
                    if len(df_inv_data) > 0:
                        df_inv_data['deviceId'] = d
                        df_inv_data['pvSystemId'] = s
                        inv_data_list.append(df_inv_data)
                        logging.info(f"Extracted SolarEdge API get_inverter_data method for system={s}, device={d}, start_time={start_time}, end_time={end_time}")
                        days += 1
                except Exception as e:
                    logging.error(f"Failed calling SolarEdge API get_inverter_data method for system={s}, device={d}, start_time={start_time}, end_time={end_time}")
                    logging.error(str(e))
                start_time = end_time
            else:
                days = 0
                inv_data_eq = equalize_fronius_dataframes(inv_data_list)
                df_inv_data = pd.concat(inv_data_eq)
                try:
                    if len(df_inv_data) > 0:
                        df_inv = pd.merge(df_inv_data, df_dev_details, on='deviceId', how='inner')
                        df_inv = pd.merge(df_inv, df_pvs_details, on='pvSystemId', how='inner')

                        df_inv = df_inv[['datetime'] + [c for c in df_pvs_details.columns if c != 'datetime'] + 
                                        [c for c in df_dev_details.columns if c not in ['datetime', 'pvSystemId']] +
                                        [c for c in df_inv_data.columns if c not in ['datetime', 'pvSystemId', 'deviceId']]]

                        df_inv.to_csv(f"s3://prod-satia-raw-data/{df_inv.loc[0,'name']}/inverter_details_{start_time}_{d}.csv",
                                      index=False,
                                       storage_options={"key" : aws_access_key_id,
                                                       "secret": aws_secret_key})
                except Exception as e:
                    logging.error(f"Couldn't store inverter data into S3 for for system={s}, device={d}, start_time={start_time}, end_time={end_time}")
                    raise(e)


def main(args: ArgumentParser) -> None:
    with open(args.config_file) as f:
        config = json.load(f)

    # store_solaredge_inverter_data_to_S3(sites=config["SOLAREDGE"],
    #                                     aws_secret_key=config["AWS_SECRET_ACCESS_KEY"],
    #                                     aws_access_key_id=config["AWS_ACCESS_KEY_ID"])
    
    store_fronius_inverter_data_to_S3(sites=config["FRONIUS"],
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
