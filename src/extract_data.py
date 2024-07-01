import json
from argparse import ArgumentParser
import os
import sys
from datetime import datetime, timedelta
import logging
from pandas import json_normalize

sys.path.insert(0, os.getcwd())

from src.api_solared import *
from src.api_fronius import *
from src.api_huaweii import *


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


def store_huaweii_inverter_data_to_S3(sites:dict):
    extractor = HuaweiiExtractor(sites)
    extractor.log_in()
    plants = extractor.get_plant_list()
    df_plants = pd.DataFrame(plants)
    plantCodes = df_plants.plantCode.unique()
    devices = extractor.get_device_list(plantCodes)
    df_devices = pd.DataFrame(devices)
    df_devices = df_devices[df_devices.devTypeId == 1]
    df_devices = df_devices.drop(['latitude', 'longitude'], axis=1)
    df_devices.rename(columns={'stationCode':'plantCode'}, inplace=True)
    df = pd.merge(df_devices, df_plants, on='plantCode', how='inner')
    
    for pl in range(len(df_plants)):
        start_date = df_plants.loc[pl, 'gridConnectionDate']
        plant = df_plants.loc[pl, 'plantCode']
        start_date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S%z")
        end_date = start_date + timedelta(days=3)
        devices = list(df[df['plantCode'] == plant]['id'])
        devices = [str(d) for d in devices]
        
        while end_date <= datetime.now(end_date.tzinfo):
            start_time = int(start_date.timestamp() * 1000)
            end_time = int(end_date.timestamp() * 1000)
            dev_data = extractor.get_device_data(devices, start_time, end_time)

            df_data_tmp = json_normalize(dev_data['data'],
                                         meta=['devId',
                                           'sn', 
                                           'collectTime', 
                                                ['dataItemMap', 'pv26_i'],
                                                ['dataItemMap', 'pv2_u'],
                                                ['dataItemMap', 'pv28_i'],
                                                ['dataItemMap', 'pv4_u'],
                                                ['dataItemMap', 'pv22_i'],
                                                ['dataItemMap', 'power_factor'],
                                                ['dataItemMap', 'pv6_u'],
                                                ['dataItemMap', 'pv24_i'],
                                                ['dataItemMap', 'mppt_total_cap'],
                                                ['dataItemMap', 'pv8_u'],
                                                ['dataItemMap', 'pv22_u'],
                                                ['dataItemMap', 'open_time'],
                                                ['dataItemMap', 'a_i'],
                                                ['dataItemMap', 'pv24_u'],
                                                ['dataItemMap', 'c_i'],
                                                ['dataItemMap', 'mppt_9_cap'],
                                                ['dataItemMap', 'pv20_u'],
                                                ['dataItemMap', 'pv19_u'],
                                                ['dataItemMap', 'pv15_u'],
                                                ['dataItemMap', 'a_u'],
                                                ['dataItemMap', 'reactive_power'],
                                                ['dataItemMap', 'pv17_u'],
                                                ['dataItemMap', 'c_u'],
                                                ['dataItemMap', 'mppt_8_cap'],
                                                ['dataItemMap', 'pv20_i'],
                                                ['dataItemMap', 'pv15_i'],
                                                ['dataItemMap', 'efficiency'],
                                                ['dataItemMap', 'pv17_i'],
                                                ['dataItemMap', 'pv11_i'],
                                                ['dataItemMap', 'pv13_i'],
                                                ['dataItemMap', 'pv11_u'],
                                                ['dataItemMap', 'mppt_power'],
                                                ['dataItemMap', 'pv13_u'],
                                                ['dataItemMap', 'close_time'],
                                                ['dataItemMap', 'pv19_i'],
                                                ['dataItemMap', 'mppt_7_cap'],
                                                ['dataItemMap', 'mppt_5_cap'],
                                                ['dataItemMap', 'pv27_u'],
                                                ['dataItemMap', 'pv2_i'],
                                                ['dataItemMap', 'active_power'],
                                                ['dataItemMap', 'pv4_i'],
                                                ['dataItemMap', 'pv6_i'],
                                                ['dataItemMap', 'pv8_i'],
                                                ['dataItemMap', 'mppt_6_cap'],
                                                ['dataItemMap', 'pv27_i'],
                                                ['dataItemMap', 'pv1_u'],
                                                ['dataItemMap', 'pv3_u'],
                                                ['dataItemMap', 'pv23_i'],
                                                ['dataItemMap', 'pv5_u'],
                                                ['dataItemMap', 'pv25_i'],
                                                ['dataItemMap', 'pv7_u'],
                                                ['dataItemMap', 'pv23_u'],
                                                ['dataItemMap', 'inverter_state'],
                                                ['dataItemMap', 'pv9_u'],
                                                ['dataItemMap', 'pv25_u'],
                                                ['dataItemMap', 'total_cap'],
                                                ['dataItemMap', 'b_i'],
                                                ['dataItemMap', 'mppt_3_cap'],
                                                ['dataItemMap', 'pv21_u'],
                                                ['dataItemMap', 'mppt_10_cap'],
                                                ['dataItemMap', 'pv16_u'],
                                                ['dataItemMap', 'pv18_u'],
                                                ['dataItemMap', 'temperature'],
                                                ['dataItemMap', 'bc_u'],
                                                ['dataItemMap', 'b_u'],
                                                ['dataItemMap', 'pv21_i'],
                                                ['dataItemMap', 'elec_freq'],
                                                ['dataItemMap', 'mppt_4_cap'],
                                                ['dataItemMap', 'pv16_i'],
                                                ['dataItemMap', 'pv18_i'],
                                                ['dataItemMap', 'day_cap'],
                                                ['dataItemMap', 'pv12_i'],
                                                ['dataItemMap', 'pv14_i'],
                                                ['dataItemMap', 'pv12_u'],
                                                ['dataItemMap', 'pv14_u'],
                                                ['dataItemMap', 'mppt_1_cap'],
                                                ['dataItemMap', 'pv10_u'],
                                                ['dataItemMap', 'pv1_i'],
                                                ['dataItemMap', 'pv26_u'],
                                                ['dataItemMap', 'pv3_i'],
                                                ['dataItemMap', 'pv28_u'],
                                                ['dataItemMap', 'mppt_2_cap'],
                                                ['dataItemMap', 'pv5_i'],
                                                ['dataItemMap', 'ab_u'],
                                                ['dataItemMap', 'ca_u'],
                                                ['dataItemMap', 'pv7_i'],
                                                ['dataItemMap', 'pv10_i'],
                                                ['dataItemMap', 'pv9_i']
                                                ])

            df_data_tmp['collectTime'] = df_data_tmp['collectTime'].apply(lambda x: datetime.fromtimestamp(x/1000.0))
            try:
                if len(df_data_tmp) >  0:
                    df_data_tmp.to_csv(f"s3://prod-satia-raw-data/{site}/inverter_details_{start_time}_{serial_number}.csv",
                                       index=False,
                                       storage_options={"key" : aws_access_key_id,
                                                        "secret": aws_secret_key})
                    
                    logging.info(f"Data stored into S3 for site={site}, serial_number={serial_number}, start_time={start_time}, end_time={end_time}")
                else:
                    logging.warning(f"No data retrieved for site={site}, serial_number={serial_number}, start_time={start_time}, end_time={end_time}")
                
            except Exception as e:
                logging.error(f"Couldn't store inverter data into S3 for site={site}, serial_number={serial_number}, start_time={start_time}, end_time={end_time}")
                raise(e)
            start_date = end_date
            end_date = start_date + timedelta(days=3)





def main(args: ArgumentParser) -> None:
    with open(args.config_file) as f:
        config = json.load(f)

    if args.api == 'solaredge':
        store_solaredge_inverter_data_to_S3(sites=config["SOLAREDGE"],
                                            aws_secret_key=config["AWS_SECRET_ACCESS_KEY"],
                                            aws_access_key_id=config["AWS_ACCESS_KEY_ID"])
    
    elif args.api == 'fronius':
        store_fronius_inverter_data_to_S3(sites=config["FRONIUS"],
                                          aws_secret_key=config["AWS_SECRET_ACCESS_KEY"],
                                          aws_access_key_id=config["AWS_ACCESS_KEY_ID"])
    elif args.api == 'huaweii':
        store_huaweii_inverter_data_to_S3(sites = config["HUAWEII"],
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
    
    parser.add_argument('--api',
                        type=str,
                        required=False,
                        default='huaweii',
                        options=['huaweii', 'fronius', 'solaredge'])
    
    args = parser.parse_args()
    main(args)
