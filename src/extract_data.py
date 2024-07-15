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
from src.api_metomatics import *
from src.api_aws import *





def store_solaredge_inverter_data_to_S3(sites:dict,
                                        meteo_credentials:dict,
                                        aws_access_key_id,
                                        aws_secret_key,
                                        coordinates:dict,
                                        start_time:datetime = None,
                                        end_time:datetime = None):

    meteo_extractor = MeteoExtractor(meteo_credentials)
    aws_s3 = AWS3Extractor(aws_secret_key=aws_secret_key,
                           aws_access_key_id=aws_access_key_id)

    for site in sites.keys():
        solaredge_extr = SolarEdgeExtractor(sites[site])
        try:
            df_site_details = solaredge_extr.get_site_details_as_df()
        except Exception as e:
            logging.error('Failed calling SolarEdge API get_site_details method')
            raise e
        
        installation_date = datetime.strptime(df_site_details.loc[0,'installationDate'], '%Y-%m-%d')
        site_id = df_site_details.loc[0, 'site_id']
        city = df_site_details.loc[0, 'location_city']
        timezone = df_site_details.loc[0, 'location_timeZone']
        
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
    
            start_time = aws_s3.get_last_data_date(folder=f'SolarEdge/{site}/PlantData')
            if start_time == None:
                start_time = installation_date
            
            while start_time <= datetime.now():
                end_time = start_time + timedelta(days=5)

                try:
                    if site in coordinates.keys():
                        lon = coordinates[site]["lon"]
                        lat = coordinates[site]["lat"]
                        df_meteo = meteo_extractor.get_wheather_data(start_date=start_time,
                                                                     end_date=end_time,
                                                                     timezone=timezone,
                                                                     lon=lon,
                                                                     lat=lat)
                    else:
                        df_meteo = meteo_extractor.get_wheather_data(start_date=start_time,
                                                                     end_date=end_time,
                                                                     timezone=timezone,
                                                                     place=city)
                except Exception as e:
                    logging.error(str(e))

                try:
                    df_inv_data = solaredge_extr.get_inverter_data(serial_number=serial_number,
                                                                   start_time=start_time,
                                                                   end_time=end_time)
                    
                    logging.info(f"Extracted SolarEdge API get_inverter_data method for site={site}, serial_number={serial_number}, start_time={start_time}, end_time={end_time}")
                except Exception as e:
                    logging.error(f"Failed calling SolarEdge API get_inverter_data method for site={site}, serial_number={serial_number}, start_time={start_time}, end_time={end_time}")
                    logging.error(str(e))
                    continue
            
                df_inv_data['component_id'] = serial_number
                df_inv_data = pd.merge(df_inv_data, df_components, on='component_id', how='inner')
                df_inv_data = pd.merge(df_inv_data, df_site_details, on='site_id', how='inner')

                idx_cols = ['datetime'] + list(df_site_details.columns) + [c for c in df_components.columns if c!='site_id']
                data_cols = [c for c in df_inv_data.columns if c not in idx_cols]
                df_inv_data = df_inv_data[idx_cols + data_cols].drop_duplicates()
                try:
                    if len(df_inv_data) >  0:
                        aws_s3.store_csv_s3(df = df_inv_data,
                                            folder=f'SolarEdge/{site}/PlantData',
                                            file_name=f'inverter_details_{start_time}_{serial_number}.csv')
                        
                        logging.info(f"Data stored into S3 for site={site}, serial_number={serial_number}, start_time={start_time}, end_time={end_time}")
                    else:
                        logging.warning(f"No data retrieved for site={site}, serial_number={serial_number}, start_time={start_time}, end_time={end_time}")
                
                except Exception as e:
                    logging.error(f"Couldn't store inverter data into S3 for site={site}, serial_number={serial_number}, start_time={start_time}, end_time={end_time}")
                    raise(e)

                try:
                    if len(df_meteo) > 0:
                        aws_s3.store_csv_s3(df = df_meteo,
                                            folder=f'SolarEdge/{site}/WeatherData',
                                            file_name=f'weather_data_{start_time}.csv')
                except Exception as e:
                    raise(e)
                
                start_time = end_time 
    return






def store_fronius_inverter_data_to_S3(sites:dict,
                                      meteo_credentials:dict,
                                      aws_secret_key:str,
                                      aws_access_key_id: str,
                                      coordinates:dict,
                                      start_time:datetime = None,
                                      end_time:datetime = None):
    
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

    
    fronius_ext = FroniusExtractor(sites)
    meteo_extractor = MeteoExtractor(meteo_credentials)
    aws_s3 = AWS3Extractor(aws_secret_key=aws_secret_key,
                           aws_access_key_id=aws_access_key_id)
    
    try:
        df_pvs = fronius_ext.get_pv_systems_and_components()
    except Exception as e:
        logging.error('Failed calling Fronius API get_pv_systems_and_components')
        raise e
    
    ################### This is just for testing purposes ####################################
    df_pvs = df_pvs[(df_pvs['pvSystemIds'] == '25c1b557-5621-4cb8-9d2b-84907c324aab') &
                    (df_pvs['deviceIds'] == 'f62a7b80-2686-4d49-b0fa-afc100bd4c95')]

    ##########################################################################################

    for s, d in zip(df_pvs['pvSystemIds'], df_pvs['deviceIds']):
        # Get PV System details
        try:
            df_pvs_details = fronius_ext.get_pv_system_details_as_df(pv_system_id=s)
            site = df_pvs_details.loc[0, 'name']
            timezone = df_pvs_details.loc[0, 'timeZone']
            city = df_pvs_details.loc[0, 'address_city']
            installation_date = df_pvs_details.loc[0, 'installationDate']
            logging.info(f'Successfully extracted pv system details for pv_system={s}')
        except Exception as e:
            logging.error(f'Failed calling Fronius API get_pv_system_details method for pv_system={s}')
            logging.error(str(e))

        # Get Device details
        try:
            df_dev_details = fronius_ext.get_device_details_as_df(pv_system_id=s, device_id=d)
            logging.info(f'Successfully extracted pv device details for pv_system={s} and device_id={d}')
        except Exception as e:
            logging.error(f'Failed calling Fronius API get_device_details method for pv_system={s} and device_id={d}')


        start_time = aws_s3.get_last_data_date(folder=f'Fronius/{site}/PlantData')
        if start_time == None:
            start_time = installation_date
        days = 0
        inv_data_list = []
        while start_time <= datetime.now():
            if days <= 7:
                end_time = start_time + timedelta(days=1)
                try:
                    df_inv_data = fronius_ext.get_device_data_as_df(pv_system_id = s,
                                                                    device_id = d,
                                                                    start_time = start_time,
                                                                    end_time=end_time)
                    
                    if len(df_inv_data) > 0:
                        df_inv_data['deviceId'] = d
                        df_inv_data['pvSystemId'] = s
                        inv_data_list.append(df_inv_data)
                        logging.info(f"Extracted Fronius API get_inverter_data method for system={s}, device={d}, start_time={start_time}, end_time={end_time}")
                        days += 1
                except Exception as e:
                    logging.error(f"Failed calling Fronius API get_inverter_data method for system={s}, device={d}, start_time={start_time}, end_time={end_time}")
                    logging.error(str(e))
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
                        
                        aws_s3.store_csv_s3(df = df_inv_data,
                                            folder=f'Fronius/{site}/PlantData',
                                            file_name=f'inverter_details_{start_time}_{d}.csv')
                        
                        # Extract meteo data
                        if site in coordinates.keys():
                            df_meteo = meteo_extractor.get_wheather_data(start_date=datetime.strptime(min(df_inv['datetime']), '%Y-%m-%d %H:%M:%S'),
                                                                        end_date=datetime.strptime(max(df_inv['datetime']), '%Y-%m-%d %H:%M:%S'),
                                                                        timezone=timezone,
                                                                        lon=coordinates[site]["lon"],
                                                                        lat=coordinates[site]["lat"])
                        else:

                            df_meteo = meteo_extractor.get_wheather_data(start_date=datetime.strptime(min(df_inv['datetime']), '%Y-%m-%d %H:%M:%S'),
                                                                        end_date=datetime.strptime(max(df_inv['datetime']), '%Y-%m-%d %H:%M:%S'),
                                                                        timezone=timezone,
                                                                        place=city)

                        if len(df_meteo) > 0:
                            aws_s3.store_csv_s3(df = df_meteo,
                                                folder=f'Fronius/{site}/WeatherData',
                                                file_name=f'weather_data_{start_time}.csv')

                except Exception as e:
                    logging.error(f"Couldn't store inverter data into S3 for for system={s}, device={d}, start_time={start_time}, end_time={end_time}")
                    raise(e)
            start_time = end_time


def store_huaweii_inverter_data_to_S3(sites:dict,
                                      meteo_credentials:dict,
                                      aws_secret_key:str,
                                      aws_access_key_id: str):
    extractor = HuaweiiExtractor(sites)
    meteo_extractor = MeteoExtractor(meteo_credentials)
    aws_s3 = AWS3Extractor(aws_secret_key=aws_secret_key,
                           aws_access_key_id=aws_access_key_id)
    try:
        extractor.log_in()
    except Exception as e:
        logging.error(f'Failed to login in Huaweii API: {str(e)}')
    
    try:
        plants = extractor.get_plant_list()
        df_plants = pd.DataFrame(plants)
        plantCodes = df_plants.plantCode.unique()
    except Exception as e:
        logging.error(f'Failed calling Huaweii API get_plant_list: {str(e)}')
    
    try:
        devices = extractor.get_device_list(plantCodes)
        df_devices = pd.DataFrame(devices)
        df_devices = df_devices[df_devices.devTypeId == 1]
        df_devices.rename(columns={'stationCode':'plantCode'}, inplace=True)
    except Exception as e:
        logging.error(f'Failed calling Huaweii API get_device_list for plants {plantCodes}: {str(e)}')
    
    df = pd.merge(df_devices, df_plants, on='plantCode', how='inner')
    df.rename(columns={'id': 'devId'}, inplace=True)
    
    for pl in range(len(df_plants)):
        plant = df_plants.loc[pl, 'plantCode']
        site = df_plants.loc[pl, 'plantName']
        lon = df_plants.loc[pl, 'longitude']
        lat = df_plants.loc[pl, 'latitude']
        devices = list(df[df['plantCode'] == plant]['devId'])
        devices = [str(d) for d in devices]

        start_date = aws_s3.get_last_data_date(folder=f'Huaweii/{site}/PlantData')
        if start_date == None:
            start_date = df_plants.loc[pl, 'gridConnectionDate']
        
        start_date = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S%z")
        end_date = start_date + timedelta(days=3)
        
        while end_date <= datetime.now(end_date.tzinfo):
            start_time = int(start_date.timestamp() * 1000)
            end_time = int(end_date.timestamp() * 1000)

            try:
                df_dev_data = extractor.get_device_data_as_df(devices, start_time, end_time)
            except Exception as e:
                logging.error(f'Failed calling Huaweii API get_device_data for devices {devices}: {str(e)}')
            
            df_ = pd.merge(df, df_dev_data, on='devId', how='inner')

            # Extract meto data
            if (lon != None) & (lon != '1.000000') & (lat != None) & (lat != '1.000000') & (lon != '0.000000') & (lat != '0.000000'):
                lat = lat + "N"
                lon = str(abs(float(lon))) + "W"
                df_meteo = meteo_extractor.get_wheather_data(start_date=start_date,
                                                             end_date=end_date,
                                                             lon=lon,
                                                             lat=lat)
            else:
                df_meteo = pd.DataFrame()

            try:
                if len(df_dev_data) >  0:

                    aws_s3.store_csv_s3(df = df_,
                                        folder=f'Huaweii/{site}/PlantData',
                                        file_name=f'inverter_details_{start_date.strftime("%Y-%m-%d %H:%M-%S")}_.csv')
                    
                    logging.info(f"Data stored into S3 for site={site}, start_time={start_time}, end_time={end_time}")
                else:
                    logging.warning(f"No data retrieved for site={site}, start_time={start_time}, end_time={end_time}")
                
                if len(df_meteo) > 0:
                    aws_s3.store_csv_s3(df = df_meteo,
                                        folder=f'Huaweii/{site}/WeatherData',
                                        file_name=f'weather_data_{start_date.strftime("%Y-%m-%d %H:%M-%S")}_.csv')

            except Exception as e:
                logging.error(f"Couldn't store inverter data into S3 for site={site}, start_time={start_time}, end_time={end_time}")
                raise(e)
            start_date = end_date
            end_date = start_date + timedelta(days=3)


def main(args: ArgumentParser) -> None:
    with open(args.config_file) as f:
        config = json.load(f)
    
    with open(args.coord_file) as f:
        coord = json.load(f)

    if args.api == 'solaredge':
        store_solaredge_inverter_data_to_S3(sites=config["SOLAREDGE"],
                                            meteo_credentials=config["METEOSOURCE"],
                                            coordinates=coord["SOLAREDGE"],
                                            aws_secret_key=config["AWS_SECRET_ACCESS_KEY"],
                                            aws_access_key_id=config["AWS_ACCESS_KEY_ID"])
    
    elif args.api == 'fronius':
        store_fronius_inverter_data_to_S3(sites=config["FRONIUS"],
                                          meteo_credentials=config["METEOSOURCE"],
                                          coordinates=coord["FRONIUS"],
                                          aws_secret_key=config["AWS_SECRET_ACCESS_KEY"],
                                          aws_access_key_id=config["AWS_ACCESS_KEY_ID"])
    elif args.api == 'huaweii':
        store_huaweii_inverter_data_to_S3(sites = config["HUAWEII"],
                                          meteo_credentials = config["METEOSOURCE"],
                                          aws_secret_key=config["AWS_SECRET_ACCESS_KEY"],
                                          aws_access_key_id=config["AWS_ACCESS_KEY_ID"])
    print('Done')
    



    

if __name__ == "__main__":
    logging.basicConfig(filename=os.path.join(os.getcwd(), 'logs', 'upload.log'),
                        filemode='w', 
                        format='%(name)s - %(levelname)s - %(message)s')
    parser = ArgumentParser()
    parser.add_argument('--config_file',
                        type=str,
                        required=False,
                        default=os.path.join(os.getcwd(), 'config.json'))
    
    parser.add_argument('--coord_file',
                        type=str,
                        required=False,
                        default=os.path.join(os.getcwd(), 'coordinates.json'))

    parser.add_argument('--api',
                        type=str,
                        required=False,
                        default='huaweii',
                        choices=['huaweii', 'fronius', 'solaredge'])
    
    args = parser.parse_args()
    main(args)
