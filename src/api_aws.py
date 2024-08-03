import pandas as pd
import boto3
from io import StringIO
from datetime import datetime

class AWS3Extractor (object):
    def __init__(self, 
                 aws_access_key_id:str, 
                 aws_secret_key:str) -> None:
        
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_key = aws_secret_key
    
    def read_csv_from_s3(self, 
                         object_key:str,
                         bucket_name:str = 'prod-satia-raw-data') -> pd.DataFrame:
            
            s3_client = boto3.client('s3',
                                     aws_access_key_id=self.aws_access_key_id,
                                     aws_secret_access_key= self.aws_secret_key)
    
            csv_obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            body = csv_obj['Body']
            csv_string = body.read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_string))
            return df
    

    def get_last_data_date(self,
                           folder:str,
                           bucket:str = 'prod-satia-raw-data'):
    
        s3 = boto3.resource('s3',
                            aws_access_key_id=self.aws_access_key_id,
                            aws_secret_access_key= self.aws_secret_key)
        
        last_date = None
        s3_bucket = s3.Bucket(bucket)
        files = [f.key for f in s3_bucket.objects.filter(Prefix=folder).all()]
        if len(files) > 0:
            dates = [f.split('/')[-1].split('_')[2] for f in files]
            keys = [datetime.strptime(x, '%Y-%m-%d %H:%M:%S') for x in dates]
            files_dict = dict([(k, v) for k, v in zip(keys, files)])
            last_file = files_dict[max(files_dict.keys())]
            df_last = self.read_csv_from_s3(object_key = last_file)
            df_last['datetime'] = df_last['datetime'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))
            last_date = max(df_last['datetime'])
        return last_date
    
    def store_csv_s3(self, 
                     df:pd.DataFrame, 
                     folder:str, 
                     file_name:str,
                     bucket_name:str = 'prod-satia-raw-data'):
         
         df.to_csv(f"s3://{bucket_name}/{folder}/{file_name}",
                    index=False,
                    storage_options={"key" : self.aws_access_key_id,
                                     "secret": self.aws_secret_key})
