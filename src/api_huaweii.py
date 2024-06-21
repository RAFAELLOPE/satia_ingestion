import requests
import json
import os

class HuaweiiExtractor(object):
    def __init__(self, config:dict, intl:str = 'eu5'):
        self.user = config["USER"]
        self.password = config["PASSWORD"]
        self.login = f'https://{intl}.fusionsolar.huawei.com/thirdData/login'
        self.plant_list = f'https://{intl}.fusionsolar.huawei.com/thirdData/stations'
        self.header = {"Content-Type": "application/json"}
        self.token = None
    
    def transform_plant_list(self):
        pass

    def get_plant_list(self) -> dict:
        data = {"userName":self.user,
                "systemCode": self.password}
        try:
            res = requests.post(self.login, 
                                headers=self.header, 
                                json=data)
            res_data = json.loads(res.content)
            if res_data['success'] == True:
                self.token = res.headers["xsrf-token"]
                header = {"XSRF-TOKEN": self.token}
                body = {'pageNo': 1}
                res = requests.post(self.plant_list,
                                    headers=header,
                                    json=body)
                res_data = json.loads(res.content)

        except requests.exceptions.RequestException as e:
            raise e
        return

with open(os.path.abspath(os.path.join(os.getcwd(), 'config.json'))) as f:
    config = json.load(f)
    extractor = HuaweiiExtractor(config["HUAWEII"])
    _ = extractor.get_plant_list()


