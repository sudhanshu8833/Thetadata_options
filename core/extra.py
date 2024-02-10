import requests
from datetime import datetime
import pandas as pd
from pprint import pprint
import json


class thetaData:
    def __init__(self):
        self.get_roots_url="http://127.0.0.1:25510/v2/list/roots/option"
        self.json_data={}
        self.currency="ask"
        self.stock_price={}


    def get_expirations(self, instrument):
        return f"http://127.0.0.1:25510/v2/list/expirations?root={instrument}"
    
    def get_bulk_quotes(self, instrument, expiry):
        return f"http://127.0.0.1:25510/bulk_snapshot/option/quote?root={instrument}&exp={expiry}"

    def get_ticker_price(self,instrument):
        return f"http://127.0.0.1:25510/v2/snapshot/stock/trade?root={instrument}"

    def convert_to_datetime(self, date_value):
        date_str = str(date_value)
        date_obj = datetime.strptime(date_str, "%Y%m%d")
        return date_obj


    def manage_quotes(self,instrument,expiry):
        print(instrument,expiry)
        if(self.convert_to_datetime(expiry)<=datetime.now()):
            return
        quotes=requests.get(self.get_bulk_quotes(instrument,expiry)).json()['response']

        if(quotes[0]==0):
            print(quotes)
            return
        json_data={}
        for d in quotes:
            json_data[d['contract']['strike']]={
                "type":d['contract']['right'],
                "price":d['tick'][-3]
            }
        print(json_data)
        self.json_data[instrument][expiry]=json_data

    def base_called(self,instrument):
        expirations=requests.get(self.get_expirations(instrument)).json()['response']
        self.json_data[instrument]={}
        for expiry in expirations:
            self.manage_quotes(instrument,expiry)

    def update_stock_price(self,instrument):
        try:
            response=requests.get(self.get_ticker_price(instrument)).json()['response']
            self.stock_price[instrument]=response[0][-2]
        except:
            self.stock_price[instrument]="NA"

    def get_strike(dictionary, value, option_type):
        filtered_keys = [key for key, val in dictionary.items() if val['type'] == option_type]
        return min(filtered_keys, key=lambda key: abs(key - value))

    def main(self):
        tickers=requests.get(self.get_roots_url).json()['response']
        for ticker in tickers[:20]:
            print(ticker)
            self.update_stock_price(ticker)
            self.base_called(ticker)

        final_list=[]
        for key,value in self.json_data.items():
            if(self.stock_price[key]=="NA" or not bool(value)):
                tickers.remove(key)
                
        
        with open("tickers.json",'w') as json_file:
            json.dump({"symbols":tickers},indent=4)



if __name__=="__main__":
    the=thetaData()
    the.main()
    pprint(the.json_data)