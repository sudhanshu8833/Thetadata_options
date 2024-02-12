import requests
from datetime import datetime
import pandas as pd
from pprint import pprint
import os
import json
import numpy as np
import time
import asyncio

BASE_DIR=os.path.dirname(os.path.realpath(__file__))
json_path=os.path.join(BASE_DIR,'tickers.json')
blocked_ticker=[]
with open(json_path,'r') as json_file:
    blocked_ticker=json.load(json_file)['blocked']

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
        if(self.convert_to_datetime(expiry)<=datetime.now()):
            return

        quotes=requests.get(self.get_bulk_quotes(instrument,expiry)).json()['response']

        if(quotes[0]==0):
            return
        json_data={}
        for d in quotes:
            if d['contract']['strike'] not in json_data:
                json_data[d['contract']['strike']]={}
            json_data[d['contract']['strike']][d['contract']['right']]=d['tick'][-3]


        self.json_data[instrument][expiry]=json_data

    def base_called(self,instrument):
        expirations=requests.get(self.get_expirations(instrument)).json()['response']
        self.json_data[instrument]={}
        for expiry in expirations:
            self.manage_quotes(instrument,expiry)

    async def update_stock_price(self,instrument):
        try:
            response=requests.get(self.get_ticker_price(instrument)).json()['response']
            self.stock_price[instrument]=response[0][-2]
        except:
            self.stock_price[instrument]="NA"


    def closest_strike(self,dictionary, value):
        filtered_keys = dictionary.keys()  # Assuming you don't need to filter the keys
        return min(filtered_keys, key=lambda key: abs(key - value))

    def generate_list(self,instrument,value):
        temp_list=[]
        temp_list.append(self.stock_price[instrument])

        for expiry,strikes in value.items():
            closest_key=self.closest_strike(strikes,self.stock_price[instrument])
            call=strikes[closest_key]['C']
            put=strikes[closest_key]['P']
            temp_list.append(call)
            temp_list.append(put)
            percentage_difference=((call-put)/self.stock_price[instrument])*100
            temp_list.append(percentage_difference)


        return temp_list

    def convert_to_df(self):
        df = pd.DataFrame.from_dict(self.final_list, orient='index')
        df.index.name = 'ticker'
        df = df.applymap(lambda x: x[1:] if isinstance(x, list) else x)
        df.reset_index(inplace=True)

        new_columns = {}
        for i in range(len(df.columns)):
            if i == 0:
                new_columns[i] = 'ticker_price'
            elif (i - 1) % 3 == 0:
                new_columns[i] = f'call_{(i - 1) // 3 + 1}'
            elif (i - 1) % 3 == 1:
                new_columns[i] = f'put_{(i - 1) // 3 + 1}'
            else:
                new_columns[i] = f'difference_{(i - 1) // 3 + 1}'


        df.rename(columns=new_columns, inplace=True)
        sorted_df = df.sort_values(by='difference_1', ascending=False)

        columns = sorted_df.columns
        new_row_data = {col: [np.nan] * len(blocked_ticker) if col != 'ticker' else blocked_ticker for col in columns}
        sorted_df = sorted_df._append(pd.DataFrame(new_row_data), ignore_index=True)
        file_path=os.path.join(os.path.dirname(BASE_DIR),'output.csv')
        sorted_df.to_csv(file_path)
        self.df=sorted_df

    async def main(self):
        tickers=requests.get(self.get_roots_url).json()['response']

        for ticker in tickers[:500]:
            if ticker in blocked_ticker:
                continue

            await self.update_stock_price(ticker)
            self.base_called(ticker)

        self.final_list={}
        for key,value in self.json_data.items():

            if(self.stock_price[key]=="NA" or not bool(value)):
                blocked_ticker.append(key)
                continue

            temp_list=self.generate_list(key,value)
            self.final_list[key]=temp_list


        self.convert_to_df()
        with open(json_path,'w') as json_file:
            json.dump({"blocked":blocked_ticker},json_file,indent=4)

    def run(self):
        while True:
            self.main()


if __name__=="__main__":
    the=thetaData()
    time_now=time.time()
    the.main()
    print(time.time()-time_now)
