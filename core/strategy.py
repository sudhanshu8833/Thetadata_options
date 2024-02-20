import aiohttp
import asyncio
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
import requests
import time

BASE_DIR = os.path.dirname(os.path.realpath(__file__))
json_path = os.path.join(BASE_DIR, 'tickers.json')
blocked_ticker = []
with open(json_path, 'r') as json_file:
    blocked_ticker = json.load(json_file)['blocked']

class ThetaData:
    def __init__(self):
        self.get_roots_url = "http://127.0.0.1:25510/v2/list/roots/option"
        self.json_data = {}
        self.currency = "ask"
        self.stock_price = {}

    async def get_expirations(self, session, instrument):
        url = f"http://127.0.0.1:25510/v2/list/expirations?root={instrument}"
        async with session.get(url) as response:
            response=await response.json()
            return response

    async def get_bulk_quotes(self, session, instrument, expiry):
        url = f"http://127.0.0.1:25510/bulk_snapshot/option/quote?root={instrument}&exp={expiry}"
        async with session.get(url) as response:
            response=await response.json()
            return response['response']

    async def get_ticker_price(self, session, instrument):
        url = f"http://127.0.0.1:25510/v2/snapshot/stock/trade?root={instrument}"
        async with session.get(url) as response:
            response=await response.json()
            return response['response']

    def convert_to_datetime(self, date_value):
        return datetime.strptime(str(date_value), "%Y%m%d")

    async def manage_quotes(self, session, instrument, expiry):
        if self.convert_to_datetime(expiry) <= datetime.now():
            return

        quotes = await self.get_bulk_quotes(session, instrument, expiry)

        if quotes[0] == 0:
            return

        json_data = {}
        for d in quotes:
            contract = d['contract']
            if contract['strike'] not in json_data:
                json_data[contract['strike']] = {}
            json_data[contract['strike']][contract['right']] = d['tick'][-3]

        self.json_data[instrument][expiry] = json_data

    async def base_called(self, session, instrument):
        expirations = await self.get_expirations(session, instrument)
        self.json_data[instrument] = {}
        for expiry in expirations['response']:
            await self.manage_quotes(session, instrument, expiry)

    async def update_stock_price(self, session, instrument):
        try:
            response = await self.get_ticker_price(session, instrument)
            self.stock_price[instrument] = response[0][-2]
        except:
            self.stock_price[instrument] = "NA"

    # Other methods remain the same...

    async def gather_calls(self, session, ticker):
        try:
            print(ticker)
            if ticker in blocked_ticker:
                return

            await self.update_stock_price(session, ticker)
            await self.base_called(session, ticker)
        except:
            print(f"Exception came for {ticker}")
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

    async def main(self):
        async with aiohttp.ClientSession() as session:
            tickers = requests.get(self.get_roots_url).json()['response']
            await asyncio.gather(*[self.gather_calls(session, arg) for arg in tickers])

        self.final_list={}
        for key,value in self.json_data.items():
            try:
                if(self.stock_price[key]=="NA" or not bool(value)):
                    blocked_ticker.append(key)
                    continue

                temp_list=self.generate_list(key,value)
                self.final_list[key]=temp_list
            except:
                print("ERROR OCCURED in analysis")

        self.convert_to_df()
        with open(json_path,'w') as json_file:
            json.dump({"blocked":blocked_ticker},json_file,indent=4)


if __name__ == "__main__":
    the = ThetaData()
    time1=time.time()
    asyncio.run(the.main())
    print(time.time()-time1)

