import requests
import aiohttp
import asyncio
from datetime import datetime,timedelta
import pandas as pd
from pprint import pprint
import os
import json
import numpy as np
import time
import traceback

BASE_DIR=os.path.dirname(os.path.realpath(__file__))
json_path=os.path.join(BASE_DIR,'tickers.json')
blocked_ticker=[]
with open(json_path,'r') as json_file:
    blocked_ticker=json.load(json_file)['blocked']


data={}
data_path=os.path.join(BASE_DIR,'data.json')
with open(data_path,'r') as json_file:
    data=json.load(json_file)


class thetaData:
    def __init__(self):
        self.get_roots_url="http://127.0.0.1:25510/v2/list/roots/option"
        self.json_data={}
        self.currency="ask"
        self.stock_price={}
        self.day=False


    async def get_expirations(self, session, instrument):
        url = f"http://127.0.0.1:25510/v2/list/expirations?root={instrument}"
        async with session.get(url) as response:
            response=await response.json()
            return response['response']

    async def get_bulk_quotes(self, session, instrument, expiry):
        url = f"http://127.0.0.1:25510/bulk_snapshot/option/quote?root={instrument}&exp={expiry}"
        async with session.get(url) as response:
            response=await response.json()
            return response['response']

    async def get_ticker_price(self, session, instrument):
        if self.day:
            return data['json_data']['stock_price'][instrument]
        url = f"http://127.0.0.1:25510/v2/snapshot/stock/trade?root={instrument}"
        async with session.get(url) as response:
            response=await response.json()
            return response['response']


    def convert_to_YYYMMDD(self, date):
        return date.strftime("%Y%m%d")

    def get_url(self,instrument,expiry,strike,right):
        today_date=self.convert_to_YYYMMDD(datetime.now())
        four_days_back=self.convert_to_YYYMMDD(datetime.now()-timedelta(days=4))
        return f"http://127.0.0.1:25510/v2/hist/option/eod?root={instrument}&exp={expiry}&strike={strike}&right={right}&start_date={four_days_back}&end_date={today_date}"


    async def get_EOD(self,session,instrument,expiry,strike,right,type):
        today_date=self.convert_to_YYYMMDD(datetime.now())
        four_days_back=self.convert_to_YYYMMDD(datetime.now()-timedelta(days=4))
        url=""
        if type=="option":
            url=f"http://127.0.0.1:25510/v2/hist/{type}/eod?root={instrument}&exp={expiry}&strike={strike}&right={right}&start_date={four_days_back}&end_date={today_date}"
            async with session.get(url) as response:
                response= await response.json()
                response=response['response'][-1][5]    
                return response
        else:
            url=f"http://127.0.0.1:25510/hist/stock/eod?root={instrument}&start_date={four_days_back}&end_date={today_date}"
            async with session.get(url) as response:
                response= await response.json()
                response=response['response'][-1][3]    
                return response

    async def option_trade_price(self,session, instrument, expiry, contract, strike):
        if str(expiry) == "nan":
            return 0
        url = f"http://127.0.0.1:25510/snapshot/option/trade?root={instrument}&exp={expiry}&right={contract}&strike={strike}"

        async with session.get(url) as response:
            response=await response.json()
            response=response['response']
            if(response[0]==0):
                return 0
            else:
                return response[0][-2]


    def convert_to_datetime(self, date_value):
        date_str = str(date_value)
        date_obj = datetime.strptime(date_str, "%Y%m%d")
        return date_obj


    async def manage_quotes(self,session,instrument,expiry):
        if(self.convert_to_datetime(expiry)<datetime.now()):
            return

        closest_key=0

        if self.day:
            try:
                self.json_data[instrument][expiry]=data['json_data'][instrument][expiry]
                closest_key=self.closest_strike(self.json_data[instrument][expiry],self.stock_price[instrument]*1000)
            except Exception as e:
                print(traceback.format_exc())
                return

        else:
            quotes=await self.get_bulk_quotes(session,instrument,expiry)
            if(quotes[0]==0):
                return
            json_data={}
            for d in quotes:
                if d['contract']['strike'] not in json_data:
                    json_data[d['contract']['strike']]={}
                json_data[d['contract']['strike']][d['contract']['right']]=d['tick'][-3]
            closest_key=self.closest_strike(json_data,self.stock_price[instrument]*1000)
            self.json_data[instrument][expiry]=json_data


        call=await self.option_trade_price(session,instrument,expiry,'C',closest_key)
        if(call!=0):
            self.json_data[instrument][expiry][closest_key]['C']=call
        else:
            self.json_data[instrument][expiry][closest_key]['C']=await self.get_EOD(session,instrument,expiry,closest_key,'C',"option")

        put=await self.option_trade_price(session,instrument,expiry,'P',closest_key)
        if(put!=0):
            self.json_data[instrument][expiry][closest_key]['P']=put
        else:
            self.json_data[instrument][expiry][closest_key]['P']=await self.get_EOD(session,instrument,expiry,closest_key,'P',"option")
        

    async def base_called(self,session,instrument):
        expirations=await self.get_expirations(session,instrument)
        self.json_data[instrument]={}
        for expiry in expirations:
            await self.manage_quotes(session,instrument,str(expiry))
        # await asyncio.gather(*[self.manage_quotes(session,instrument, str(expiry)) for expiry in expirations])

    async def update_stock_price(self,session,instrument):
        if (self.day):
            try:
                self.stock_price[instrument]=data['stock_price'][instrument]
                return
            except:
                self.stock_price[instrument]="NA"
                return
        try:
            response=await self.get_ticker_price(session,instrument)
            self.stock_price[instrument]=response[0][-2]
        except:
            self.stock_price[instrument]=await self.get_EOD(session,instrument,"NA","NA",'C',"stock")


    def closest_strike(self,dictionary, value):
        filtered_keys = dictionary.keys()  # Assuming you don't need to filter the keys
        return min(filtered_keys, key=lambda key: abs(int(key) - int(value)))

    def generate_list(self,instrument,value):
        temp_list=[]
        temp_list.append(self.stock_price[instrument])

        for expiry,strikes in value.items():
            closest_key=self.closest_strike(strikes,self.stock_price[instrument]*1000)
            call=strikes[closest_key]['C']
            put=strikes[closest_key]['P']
            temp_list.append(call)
            temp_list.append(put)
            percentage_difference=((call-put)/self.stock_price[instrument])*100
            temp_list.append(percentage_difference)
            temp_list.append(str(closest_key))
            temp_list.append(str(expiry))

            temp_list.append(self.get_url(instrument,expiry,closest_key,'C'))



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
            elif (i - 1) % 6 == 0:
                new_columns[i] = f'call_{(i - 1) // 6 + 1}'
            elif (i - 1) % 6 == 1:
                new_columns[i] = f'put_{(i - 1) // 6 + 1}'
            elif (i - 1) % 6 == 2:
                new_columns[i] = f'difference_{(i - 1) // 6 + 1}'
            elif (i - 1) % 6 == 3:
                new_columns[i] = f'strike_{(i - 1) // 6 + 1}'
            elif (i - 1) % 6 == 4:
                new_columns[i] = f'expiry_{(i - 1) // 6 + 1}'
            else:
                new_columns[i] = f'url_{(i - 1) // 6 + 1}'

        print(df)
        df.rename(columns=new_columns, inplace=True)
        sorted_df = df.sort_values(by='difference_1', ascending=False)

        columns = sorted_df.columns
        new_row_data = {col: [np.nan] * len(blocked_ticker) if col != 'ticker' else blocked_ticker for col in columns}
        sorted_df = sorted_df._append(pd.DataFrame(new_row_data), ignore_index=True)
        file_path=os.path.join(os.path.dirname(BASE_DIR),'output.csv')
        sorted_df.to_csv(file_path)
        self.df=sorted_df

    async def gather_calls(self, session, ticker):
        try:
            print(ticker)
            if ticker in blocked_ticker:
                return

            await self.update_stock_price(session, ticker)
            await self.base_called(session, ticker)
        except Exception as e:
            print(traceback.format_exc())

    async def main(self):
        tickers=requests.get(self.get_roots_url).json()['response']

        async with aiohttp.ClientSession() as session:
            tickers = requests.get(self.get_roots_url).json()['response']
            # tickers=['AFTY']
            await asyncio.gather(*[self.gather_calls(session, arg) for arg in tickers])



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
#
if __name__=="__main__":
    the=thetaData()
    time_now=time.time()
    asyncio.run(the.main())
    print(time.time()-time_now)
