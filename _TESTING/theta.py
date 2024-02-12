import aiohttp
import asyncio
import json
import os
import pandas as pd
import numpy as np
import time
from datetime import datetime

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
        async with session.get(f"http://127.0.0.1:25510/v2/list/expirations?root={instrument}") as response:
            data = await response.json()
            if 'url' in data:
                return data['url']
            elif 'response' in data:
                return data['response']['url']  # Adjust this according to the actual response structure
            else:
                raise ValueError("Unable to extract URL from response")

    async def get_bulk_quotes(self, session, instrument, expiry):
        async with session.get(f"http://127.0.0.1:25510/bulk_snapshot/option/quote?root={instrument}&exp={expiry}") as response:
            return await response.json()

    async def get_ticker_price(self, session, instrument):
        async with session.get(f"http://127.0.0.1:25510/v2/snapshot/stock/trade?root={instrument}") as response:
            data = await response.json()
            return data['response'][0][-2] if data['response'] else "NA"

    def convert_to_datetime(self, date_value):
        date_str = str(date_value)
        date_obj = datetime.strptime(date_str, "%Y%m%d")
        return date_obj

    async def manage_quotes(self, session, instrument, expiry):
        if self.convert_to_datetime(expiry) <= datetime.now():
            return

        async with session.get(self.get_bulk_quotes(session, instrument, expiry)) as response:
            quotes = await response.json()

            if quotes[0] == 0:
                return

            json_data = {}
            for d in quotes:
                if d['contract']['strike'] not in json_data:
                    json_data[d['contract']['strike']] = {}
                json_data[d['contract']['strike']][d['contract']['right']] = d['tick'][-3]

            self.json_data[instrument][expiry] = json_data

    async def base_called(self, session, instrument):
        expirations_response = await self.get_expirations(session, instrument)
        expirations_url = expirations_response['url']  # Extract the URL from the response
        async with session.get(expirations_url) as response:
            expirations = await response.json()
            self.json_data[instrument] = {}
            for expiry in expirations:
                await self.manage_quotes(session, instrument, expiry)

    async def update_stock_price(self, session, instrument):
        try:
            stock_price = await self.get_ticker_price(session, instrument)
            self.stock_price[instrument] = stock_price
        except Exception as e:
            print(f"Failed to update stock price for {instrument}: {e}")
            self.stock_price[instrument] = "NA"

    def closest_strike(self, dictionary, value):
        filtered_keys = dictionary.keys()
        return min(filtered_keys, key=lambda key: abs(key - value))

    def generate_list(self, instrument, value):
        temp_list = []
        temp_list.append(self.stock_price[instrument])

        for expiry, strikes in value.items():
            closest_key = self.closest_strike(strikes, self.stock_price[instrument])
            call = strikes[closest_key]['C']
            put = strikes[closest_key]['P']
            temp_list.append(call)
            temp_list.append(put)
            percentage_difference = ((call - put) / self.stock_price[instrument]) * 100
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
        sorted_df = sorted_df.append(pd.DataFrame(new_row_data), ignore_index=True)

        file_path = os.path.join(os.path.dirname(BASE_DIR), 'output.csv')
        sorted_df.to_csv(file_path)
        self.df = sorted_df

    async def main(self):
        async with aiohttp.ClientSession() as session:
            tickers = await session.get(self.get_roots_url)
            tickers = await tickers.json()
            print(type(tickers))
            tickers=tickers['response']
            print(tickers)
            for ticker in tickers[:500]:
                if ticker in blocked_ticker:
                    continue

                await self.update_stock_price(session, ticker)
                await self.base_called(session, ticker)

            self.final_list = {}
            for key, value in self.json_data.items():
                if self.stock_price[key] == "NA" or not bool(value):
                    blocked_ticker.append(key)
                    continue

                temp_list = self.generate_list(key, value)
                self.final_list[key] = temp_list

            self.convert_to_df()

            with open(json_path, 'w') as json_file:
                json.dump({"blocked": blocked_ticker}, json_file, indent=4)

    async def run(self):
        while True:
            await self.main()

if __name__ == "__main__":
    the = ThetaData()
    time_now = time.time()
    asyncio.run(the.main())
    print(time.time() - time_now)
