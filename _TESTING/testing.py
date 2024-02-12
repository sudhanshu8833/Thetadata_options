import asyncio
import aiohttp

async def fetch_ticker_data(session, ticker):
    url = f'https://www.google.com'  # Replace this with the actual API endpoint
    async with session.get(url) as response:
        if response.status == 200:
            data = await response.text()
            process_ticker_data(data,ticker)
        else:
            print(f"Failed to fetch data for ticker {ticker}")

def process_ticker_data(data,instrument):
    # Process the retrieved data here
    print(instrument)

async def fetch_all_tickers(tickers):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_ticker_data(session, ticker) for ticker in tickers]
        await asyncio.gather(*tasks)

tickers = ['AAPL', 'GOOG', 'MSFT']  # Add your 12000 tickers here

asyncio.run(fetch_all_tickers(tickers))
