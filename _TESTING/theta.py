import pandas as pd
import numpy as np

# Sample DataFrame
df = pd.DataFrame({
    'ticker': ['AAPL', 'MSFT', 'JINDAL'],
    'call_1': [1, 23, 12],
    'put_1': [2, 2312, 3],
    'difference_1': [3.0, np.nan, 3.0],
    'call_2': [4.0, np.nan, 1.0],
    'put_2': [54.0, np.nan, 3.0],
    'difference_2': [np.nan, np.nan, 1.0],
    'call_3': [np.nan, np.nan, 2.0],
    'put_3': [np.nan, np.nan, 5.0],
    'ticker_price': [1, 23, 12]
})

# New ticker to add
new_tickers = ["MSFT", "HEL"]

# Get list of existing columns
columns = df.columns

# Create a dictionary with NaN values for all columns except 'ticker'
new_row_data = {col: [np.nan] * len(new_tickers) if col != 'ticker' else new_tickers for col in columns}

# Append new row to DataFrame
df = df._append(pd.DataFrame(new_row_data), ignore_index=True)

print(df)
