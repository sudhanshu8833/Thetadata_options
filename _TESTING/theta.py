import pandas as pd

# Sample dictionary
data_dict = {
    "AAPL": [1, 2, 3, 4, 54],
    "MSFT": [23, 2312],
    "JINDAL": [12, 3, 3, 1, 3, 1, 2, 5]
}

# Convert the dictionary to a DataFrame
df = pd.DataFrame.from_dict(data_dict, orient='index')
df.index.name = 'ticker'
df.reset_index(inplace=True)
new_columns = {}
for i in range(len(df.columns)):
    if i % 3 == 0:
        new_columns[i] = f'call_{i//3 + 1}'
    elif i % 3 == 1:
        new_columns[i] = f'put_{i//3 + 1}'
    else:
        new_columns[i] = f'difference_{i//3+1}'

df.rename(columns=new_columns, inplace=True)
print(df)
# Define function to extract call, put, and difference values
def get_values(row):
    calls = row.values.tolist()
    puts = row.values.tolist()
    differences = [put - call for call, put in zip(calls, puts)]
    return calls + puts + differences

# Apply the function to each row and expand the results into separate columns
df[['call_1', 'put_1', 'difference_1', 'call_2', 'put_2', 'difference_2', 'call_3', 'put_3', 'difference_3']] = df.apply(get_values, axis=1, result_type='expand')

# Drop the original columns
df.drop(columns=[0, 1, 2, 3, 4, 5, 6, 7], inplace=True)

# Fill NaN values with 'NA'
df.fillna('NA', inplace=True)

print(df)
