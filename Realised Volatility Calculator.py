import pandas as pd
import numpy as np
import time
import matplotlib.pyplot as plt

# USER INPUTS
data_file = r"jpm1min_2006_2009.csv"
identifyer = r"jpm1min.csv"
one_day_close_prices = r"JPM.csv"

script_start_time = time.time()

start_time = time.time()

# Read the CSV file into a DataFrame
df = pd.read_csv(data_file)

# Convert the timestamp column to datetime format
df['datetime'] = pd.to_datetime(df['datetime'])

# Check for and remove duplicate timestamps
df = df[~df['datetime'].duplicated()]

# Set the timestamp column as the index
df.set_index('datetime', inplace=True)

# Group by day and interpolate within each group
df_interpolated = df.groupby(df.index.date).apply(lambda x: x.resample('1T')
                                                  .interpolate(method=
                                                  'linear'))

df_interpolated = df_interpolated.reset_index()
df_interpolated = df_interpolated.drop(df_interpolated.columns[0],axis=1)
df_interpolated = df_interpolated.set_index(df_interpolated.columns[0])

df_interpolated = df_interpolated.astype({col: float for col in 
                                          df_interpolated.columns})

# Save the interpolated DataFrame to a new CSV file
interpolatedfilename = f"interpolated_{identifyer}"
df_interpolated.to_csv(interpolatedfilename, float_format='%.30f')

end_time = time.time() 
execution_time = end_time - start_time

print(f"Execution time: {execution_time:.1f} seconds")

data_file = str(f"interpolated_{identifyer}")

start_time = time.time()

prices = pd.read_csv(str(data_file),index_col = 'datetime')['close']

Lclose = (np.log(prices)).rename('Lclose')

Dclose = (prices - prices.shift(1)).rename('Dclose')

DLclose = (Lclose - Lclose.shift(1)).rename('DLclose')

returns = pd.concat([prices,Lclose,Dclose,DLclose],axis = 1)

returns = returns.astype({col: float for col in returns.columns})

returns.to_csv(f"returns_{identifyer}", index_label = 'datetime', 
               float_format='%.30f')

end_time = time.time() 
execution_time = end_time - start_time

print(f"Execution time: {execution_time:.1f} seconds")

data_file = str(f"returns_{identifyer}")
start_time = time.time()
pricerets = pd.read_csv(data_file)

# Convert the timestamp column to datetime format
pricerets['datetime'] = pd.to_datetime(pricerets['datetime'])

# Set the timestamp column as the index
pricerets.set_index('datetime', inplace=True)

realizedvols = pricerets['DLclose']**2

realizedvols.to_csv(r"RVol1Min.csv",index_label = 'datetime', float_format='%.30f')

RVol1Min = realizedvols

end_time = time.time()
execution_time = end_time - start_time

print(f"Execution time: {execution_time:.1f} seconds")

data_file = str(f"returns_{identifyer}")
frequency = '5min'
start_time = time.time()
pricerets = pd.read_csv(data_file)

# Convert the timestamp column to datetime format
pricerets['datetime'] = pd.to_datetime(pricerets['datetime'])

# Set the timestamp column as the index
pricerets.set_index('datetime', inplace=True)

pricerets = pricerets['DLclose']**2
# Group by day and interpolate within each group
realizedvols = pricerets.groupby([pricerets.index.date, 
                                      pd.Grouper(freq=str(frequency))]).apply(
                                          lambda x: x.sum())

realizedvols = realizedvols.reset_index()
realizedvols = realizedvols.drop(realizedvols.columns[0], axis=1)
realizedvols = realizedvols.set_index(realizedvols.columns[0])
realizedvols = realizedvols.astype({col: float for col in 
                                      realizedvols.columns})

realizedvols.to_csv(r"RVol5min.csv", 
                     index_label = 'datetime', float_format='%.30f')

RVol5Min = realizedvols

end_time = time.time()
execution_time = end_time - start_time

print(f"Execution time: {execution_time:.1f} seconds")

data_file = str(f"returns_{identifyer}")
frequency = '10min'
start_time = time.time()
pricerets = pd.read_csv(data_file)

# Convert the timestamp column to datetime format
pricerets['datetime'] = pd.to_datetime(pricerets['datetime'])

# Set the timestamp column as the index
pricerets.set_index('datetime', inplace=True)

pricerets = pricerets['DLclose']**2
# Group by day and interpolate within each group
realizedvols = pricerets.groupby([pricerets.index.date, 
                                      pd.Grouper(freq=str(frequency))]).apply(
                                          lambda x: x.sum())

realizedvols = realizedvols.reset_index()
realizedvols = realizedvols.drop(realizedvols.columns[0], axis=1)
realizedvols = realizedvols.set_index(realizedvols.columns[0])
realizedvols = realizedvols.astype({col: float for col in 
                                      realizedvols.columns})

realizedvols.to_csv(r"RVol10min.csv", 
                     index_label = 'datetime', float_format='%.30f')

RVol10Min = realizedvols

end_time = time.time()
execution_time = end_time - start_time

print(f"Execution time: {execution_time:.1f} seconds")

data_file = str(f"returns_{identifyer}")
frequency = '15min'
start_time = time.time()
pricerets = pd.read_csv(data_file)

# Convert the timestamp column to datetime format
pricerets['datetime'] = pd.to_datetime(pricerets['datetime'])

# Set the timestamp column as the index
pricerets.set_index('datetime', inplace=True)

pricerets = pricerets['DLclose']**2
# Group by day and interpolate within each group
realizedvols = pricerets.groupby([pricerets.index.date, 
                                      pd.Grouper(freq=str(frequency))]).apply(
                                          lambda x: x.sum())

realizedvols = realizedvols.reset_index()
realizedvols = realizedvols.drop(realizedvols.columns[0], axis=1)
realizedvols = realizedvols.set_index(realizedvols.columns[0])
realizedvols = realizedvols.astype({col: float for col in 
                                      realizedvols.columns})

realizedvols.to_csv(r"RVol15min.csv", 
                     index_label = 'datetime', float_format='%.30f')

RVol15Min = realizedvols

end_time = time.time()
execution_time = end_time - start_time

print(f"Execution time: {execution_time:.1f} seconds")

data_file = str(f"returns_{identifyer}")
frequency = 'B'
start_time = time.time()
pricerets = pd.read_csv(data_file)

# Convert the timestamp column to datetime format
pricerets['datetime'] = pd.to_datetime(pricerets['datetime'])

# Set the timestamp column as the index
pricerets.set_index('datetime', inplace=True)

pricerets = pricerets['DLclose']**2
# Group by day and interpolate within each group
realizedvols = pricerets.groupby([pricerets.index.date, 
                                      pd.Grouper(freq=str(frequency))]).apply(
                                          lambda x: x.sum())

realizedvols = realizedvols.reset_index()
realizedvols = realizedvols.drop(realizedvols.columns[0], axis=1)
realizedvols = realizedvols.set_index(realizedvols.columns[0])
realizedvols = realizedvols.astype({col: float for col in 
                                      realizedvols.columns})

realizedvols.to_csv(r"RVolDaily.csv", 
                     index_label = 'datetime', float_format='%.30f')

RVolDaily = realizedvols

end_time = time.time()
execution_time = end_time - start_time

print(f"Execution time: {execution_time:.1f} seconds")

# start_time = time.time()
# returns.index = pd.to_datetime(returns.index, format='%Y-%m-%d %H:%M:%S')  # Adjust the format to match your data
# pricerets = returns['DLclose']
# dailies = pricerets.groupby(pricerets.index.date).apply(lambda x: x.sum())
# dailies = dailies.reset_index()
# dailies = dailies.set_index(dailies.columns[0])
# dailies = dailies.astype({col: float for col in 
#                                       dailies.columns})
# dailies.to_csv(r"dataForGARCH.csv", 
#                      index_label = 'datetime', float_format='%.30f')
# end_time = time.time()
# execution_time = end_time - start_time

# print(f"Execution time: {execution_time:.1f} seconds")

#                 Note:
# The remaining code will return an error if any of the below files are deleted or overwritten:
#         dataForGARCH.csv
#         JPM.csv
#         jpm1min_2006_2009.csv

htgarch = pd.read_csv(r"dataForGARCH.csv", index_col = 'datetime')
htgarch['CondV'] = pd.to_numeric(htgarch['CondV'], errors='coerce')  # Convert values to numeric, coerce errors
htgarch.index = pd.to_datetime(htgarch.index)  # Ensure index is datetime

returns['close'] = pd.to_numeric(returns['close'], errors='coerce')  # Convert values to numeric, coerce errors
returns.index = pd.to_datetime(returns.index)  # Ensure index is datetime

oneday = pd.read_csv(str(one_day_close_prices), index_col="datetime")
oneday.index = pd.to_datetime(oneday.index, format='%d/%m/%Y %H:%M')
oneday['close'] = pd.to_numeric(oneday['close'], errors='coerce')

difference = (htgarch['CondV'] - RVolDaily['DLclose'])

plt.figure(dpi=600)
fig, axs = plt.subplots(9, 1, figsize=(15, 20), sharex=True)  # 6 rows, 1 column

axs[0].plot(returns['close'])
axs[0].set_title('JPM 1Min Close Price')

axs[1].plot(RVol1Min)
axs[1].set_title('1 Minute Realised Volatility')

axs[2].plot(RVol5Min)
axs[2].set_title('5 Minute Realised Volatility')

axs[3].plot(RVol10Min)
axs[3].set_title('10 Minute Realised Volatility')

axs[4].plot(RVol15Min)
axs[4].set_title('15 Minute Realised Volatility')

axs[5].plot(RVolDaily)
axs[5].set_title('Daily Realised Volatility')

axs[6].plot(htgarch['CondV'])
axs[6].set_title('ARMA(0,0)-GARCH(1,1) Conditional Variance')

axs[7].plot(difference)
axs[7].set_title('Conditional Variance - Realised Volatility')

axs[8].plot(oneday['close'])
axs[8].set_title('JPM Daily Close Price')

plt.savefig('Q5 Plots.png')
plt.show()

script_end_time = time.time()
execution_time = script_end_time - script_start_time

print(f"Total Time Elapsed: {execution_time:.1f} seconds")

