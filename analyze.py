import pandas as pd
import sys
from hurry.filesize import size
import matplotlib.pyplot as plt
import numpy as np
import time
from tqdm import tqdm
'''
Author: Chase Carroll
Creation Date: Oct 19, 2020
Last Updated: Oct 21, 2020

MODULES
----------------
pandas: Dataframe operations
sys: Some general QoL functions.
hurry.filesize: Provides user-friendly memory-usage display as string. https://pypi.org/project/hurry.filesize/#description
pyplot: For plotting support.
numpy: TBD. Possibly unnecessary and may be removed at later date.
time: Used for performance checking purposes.
tqdm: Used for performance checking purposes. https://pypi.org/project/tqdm/
'''

#LOAD AND TRUNCATE DATAFRAMES
#TODO consider reworking the file acquisition so that it grabs the latest csv from the internet.
#TODO if rework above is complete, also automate the CPD row selection process based on start and end date.
cpd_df = pd.read_csv("Public_CPD_Arrests.csv")
# print('Full CPD Arrests data')
# print(cpd_df)
# print('Size of CPD Arrests dataframe: {}\n------------------------'.format(size(sys.getsizeof(cpd_df))))
r_cpd_df = cpd_df[18580:23980] #crime during track of covid data
# print('Reduced CPD Arrests data')
# print(r_cpd_df)
# print('Size of reduced CPD Arrests dataframe: {}\n------------------------------------------------'.format(size(sys.getsizeof(r_cpd_df))))
del(cpd_df) #conserve memory

cov_df = pd.read_csv("time_series_covid19_confirmed_US.csv")
# print('Full Covid19 US data')
# print(cov_df)
# print('Size of Covid19 dataframe: {}\n------------------------'.format(size(sys.getsizeof(cov_df))))
r_cov_df = cov_df.loc[(cov_df['Province_State'] == 'Tennessee') & (cov_df['Admin2'] == 'Hamilton')] #covid data for Hamilton county, TN
# print('Reduced Covid19 US data')
# print(r_cov_df)
# print('Size of reduced Covid19 dataframe: {}\n------------------------------------------------'.format(size(sys.getsizeof(r_cov_df))))
del(cov_df) #conserve memory

'''
Final plot will be based on daily measurements from both dataframes.
    *Must identify date for each record in both dataframes.
        **Iterate through rows of CPD, making an individual crime sum for each day.
            ***Dictionaries inside dictionary.
                ****{date: {crime: value,crime: value,TOTAL_CRIMES: value},date: {...}}
            ***Use date as key.
            ***Use crimes as value.
            ***Keep total counted crimes for each day and add as a unique TOTAL_DAILY_CRIMES key.
        **Iterate through columns of COV, recording values in dictionary with day as key and cases as value.
            ***Will need to cut off the extra columns at the start and just work with the date columns.
'''

#PLOT CPD DATAFRAME
'''
Note: Multiple charges in same arrest removed. Only counting multiple charges spread out over arrests.
'''
crime_dict_by_date = {} #perform tracking per date
start = time.time() #performance measurement
cnt = 0 #performance measurement
for i,s in tqdm(r_cpd_df.iterrows(),total= 5400,desc='CPD Arrest Data Rows',unit='rows'):
    date = s['Arrest Date'][:9].strip()
    charges = list(set(list(map(str.strip,s['Charges'].split(',')))))
    for c in charges:
        if date not in crime_dict_by_date: #date not present, add it and the first crime
            crime_dict_by_date[date] = {c:1}
            crime_dict_by_date[date]['TOTAL_DAILY_CRIMES'] = 1
        else: #date present
            if c not in crime_dict_by_date[date]: #crime sub-key not present, add crime sub-key
                crime_dict_by_date[date][c] = 1
                crime_dict_by_date[date]['TOTAL_DAILY_CRIMES'] += 1
            else: #crime sub-key present, increment crime sub-key
                crime_dict_by_date[date][c] += 1
                crime_dict_by_date[date]['TOTAL_DAILY_CRIMES'] += 1
    cnt += 1
# print('Processing time (CPD dataset): {} seconds'.format(time.time() - start))
# print('Rows processed: {}\n'.format(cnt))

cpd_df = pd.DataFrame.from_dict(crime_dict_by_date)
cpd_df.fillna(0,inplace=True)
cpd_df.sort_values(by=['1/22/2020'],inplace=True,ascending=False)
cpd_df.drop(cpd_df.tail(1).index,inplace=True)
t_cpd_df = cpd_df.transpose()
plt.figure()
t_cpd_df.plot(legend=False)
plt.xlabel('Date')
plt.ylabel('Frequency')
plt.title('Chattanooga PD Arrests')
plt.show()


#PLOT COV DATAFRAME
cov_df = r_cov_df.loc[:,'1/22/2020':]
t_cov_df = cov_df.transpose()
# t_cov_df.reset_index(level=t_cov_df.index.names, inplace=True)
# t_cov_df.rename(columns={t_cov_df.columns[0]:'Date',t_cov_df.columns[1]:'Cases'}, inplace=True)
t_cov_df.rename(columns={t_cov_df.columns[0]:'Cases'}, inplace=True)
# for i,s in t_cov_df.iterrows():
#     print('{}: \t{} cases'.format(i,s['Cases']))
t_cov_df.cumsum()
plt.figure()
t_cov_df.plot()
plt.xlabel('Date')
plt.ylabel('Cases')
plt.title('Hamilton County Covid Cases')
plt.show()


#OVERLAY PLOTS OF THE DATAFRAMES
#...