import pandas as pd
import os
import numpy as np
import glob
import datetime as datetime
global date

'''
===========
DESCRIPTION
===========

This program uses the input of daily raw data CSV files
and cleans, processes and aggregates the data into
a summarized and meaningful dataset.

'''


# Insert date and month of the required raw data daily file.
date = "2018-12-31"
month = date[0:7] 
# Directory that raw data is saved.
directory_from = 'C:\Users\warren'
# Directory that processes data will be daved.
directory_to = 'G:\My Drive'
dataSet = 'G:\My Drive\DataSet2018-12.csv'

# Cancels error message
pd.options.mode.chained_assignment = None

removeData = '{}\{}\{}\Raw_data'.format(directory_from, month,date)
rawData = glob.glob(r'{}\{}\{}\Raw_data\*.csv'.format(directory_from, month,date))
path_to = '{}\\{}\\{}\\Aggregated\\'.format(directory_to,month, date)
path_to_loadProfile = '{}\\Load Profile Data\\'.format(directory_to)

def remove_files():
    # Removes raw data files that are smaller than 30kb (empty files)
    files_in_dir = os.listdir(removeData)
    for file in files_in_dir:
        file = removeData + '\\' +  file
        if os.path.getsize(file) < 40 * 1024:
            try:
                os.remove(file)
            except:
                continue

def process_raw_files():
    '''
    Iterates through each csv file and saves an aggregated data file.

    Summarizes HOURS OF USE, BATTERY VOLTAGE metrics and use between LIGHT
    and LIGHTS + MEDIA

    '''
    for f in rawData:
        try:
            g = os.path.basename(os.path.splitext(f)[0])
            all_data = pd.read_csv(f, index_col=0)
            all_data.index = pd.to_datetime(all_data.index)
            all_data['Hour'] = all_data.index.hour + 1 
            all_data['Lights'] = 0 
            all_data['Lights'][(all_data['Output Power, (W)'] > 0.6) & (all_data['Output Power, (W)'] < 3.9)] = 1  #12
            all_data['Minutes_of_lights'] = all_data['Lights'].cumsum()
            all_data['Lights+Media'] = 0
            all_data['Lights+Media'][(all_data['Output Power, (W)'] >= 3.9)] = 1
            all_data['Minutes_of_Lights+Media'] = all_data['Lights+Media'].cumsum()
            all_data['Total_Hours-Of_Use (h)'] = (all_data['Minutes_of_lights'] + all_data['Minutes_of_Lights+Media'])/60
            all_data['Max Watt, W'] = all_data.groupby('Hour')['Output Power, (W)'].transform('max')
            all_data['Battery Voltage, V (Max)'] = all_data.groupby('Hour')['Battery Voltage, (V)'].transform('max')
            all_data['Battery Voltage, V (Min)'] = all_data.groupby('Hour')['Battery Voltage, (V)'].transform('min')
            all_data['Battery Temperature, C'] = all_data.groupby('Hour')['Battery Temperature, (degC)'].transform('mean').fillna(0)
            all_data['Battery Voltage, V (Avg)'] = all_data.groupby('Hour')['Battery Voltage, (V)'].transform('mean').fillna(0)

            idx = (all_data['Battery Voltage, (V)'] >= 14.0).idxmax()
            all_data['Time of FC'] = np.where(all_data.index == idx, idx.strftime('%H%M'), '')
            all_data['Time of Full Charge'] = all_data.groupby('Hour')['Time of FC'].transform('max')

            df2 = all_data.iloc[59::60,[11,7,16,15,13,18,19,17,20,10,21,23]]
            
            df2['Output Energy, (Wh/h)'] = df2['Output Energy, (Wh)'].diff().fillna(df2['Output Energy, (Wh)'])
            df2['Minutes_Light_/h'] = df2['Minutes_of_lights'].diff().fillna(df2['Minutes_of_lights'])
            df2['Minutes_Light+Media_/h'] = df2['Minutes_of_Lights+Media'].diff().fillna(df2['Minutes_of_Lights+Media'])
            df2['Lights (Wh)'] = 0
            df2['Lights (Wh)'][(df2['Output Energy, (Wh/h)']>0) &(df2['Minutes_Light_/h']>0)] =(df2['Minutes_Light_/h']/(df2['Minutes_Light_/h']+df2['Minutes_Light+Media_/h']))*df2['Output Energy, (Wh/h)']
            df2['Lights+Media (Wh)'] = 0
            df2['Lights+Media (Wh)'][(df2['Output Energy, (Wh/h)']>0) &(df2['Minutes_Light+Media_/h']>0)] =(df2['Minutes_Light+Media_/h']/(df2['Minutes_Light_/h']+df2['Minutes_Light+Media_/h']))*df2['Output Energy, (Wh/h)']

            df2['Lights (h)'] =df2['Minutes_Light_/h']/60
            df2['Light+Media (h)'] =df2['Minutes_Light+Media_/h']/60
            df2['Total Usage (h)']= (df2['Minutes_Light_/h']+df2['Minutes_Light+Media_/h'])/60
            df2['Charge Energy, Wh/h'] =df2['Charge Energy, (Wh)'].diff().fillna(df2['Charge Energy, (Wh)'])

            df3 = df2.iloc[:,[0,12,15,16,19,17,18,5,6,10,7,8,20,11]]
            df3 = df3.set_index('Hour')

            df3.columns = pd.MultiIndex.from_arrays([[g] * len(df3.columns),df3.columns], names=(None,None))            
            df3 = df3.round(decimals=3)
            df3 = df3.T
            df3 = df3.apply(pd.to_numeric, errors='coerce')
            L = ['Battery Voltage, V (Max)','Max Watt, W']
            M = ['Battery Voltage, V (Avg)','Battery Temperature, C']
            mask1 = df3.index.get_level_values(1).isin(L)
            mask3 = df3.index.get_level_values(1).isin(M)
            mask2 = df3.index.get_level_values(1) == 'Battery Voltage, V (Min)'
            df3['Total/Max/Min'] = np.where(mask1, df3.max(axis=1),np.where(mask2, df3.min(axis=1),np.where(mask3, df3.mean(axis=1), df3.sum(axis=1))))
     
            filename = 'Aggregated-{}'.format(g)
            file_csv = '{}.csv'.format(filename)
            try:
                df3.to_csv(os.path.join(path_to, file_csv))
            except IOError:
                os.makedirs(path_to)
                df3.to_csv(os.path.join(path_to, file_csv))
        except:
            continue
    print ('Step 1 - Saved all individual aggregated files to Aggregated folder')

def create_daily_aggregated_file(month, date):
    # iterates through all aggregated files to create a summarized file.
    files = glob.glob(r'{}\{}\\{}\Aggregated\*.csv'.format(directory_to,month,date))
    df = pd.concat([pd.read_csv(f, index_col=[0,1])for f in files],axis=0)
    df.index = df.index.set_names('CU', level=0)
    df.index = df.index.set_names('Parameters', level=1)

    try:
        df.to_csv(os.path.join(path_to, 'Aggregated_Daily_All.csv'))
    except IOError:
        os.makedirs(path_to)
        df.to_csv(os.path.join(path_to, 'Aggregated_Daily_All.csv'))
    print ('Step 2 - Saved an aggregated file in the Aggregated folder')

def create_profile_data(month, date):
    # creates a load profile file, using the aggregated files for the day.
    df = pd.read_csv('{}\\{}\\{}\\Aggregated\\Aggregated_Daily_All.csv'.format(directory_to,month, date), index_col=[0,1])
    df = df.apply(pd.to_numeric, errors='coerce').dropna(how='all')
    df = df.apply(pd.to_numeric, errors='coerce').dropna(how='all').groupby(level=1).mean()
    df.insert(0, 'Date', date)
    try:
        df.to_csv(os.path.join(path_to_loadProfile, '{}.csv'.format(date)))
    except IOError:
        os.makedirs(path_to_loadProfile)
        df.to_csv(os.path.join(path_to_loadProfile, '{}.csv'.format(date)))
    print ('Step 3 - Saved Load Profile Files!')

def update_monthly_dataset(month, date):
    # updates the main dataset with the days data.
    df1 = pd.read_csv(r'{}\{}\{}\Aggregated\Aggregated_Daily_All.csv'.format(directory_to,month,date), usecols=['CU', 'Parameters', 'Total/Max/Min'], index_col =[0,1])
    df1 = df1.rename(columns = {'Total/Max/Min':date}) # Change column name
    df2 = pd.read_csv(r'{}'.format(dataSet), index_col = [0,1])
    df3  = pd.concat([df2, df1], axis=1)
    df3.to_csv(r'{}'.format(dataSet))
    print 'Step 4 - DataSet file has been updated'

if __name__ == '__main__':
    remove_files()
    process_raw_files()
    create_daily_aggregated_file(month, date)
    create_profile_data(month, date)
    update_monthly_dataset(month,date)
