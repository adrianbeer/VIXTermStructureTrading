# -*- coding: utf-8 -*-
"""
Created on Thu Nov 19 13:11:43 2020

@author: Adrian
"""
import os
import pandas as pd

data_path = r'D:/finance/DATA'
#dataframe_path = f"{path}/dataframe.pkl"


def get_name_from_path(file_path):
    if '\\' in file_path:
        file_name = file_path.split('\\')[-1]
    else:
        file_name = file_path.split('/')[-1]
    stock_name = file_name.split('.')[0]
    return stock_name


#%%
def tai_pan_dir_to_dataframe_extended(path, subset=None):
    df_list = []
    for file in os.listdir(path):
        if not file.split(".")[-1] == "TXT": continue
        if subset and file.split(".")[0] not in subset: continue
        file_path = f"{path}\\{file}"
        df = file_to_dataframe(file_path)
        df_list.append(df)
    return pd.concat(df_list, axis=0).sort_index()


#%%
def file_to_dataframe(file_path, sep='\t', _open=True, _format='%d.%m.%Y'):
    '''Converts a Tai-Pan ASCII exported file to a DataFrame. 
    (TODO) Function can be generalized to include more columns in the future.
    
    Function assumes the following structure in the file:
    DATE CLOSE [OPEN]
    
    Instructions for (default parameters) Tai-Pan ASCII-export:
    Tick: Schlußkurse & Eröffnungskurse, Tabulator, Datumsformat: TT.MM.JJJJ
    
    :returns: Returns a df with the following structure::
        
        +------+----+-------+------+------+
        |  .   | .  | Close | Open | etc. |
        +------+----+-------+------+------+
        | Date | ID |       |      |      |
        | .    | .  |     . |    . |    . |
        +------+----+-------+------+------+
    
    '''
    # Read file
    df = pd.read_csv(file_path, sep=sep)
    
    # Remove trash column
    trash_row = df.columns[-1]
    df.drop(columns=[trash_row], inplace=True)

    # Make date column to datetimeindex
    date_column = df.iloc[:, 0]
    date_row_name = df.columns[0]
    df.drop(columns=[date_row_name], inplace=True)
    
    # Create MultiIndex from date and `_id`
    dt_index = pd.to_datetime(date_column, format=_format)
    _id = get_name_from_path(file_path)
    

    df.index = pd.MultiIndex.from_product([dt_index, [_id]])
    df.index.names = ["Date", "ID"]
    df.columns = ['Close', "Open"] if _open else ["Close"]
    
    ## Remove Holidays
    # 01-Jan
    df = df.loc[~((df.index.get_level_values('Date').day == 1) & (df.index.get_level_values('Date').month == 1)), :]
    
    # 24,25,26,31 Dec
    for holiday in [24,25,26,31]:
        df = df.loc[~((df.index.get_level_values('Date').day == holiday) & (df.index.get_level_values('Date').month == 12)), :]    
    
    return df

