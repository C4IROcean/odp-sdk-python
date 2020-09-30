import seaborn as sns
import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import mpl_toolkits
import cmocean
from matplotlib.colors import BoundaryNorm
from matplotlib.ticker import MaxNLocator
from mpl_toolkits.axes_grid1 import make_axes_locatable
import scipy
from matplotlib.lines import Line2D
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
sns.set_palette(sns.color_palette("hls", 47))


def plot_missing(df, var_list=None):
    ''' 
    Plot percentage of nulls for each variable in variable list.

    Takes a dataframe from ODP and a list of variables and plots the percentage of missing values

    Input:
    df: Pandas dataframe from ODP
    var_list: list of variables (column names) that user is interested in
    default list is all the columns

    Return: Plot of percentage of values missing at each measuremtn (lat, lon, depth)

    '''
    
    if var_list:
        if isinstance(var_list, list) == False:
            var_list = [var_list]
        if all(elem in df.columns for elem in var_list):
            info = df[var_list]
        else:
            print('Variable not in dataframe')
    else:
        info=df
    

    info_nulls = info.isnull().sum(axis = 0).sort_values()
    variables = info_nulls.index ## variables we want to plot
    perct = np.round(info_nulls.values/len(info), decimals=4)*100 ##percentage missing

    plt.figure(figsize=(10,8));
    ax = sns.barplot(y= variables, x = perct, color='cornflowerblue');
    ax.set_title('Percentage of Values Missing (Total Rows: {})'.format(len(df)), fontsize=20);
    ax.set_ylabel('Variables',size=15);
    ax.set_xlabel('Percent Missing',size=15);
    ax.tick_params(axis='both', which='major', labelsize=12);

    ## add values next to bar
    for p in ax.patches:
        _x = p.get_x() + p.get_width() + 0.03;
        _y = p.get_y() + p.get_height() - 0.3;
        value = p.get_width().round(3);
        ax.text(_x, _y, str(value) + '%', ha="left", fontsize=10);

    return ax

def missing_values(df, var_list):
    ''' 
    Get dataframe of nulls for each variable in variable list.

    Takes a dataframe from ODP and a list of variables and return dataframe of missing values

    Input:
    df: Pandas dataframe from ODP
    var_list: list of variables (column names) that user is interested in
    default list is all the columns. Variables are strings. 

    Return: Dataframe percentage of values missing at each measuremtn (lat, lon, depth)

    '''
    
    if var_list:
        if isinstance(var_list, list) == False:
            var_list = [var_list]
        if all(elem in df.columns for elem in var_list):
            info = df[var_list]
        else:
            print('Variable not in dataframe')
    else:
        info=df
    

    info_nulls = info.isnull().sum(axis = 0).sort_values()
    variables = info_nulls.index ## variables we want to plot
    perct = np.round(info_nulls.values/len(info), decimals=4)*100 ##percentage missing

    return pd.DataFrame({'Variables': variables, 'Null Values': info_nulls.values, 'Percentage Missing': perct})

    
# def cast_stats(yr_start, yr_end, longitude, latitude):
#     print('Fetching casts for {}'.format(yr_start, yr_end))
#     casts=client.get_available_casts(yr_start,yr_end)#.sample(1000)
#     casts = casts[(casts['lat'] >=latitude[0]) & (casts['lat'] <= latitude[1])]
#     casts = casts[(casts['lon'] >=longitude[0]) & (casts['lon'] <=longitude[1])]
#     metadata = []
#     print('Collecting metadata')
#     for cast_name in notebook.tqdm(casts.extId.unique()):
#         metadata.append(client.sequences.retrieve(external_id = cast_name).to_pandas().transpose())
#     md_df = pd.concat(metadata)
#     return md_df



def plot_distributions(df, var_list):
    '''
    Plot the distributions of the values for a list of variables
    
    Input:
    df: Pandas dataframe from ODP containing oceanographic variables and values
    var_list: list of variables (column names) that should be plotted
    
    Return: Plots of distriubtions of values for each variable in variable list
    '''
    if isinstance(var_list, list) == False:
            var_list = [var_list]
    
    for variable in var_list:
        plt.figure(figsize=(10,6));
        ax = sns.distplot(df[variable], kde=False);
        ax.set_ylabel('Count',size=15);
        ax.set_xlabel(variable + ' value',size=15);
        ax.tick_params(axis='both', which='major', labelsize=12);


