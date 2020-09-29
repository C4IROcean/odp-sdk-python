import seaborn as sns
import os
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import mpl_toolkits
from mpl_toolkits.basemap import Basemap, addcyclic, shiftgrid
import cartopy.crs as ccrs
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter
import cartopy.feature as cfeature
import cmocean
from matplotlib.colors import BoundaryNorm
from matplotlib.ticker import MaxNLocator
from mpl_toolkits.axes_grid1 import make_axes_locatable
import scipy
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
from matplotlib.lines import Line2D


def geo_map(ax):
    '''
    helper function for mapping
    '''
    ax.yaxis.set_major_formatter(LatitudeFormatter())
    ax.tick_params(axis="x", labelsize=6)
    ax.tick_params(axis="y", labelsize=6)
    # add land and coastline
    ax.add_feature(cfeature.LAND, facecolor='whitesmoke', zorder=1, edgecolor='black')
    ax.add_feature(cfeature.COASTLINE, linewidth=0.25, zorder=1)
    ax.add_feature(cfeature.BORDERS, linewidth=0.25, zorder=1)
    ax.add_feature(cfeature.OCEAN)



 def plot_meta_stats(df, variable):
    '''
    Get bar graph of percentage of data belonging to a specific variable subset in the metadata

    input: pandas dataframe with extId present
    input: variable that is present in the dataframe (i.e. country, dataset_code)
    returns: bar graph with percentage of data belonging to variable subset (
    i.e. data belonging to different modes of data collection ('dataset'_code))

    '''
    colors = sns.color_palette('bright') + sns.color_palette('deep') + sns.color_palette('Set3')
    df2 = df[[variable, 'extId']].groupby(variable).count().sort_values(by='extId', ascending=False).reset_index(0)
    plt.figure(figsize=(10,6));
    ax = sns.barplot(y= df2[variable], x = df2['extId']/df2['extId'].sum()*100, palette=colors)
    ax.set_ylabel(variable,size=15);
    ax.set_xlabel('Percentage of casts',size=15);
    ax.tick_params(axis='both', which='major', labelsize=12);

    for p in ax.patches:
        _x = p.get_x() + p.get_width()
        _y = p.get_y() + p.get_height() - 0.3
        value = p.get_width().round(3)
        ax.text(_x, _y, str(value) + '%', ha="left", fontsize=10);


def plot_datasets(df, variable, latitude=None, longitude=None):
    '''
    Plots on a map casts belonging to specific dataset (mode of data collection, i.e. ctd, xbt)
    Input:  pandas dataframe incuding extId and variable of choice
            bounding box for lat and lon, default is none
    Output: map with color coded casts based on dataset_code
    '''

    if latitude and longitude:
        df = df[(df.lat.between(latitude[0], latitude[1])) & (df.lon.between(longitude[0], longitude[1]))]
    else:
        df = df
    variable = variable
    variable_list = list(df[[variable, 'extId']].groupby(variable).count().reset_index(0).sort_values(by='extId', ascending=False)[variable])
    colors = sns.color_palette('bright') + sns.color_palette('deep') + sns.color_palette('Set3')

    fig = plt.figure(figsize=(15, 15))
    color_plot =colors [0:len(df[variable].unique())]
    ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
    legend_elements = []
    for i, j in enumerate(variable_list):
        sns.scatterplot(x="lon", y="lat", data=df[df[variable]==j], color = color_plot[i], s=30, marker='o', edgecolor='white', linewidths=0.05)
        legend_elements.append(Line2D([0], [0], color = colors[i], lw=4, label=variable_list[i]))
    if latitude and longitude:
        ax.set_extent([longitude[0], longitude[1], latitude[0], latitude[1]],crs=ccrs.PlateCarree())
    gl = ax.gridlines(crs=ccrs.PlateCarree(), draw_labels=True, linewidth=1, color='gray', alpha=0.7, linestyle=':')
    gl.xformatter = LONGITUDE_FORMATTER
    gl.yformatter = LATITUDE_FORMATTER
    ax.legend(handles=legend_elements, loc='lower center',
               ncol=4, borderaxespad=-7., prop={'size': 12});
    geo_map(ax), plot_meta_stats(df, variable)
