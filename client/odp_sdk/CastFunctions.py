
import pandas as pd

from scipy.interpolate import griddata
from datetime import timedelta
from mpl_toolkits.basemap import Basemap
from matplotlib import pyplot as plt



def interpolate_casts(df,variable,int_lon,int_lat,int_datetime,days_buffer=90):
    '''
    
    Interpolate dataframe of casts into a specific time and coordinate/grid
    
    '''
   
    int_time=pd.to_datetime(int_datetime)
    
    if ((int_time)<df['datetime'].min()) or ((int_time)>df['datetime'].max()) :
        raise Exception('Interpolation time outside dataset')
        
     
    dt=timedelta(days=days_buffer)
    X=df[['lon','lat','datetime',variable]][(df['datetime']>(int_time-dt)) & (df['datetime']<(int_time+dt)) ]
    X.dropna(inplace=True)
    y=X[variable]
    X=X[['lon','lat','datetime']]
    X['datetime']=X['datetime']-int_time
    X['datetime']=X['datetime'].apply(lambda x : x.total_seconds())
    
    g = griddata(X,y, (int_lon, int_lat,0), method='linear',rescale=True)
    
    return g

def plot_grid(int_lon,int_lat,g,cmap='viridis'):
    
    plt.figure(figsize=(16,10))
    

    m = Basemap(epsg='4326',resolution='l')
    m.drawcoastlines()
    m.fillcontinents()

    xp, yp = m(int_lon[:,:,0], int_lat[:,:,0])

    cs = m.pcolormesh(xp[:,:], yp[:,:], g[:,:,0],cmap=cmap)#,vmin=0,vmax= 35)

    cbar=m.colorbar(cs,location='bottom',pad= "5%")
    plt.xlim(int_lon.min(),int_lon.max()) 
    plt.ylim(int_lat.min(),int_lat.max())    
    
    

   

def plot_casts(variable,df,cmap='viridis'):
    
    plt.figure(figsize=(16,10))
    plt.title(variable)    
    
    m = Basemap(resolution='l')
    m.drawcoastlines()
    m.fillcontinents()

    x, y = m(df['lon'], df['lat'])
    v=df[variable].values
    sc=plt.scatter(x,y,c=v,cmap=cmap) 

    plt.xlim(df.lon.min(),df.lon.max()) 
    plt.ylim(df.lat.min(),df.lat.max())

    m.colorbar(sc,location='bottom',pad= "5%") 