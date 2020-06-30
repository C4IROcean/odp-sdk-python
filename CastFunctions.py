
import pandas as pd
from scipy.interpolate import griddata
from datetime import timedelta

def interpolate_casts(df,variable,int_lon,int_lat,int_datetime,days_buffer=90):
    
   
    int_time=pd.to_datetime(int_datetime)
    
    if ((int_time)<df['datetime'].min()) or ((int_time)>df['datetime'].max()) :
        raise Exception('Interpolation time outside dataset')
        
    
    dt=timedelta(days=days_buffer)
    X=df[(df['datetime']>(int_time-dt)) & (df['datetime']<(int_time+dt)) ]
    
    y=X[variable]
    X=X[['lon','lat','z','datetime']]
    X['datetime']=X['datetime']-int_time
    X['datetime']=X['datetime'].apply(lambda x : x.total_seconds())
    
    g = griddata(X,y, (int_lon, int_lat,0), method='linear',rescale=True)
    
    return g

