
from scipy.interpolate import griddata
from datetime import timedelta
import pandas as pd
def interpolate_casts(df,variable,int_lon,int_lat,int_datetime,days_buffer=90):
    
   
    int_time=pd.to_datetime(int_datetime)
    
    if ((int_time)<df['datetime'].min()) or ((int_time)>df['datetime'].max()) :
        raise Warning('Interpolation time outside dataset')
        
    
    dt=timedelta(days=days_buffer)
    X=df[(df['datetime']>(int_time-dt)) & (df['datetime']<(int_time+dt)) ]
    
    y=X[variable]
    X=X[['longitude','latitude','datetime']]
    X['datetime']=X['datetime']-int_time
    X['datetime']=X['datetime'].apply(lambda x : x.total_seconds())
    
    g = griddata(X,y, (int_lon, int_lat,0), method='linear',rescale=True)
    
    return g
#def check_if_sequence_is_within_query(seq,longitude,latitude,timespan,depth):
    
    #param_range=timespan
    #param_name='timestamp'
    #if (param_range[0]<=float(seq['{}_start'.format(param_name)])) and (param_range[1]<=float(seq['{}_start'.format(param_name)])):
        #return False
    #elif (param_range[0]>=float(seq['{}_end'.format(param_name)])) and (param_range[1]>=float(seq['{}_end'.format(param_name)])):
        #return False        
    
    #param_names=['longitude','latitude','depth']
    #param_ranges=[longitude,latitude,depth]
    
    #for param_range,param_name in zip(param_ranges,param_names):
        #if (param_range[0]<=float(seq['{}_min'.format(param_name)])) and (param_range[1]<=float(seq['{}_min'.format(param_name)])):
            #return False
        #elif (param_range[0]>=float(seq['{}_max'.format(param_name)])) and (param_range[1]>=float(seq['{}_max'.format(param_name)])):
            #return False
                 
    
    #return True

