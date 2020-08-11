
import pandas as pd

from scipy.interpolate import griddata,interp1d
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

def interpolate_profile(zv,z_int,max_z_extrapolation=10,max_z_copy_single_value=1, kind='linear'):
    
    '''
    
    Interpolate profile zv (depth, parameter) to a user defined depth.
    
    Input:
    zv    - 2-D array of depth and a parameter (temperature, oxygen, ...)
    z_int - 1-D array of depth levles to interpolate to
    max_extrapolation_z - Maximum distance to extrapolate outside profile. Use 0 for no extrapolation.
    max_z_copy_single_value - Maximum distance for copying the value of a single value profile. 
    kind - Specifies the kind of interpolation as a string (‘linear’, ‘nearest’, ‘zero’, ‘slinear’, ‘quadratic’, ‘cubic’, ‘previous’, ‘next’, where ‘zero’, ‘slinear’)
    
    Output:
    
    Returns array of interpolated values
    
    -------------------------------------------------------------------
    
    Example:
    zv=array([[ 0.        , 21.64599991],
       [ 9.93530941, 21.54500008],
       [19.87013626, 20.96299934],
       [20.40699959, 29.80448341],
       [19.36800003, 49.67173004],
       [18.8010006 , 74.50308228],
       [18.27400017, 99.3314209 ]])
       
    z_int=[0,0,25,50,75,100,125]
    
    v_int=interpolate_profile(ZV,z_int)
    
    v_int 
    >>> array([21.64599991, 20.67589412, 19.36050431, 18.79045314, 18.25980907,
               nan])
    
    
    '''
    
    #Sorting by depth
    zv = zv[np.argsort( zv[:,0] )]
    z_int=np.sort(z_int)
    
    #Remove interpolations point outside extrapolation range
    ind=(z_int>=(zv[0,0]-max_z_extrapolation)) & (z_int<=(zv[-1,0]+max_z_extrapolation))
    _z_int=z_int[ind]

   
    n_zv=len(zv)
    v_int=np.ones(len(z_int))*np.nan
    
    if n_zv>1:
        # Interpolation for more than one row
        f=interp1d(zv[:,0], zv[:,1], kind='linear', axis=- 1, copy=True, bounds_error=None, fill_value='extrapolate', assume_sorted=True)
        _v_int=f(_z_int) 
    elif  n_zv==1:
        # Only one row of data, copy value if within distance (max_z_copy_single_value)
        dist=_z_int-zv[0,0]
        _v_int[dist<=max_z_copy_single_value]=zv[0,1]
           
    v_int[ind]=_v_int
            

    return v_int
            


def plot_grid(int_lon,int_lat,g,cmap='viridis'):
    
    '''
    Plot gridded data
    '''
    
    
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
    
    '''
    Plot raw point data
    '''
    
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
