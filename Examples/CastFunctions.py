
import pandas as pd
import numpy as np
import scipy.interpolate 
from datetime import timedelta
from mpl_toolkits.basemap import Basemap
from matplotlib import pyplot as plt
import  metpy.interpolate 




def interpolate_casts_to_z(df,variable,z_int,max_z_extrapolation=3,max_z_copy_single_value=1, kind='linear'):

    '''

    Interpolate profiles in dataframe to prescribed depth level.

    Takes a complete dataframe from ODP and interpolates each cast by filtering out the values from each unique cast

    Input:
    df : Pandas dataframe fromODP 
    variable : Variable name to be interpolated as in the dataframe (Temperature, Oxygen, etc)
    z_int: List of the desired depth intervals to return, i.e [0,10,20] 
    max_z_extrapolation: The maximum length to allow extrapolating. Nan values outside this distance.
    max_z_copy_single_value: If only one row is present in the cast, this is the maximum distance between the point and the interpolation level for copying the value
    kind : Type of interpolation as in interpolate_profile

    Return:
    Dataframe of parameter values at prescribed depth levels.


    '''


    nz=len(z_int)
    data=np.zeros((len(df.externalId.unique())*nz,6),'O')
    for i,ext_id in enumerate(df.externalId.unique()):
        _df=df[df.externalId==ext_id]
        v_int=interpolate_profile(_df[['z',variable]].values,z_int,max_z_extrapolation,max_z_copy_single_value, kind)
        data[i*nz:(i+1)*nz,0:4]=(ext_id,_df.datetime.iloc[0],_df.lon.iloc[0],_df.lat.iloc[0])
        data[i*nz:(i+1)*nz,4]=z_int
        data[i*nz:(i+1)*nz,5]=v_int

    return pd.DataFrame(data,columns=['externalId','datetime','lon','lat','z',variable]).astype({'lon': float,'lat': float, variable: float,'z':float})





def interpolate_to_grid(points,values,int_points, interp_type='linear', minimum_neighbors=3,
                       gamma=0.25, kappa_star=5.052, search_radius=0.1, rbf_func='linear',rbf_smooth=0.001,rescale=True):



    '''

    Interpolate unstructured 2D data to a 2d grid

    Powered by the metpy library

    Input
    points
        (N,2) array of points, typically latitude and longitude
    values
        (N,1) array of corresponding values, i.e Temperature, Oxygen etc
    int_points:
        list of arrays for gridding i.e lat/long grid --> (np.linspace(-25,35,60*10+1),np.linspace(50,80,30*10+1))
    interp_type: str
        What type of interpolation to use. Available options include:
        1) "linear", "nearest", "cubic", or "rbf" from `scipy.interpolate`.
        2) "natural_neighbor", "barnes", or "cressman" from `metpy.interpolate`.
        Default "linear".
    minimum_neighbors: int
        Minimum number of neighbors needed to perform barnes or cressman interpolation for a
        point. Default is 3.
    gamma: float
        Adjustable smoothing parameter for the barnes interpolation. Default 0.25.
    kappa_star: float
        Response parameter for barnes interpolation, specified nondimensionally
        in terms of the Nyquist. Default 5.052
    search_radius: float
        A search radius to use for the barnes and cressman interpolation schemes.
        If search_radius is not specified, it will default to the average spacing of
        observations.
    rbf_func: str
        Specifies which function to use for Rbf interpolation.
        Options include: 'multiquadric', 'inverse', 'gaussian', 'linear', 'cubic',
        'quintic', and 'thin_plate'. Defualt 'linear'. See `scipy.interpolate.Rbf` for more
        information.
    rbf_smooth: float
        Smoothing value applied to rbf interpolation.  Higher values result in more smoothing.

    Returns
    -------
    values_interpolated: (M,) ndarray
        Array representing the interpolated values for each input point 



    '''



    if rescale==True:   
        norm_mean=points.mean(axis=0)
        points-=norm_mean

        norm_max=points.max(axis=0)
        points/=norm_max 

        for i in range(len(int_points)):
            int_points[i]-=norm_mean[i]
            int_points[i]/=norm_max[i]

    grid_points=np.meshgrid(*int_points)
    grid_points_ravel=[ c.ravel() for c in grid_points ] 
    grid_points_ravel=np.array(grid_points_ravel).T        
    
    if interp_type!='rbf':
        grid_values=metpy.interpolate.interpolate_to_points(points,values, grid_points_ravel, interp_type=interp_type, minimum_neighbors=minimum_neighbors,
                                                        gamma=gamma, kappa_star=kappa_star, search_radius=search_radius, rbf_func=rbf_func,
                              rbf_smooth=rbf_smooth).reshape(grid_points[0].shape)     
    else:
        
        points_transposed = np.array(points).transpose()
        xi_transposed = np.array(grid_points_ravel).transpose()
        rbfi = scipy.interpolate.Rbf(*points_transposed, values, function=rbf_func,
                   smooth=rbf_smooth)
        grid_values=rbfi(*xi_transposed).reshape(grid_points[0].shape)         

    if rescale==True:
        for i in range(len(int_points)):
            int_points[i]*=norm_max[i] 
            int_points[i]+=norm_mean[i]
        grid_points=np.meshgrid(*int_points)     

    return grid_points,grid_values



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

    n_zv=len(zv)
    v_int=np.ones(len(z_int))*np.nan

    if n_zv>1:
        # Interpolation for more than one row
        #Remove interpolations point outside extrapolation range
        ind=(z_int>=(zv[0,0]-max_z_extrapolation)) & (z_int<=(zv[-1,0]+max_z_extrapolation))
        _z_int=z_int[ind]        
        f=scipy.interpolate.interp1d(zv[:,0], zv[:,1], kind='linear', axis=- 1, copy=True, bounds_error=None, fill_value='extrapolate', assume_sorted=True)
        v_int[ind]=f(_z_int) 
    elif  n_zv==1:
        # Only one row of data, copy value if within distance (max_z_copy_single_value)
        dist=z_int-zv[0,0]
        v_int[dist<=max_z_copy_single_value]=zv[0,1]



    return v_int

def plot_grid(int_lon,int_lat,g,cmap='viridis',vrange=None):

    '''

    Plot a grid on a map

    Input:
    int_lon - (M,N) array of longitude grid
    int_lat - (M,N) array of latitude grid
    g - (M,N) grid to be shown
    cmap - colormap
    vrange - Ranges for grid to be shown i.e [0,35]

    '''

    plt.figure(figsize=(16,10))


    m = Basemap(epsg='4326',resolution='l')
    m.drawcoastlines()
    m.fillcontinents()

    xp, yp = m(int_lon, int_lat)

    if vrange is None:

        cs = m.pcolormesh(xp[:,:], yp[:,:], g[:,:],cmap=cmap)#,vmin=0,vmax= 35)
    else:
        cs = m.pcolormesh(xp[:,:], yp[:,:], g[:,:],cmap=cmap,vmin=vrange[0],vmax=vrange[1])

    cbar=m.colorbar(cs,location='bottom',pad= "5%")
    plt.xlim(int_lon.min(),int_lon.max()) 
    plt.ylim(int_lat.min(),int_lat.max())    


def get_units():
    units_dict = {'z': 'm', 
                  'Oxygen':'umol/kg',
     'Temperature':'degree_C',
     'Chlorophyll':'ugram/l',
     'Pressure':'dbar',
     'Nitrate': 'umol/kg',
     'Latitude':'degrees_north',
     'Longitude':'degrees_east'}
    return(units_dict)   

def plot_casts(variable,df,cmap='viridis',vrange=None):

    '''

    Plot raw casts on a map

    Input:
    variable - Temperature, Oxygen, Salinity, etc as in dataframe
    df - Pandas dataframe from ODP
    cmap - colormap
    vrange - Ranges for grid to be shown i.e [0,35]

    '''

    plt.figure(figsize=(16,10))
    plt.title(variable, fontsize=16)    

    m = Basemap(resolution='l')
    m.drawcoastlines()
    m.fillcontinents()

    x, y = m(df['lon'], df['lat'])
    v=df[variable].values

    if vrange is None:

        sc=plt.scatter(x,y,c=v,cmap=cmap) 
    else:
        
        sc=plt.scatter(x,y,c=v,cmap=cmap,vmin=vrange[0],vmax=vrange[1]) 

    plt.xlim(df.lon.min(),df.lon.max()) 
    plt.ylim(df.lat.min(),df.lat.max())

    m.colorbar(sc,location='bottom',pad= "5%").set_label('{} ({})'.format(variable, get_units()[variable]), fontsize=14) 



#import cmocean
#df_surf=pd.read_pickle(open('surfdata_intz0.pkl','rb'))
#df_surf=df_surf[(df_surf.datetime>'2018-06-01')& (df_surf.datetime<'2018-08-30') &(df_surf.z==10)]
#df_surf.dropna(inplace=True)
#df_surf['unixtime']=df_surf['datetime'].apply(lambda x : x.value) 
#df_surf.dropna(inplace=True)
#points=df_surf[['lon','lat','unixtime']].values.astype('float')
#values=df_surf['Temperature'].values .astype('float')

#int_points=[np.linspace(-25,35,60*4+1),np.linspace(50,80,30*4+1),np.linspace(pd.Timestamp('2018-06-01').value,pd.Timestamp('2018-08-31').value,8)]

#grid,g=inerpolate_to_grid(points.copy(),values.copy(),int_points.copy(), interp_type='rbf', 
                      #rbf_func='linear',rbf_smooth=0.0001,rescale=True)

#plot_grid(grid[0][:,:,0],grid[1][:,:,0],g[:,:,1],cmap=cmocean.cm.thermal,vrange=[0,20])#;plt.title(interp_type)

#plt.show()