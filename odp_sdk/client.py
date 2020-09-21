
import time
import itertools
import math
import pandas as pd
from cognite.client import CogniteClient
from multiprocessing.dummy import Pool as ThreadPool


class ODPClient(CogniteClient):
    
    '''
    
    Main entrypoint into the Ocean Data Platform SDK. 
    All services are made available through this object.
    
    Download cast data, containing ocean measurements through the water column around the globe.
    
    Example:
    
    from client import ODPClient
    
    client = ODPClient(api_key='....................')
                       
    df=client.casts(longitude=[-10,35],
                    latitude=[50,80],
                    timespan=['2018-03-01','2018-09-01']) 
    
    '''
    
    def __init__(self, api_key=None, project='odp', client_name='ODPPythonSDK', base_url=None, max_workers=None, headers=None, timeout=None, token=None, disable_pypi_version_check=None, debug=False):
        '''
        
        Constructor. ODP client inherits all properties and functions from CogniteClient
        
        '''

        super().__init__(api_key, project, client_name, base_url, max_workers, headers, timeout, token, disable_pypi_version_check, debug)   
        
        if self.login.status().logged_in==False:
            print('Connection attempt failed')
        else:
            print('Connection successful')
        
            
            
        
    def casts(self,longitude=[-180,180],latitude=[-90,90],timespan=['1700-01-01','2050-01-01'],n_threads=35, include_flagged_data = True, parameters=None):
        
        '''
        
        Download cast data within search criteria
        
        Input:
        
        longitude: list of min and max logitude, i.e [-10,35]
        latitude : list of min and max latitude, i.e [50,80]
        timespan : list of min and max datetime string ['YYYY-MM-DD'] i.e ['2018-03-01','2018-09-01']
        inclue_flagged_data : Boolean, whether flagged data that is flagged should be included or not
        parameters: List of parameters to be included in dataframe. If None all column are included. I.e. parameters=['date','lon','lat','Temperature','Oxygen']
        
        Return:
        
        Pandas dataframe with cast data

        
        '''
        
        n_threads_max=35
        if n_threads>n_threads_max:
            print('Maximum allowable number of threads is {}'.format(n_threads_max))
            n_threads=n_threads_max        
        
        t0=time.time()
        print('Locating available casts..')
        
        casts=self.get_available_casts(longitude, latitude, timespan,n_threads,
                                      meta_parameters=['extId','lat','lon','date'])

        cast_names_filtered=casts['extId'].tolist()
        print('-> {} casts found'.format(len(cast_names_filtered)))        
        
        
        if cast_names_filtered==[]:
            print('No casts found in search')
            return None  
        
        # Including flag columns to remove flagged data points
        if not include_flagged_data and (parameters is not None):
            parameters_with_flags=['z','Oxygen','Temperature','Salinity','Chlorophyll','Nitrate','pH']
            parameters_org=parameters.copy()
            for p in parameters_org:
                if p in parameters_with_flags:
                    parameters+=[p+'_WODflag']
                
        print('Downloading data from casts..')
        data=self.download_data_from_casts(cast_names_filtered,n_threads,parameters)
        
        if data.empty:
            print('No available data found in casts')
            return None
        
        data['datetime']=pd.to_datetime(data['date'],format='%Y%m%d') #Adding a column with datetime
        
        # Setting flagged measurements to None if not include_flagged_data
        if not include_flagged_data:
            for var in data.columns:
                if var+'_WODflag' in data.columns:
                    mask = data[var+'_WODflag'] != 0
                    data.loc[mask, var] = None
            if parameters is not None:
                data=data[['externalId','datetime']+parameters_org]
        
        print('-> {} data rows downloaded in {:.2f}s'.format(len(data),time.time()-t0))

        return data
            
    
    def get_casts_from_level2(self,year_start,year_end,longitude=[-180,180],latitude=[-90,90],n_threads=35,meta_parameters=None):
        '''
        
        Retrieveing table of avialable casts for given time period and boundary
        
        Input:
        
        longitude: list of min and max logitude, i.e [-10,35]
        latitude : list of min and max latitude, i.e [50,80]
        timespan : list of min and max datetime string ['YYYY-MM-DD'] i.e ['2018-03-01','2018-09-01']
        n_threads: Number of threads to be used for retrieving each cast
        parameters:List of metadata parameters to be downloaded

        Output:
        
        Dataframe with a list of available casts with metadata
        
        '''
        
        corners=((min(latitude),min(longitude)),(max(latitude),max(longitude)))
          
        
        m=mapMath()
        
        ind=(m.latlongToGridCoordinate(corners[0][0],corners[0][1], res=1),
             m.latlongToGridCoordinate(corners[1][0],corners[1][1], res=1))
        
        boxCoords,boxIndexes=m.cornerCoordinatesToAllCoordinates(1,ind[0],ind[1])  
        
        pool = ThreadPool(n_threads)
        results=[]
        for year in range(year_start,year_end+1):
            results += pool.map(self.level2_data_retrieve,zip(boxIndexes,itertools.repeat(year),itertools.repeat(meta_parameters)))
        
        return pd.concat(results).reset_index()        
    
    def level2_data_retrieve(self,arg):
        ind,year,parameters=arg
        
        try:
            return self.sequences.data.retrieve(0,None,column_external_ids=parameters,external_id='cast_wod_2_{:d}_{:d}'.format(year,ind)).to_pandas()
        except:
            return None    
    
    def filter_casts(self,casts,longitude,latitude,timespan) :
        '''
        
        Filtering a dataframe of casts based on longitude, latitude and time
        
        Input:
        
        casts:     Dataframe containing at least cast id, longitude, latitude and time
        longitude: list of min and max logitude, i.e [-10,35]
        latitude : list of min and max latitude, i.e [50,80]
        timespan : list of min and max datetime string ['YYYY-MM-DD'] i.e ['2018-03-01','2018-09-01']
        
        Output:
        
        Dataframe of filtered cast
        
        '''
        casts['lon']=pd.to_numeric(casts['lon'])
        casts['lat']=pd.to_numeric(casts['lat'])
        casts['datetime']=pd.to_datetime(casts['date'],format='%Y%m%d')
    
        casts=casts[(casts.lat>latitude[0]) & (casts.lat<latitude[1]) &
                   (casts.lon>longitude[0]) & (casts.lon<longitude[1]) &
                   (casts.datetime>timespan[0]) & (casts.datetime<timespan[1])]
        return casts
        
    def get_available_casts(self,longitude,latitude,timespan,n_threads=35,meta_parameters=None):
        
        '''
        
        Retrieves the available casts whitin search criteria
        
        Input:
        
        longitude: list of min and max logitude, i.e [-10,35]
        latitude : list of min and max latitude, i.e [50,80]
        timespan : list of min and max datetime string ['YYYY-MM-DD'] i.e ['2018-03-01','2018-09-01']
        meta_parameters: list of column names to be returned. None returns all. i.e meta_parameters=['extId','lat','lon','date']
        
        Output:
        
        Dataframe of filtered cast
        
        '''
        
        timespan=[pd.to_datetime(timespan[0]),
                  pd.to_datetime(timespan[1])]         
        
        
        #casts=self.get_casts_from_raw_table(timespan[0].year,timespan[1].year,n_threads)
        casts=self.get_casts_from_level2(timespan[0].year,timespan[1].year,longitude,latitude,n_threads,meta_parameters)       
        
        casts=self.filter_casts(casts, longitude, latitude, timespan)
        
        return casts
    

    
    def get_casts_from_raw_table(self,year_start,year_end,n_threads=35):
        
        '''
        
        Retrieveing RAW table of avialable casts for given years
        
        Input:
        year_start : casts from this year (int)
        year_end :   casts to this year (int)
        
        Output:
        
        Dataframe with cast id, position and time
        
        '''
        
        if n_threads>1:
            pool = ThreadPool(n_threads)
            results = pool.map(self.raw_table_call,range(year_start,year_end+1))
            
        else:
            results=[]
            for year in range(year_start,year_end+1):
                results.append(self.raw_table_call(year))
                
        return pd.concat(results)        
        
            
    
    def raw_table_call(self,year):
        '''
        Retrieve RAW table for given year
        
        Teturning list of avilable casts that year.
        
        '''

        try:
            return self.raw.rows.list("WOD", "cast_{}".format(year), limit=-1).to_pandas()
        except:
            print('No data for year {}'.format(year))        
    
            
    def level3_data_retrieve(self,args):
        '''
        Download data from level_3 sequence by external_id
        
        Input: 
        args - tuple of cast_name and parameters. I.e args=('cast_wod_3_2018_82_18864723',None)
        
        '''
        
        cast_name,parameters=args
        
        try:
            _df = self.sequences.data.retrieve(start=0,end=None,external_id=cast_name,column_external_ids=parameters).to_pandas()
            _df['externalId'] = cast_name
            return _df
        except:
            
            
            print('Failed retrieveing {} parameter_filter {}'.format(cast_name,parameters))
            
    

    def download_data_from_casts(self,cast_names,n_threads=35,parameters=None):
        
        '''
        
        Rettrieving data from list of level 3 casts
        
        Input:
        cast_names - The externalId of the cast
        n_threads  - Number of threads to be used for retrieving each cast
        parameters - List of parameters to be downloaded
        
        Return:
        
        Pandas data frame with cast data 
        
        '''
        if n_threads>1:
            
            pool = ThreadPool(n_threads)
            results = pool.map(self.level3_data_retrieve, zip(cast_names,itertools.repeat(parameters)))
            
        else:
            results=[]
            for cast_name in cast_names:
                results.append(self.level3_data_retrieve((cast_name,parameters)))
                
        return pd.concat(results)
     

    def get_metadata(self,cast_names):  
        '''
        
        Returns the metadata associated with the particular cast
        
        Input:
        
        cast_names - List of cast names (externalId in ODP)
        
        '''
        return self.sequences.retrieve_multiple(external_ids=cast_names).to_pandas()
        
    
    
    
    
class mapMath:

    """
    ODP specific map tools.
    
    Get the grid index related to input long/lat and grid resolution

    Parameters 
        long = range -180 -> 180
        lat  = range  -90 -> 90
        res  = geo grid resolution where 1 x 1 degree is default, for half degree  grids use 0.5
    
     NOTE to get grid index related to geo coordinates ((-180,-90),(-179,-89)), input is in the range:
       -179 >= long > -180
       -89  >= lat  > -90
    """
    def latlongToIndex(self, lat, long, res=1):

        #Correct for boarder line degrees
        if lat == -90:
            lat = -89

        if long == -180:
            long = -179
    
        x,y = self.latlongToGridCoordinate(lat, long, res)
        return self.gridCoordinateToIndex(x,y, res)

    """
    Convert input longitude and latitude input to coordinates in the grid.
    The grid is a matrix from representing the -180,-90 to 180,90 map. With 1 as resolution the matrix has 64800 cells
    Starting with coordinates 1,1 

   Parameters 
        long = range -180 -> 180
        lat  = range  -90 -> 90
        res  = geo grid resolution where 1 x 1 degree is default, for half degree  grids use 0.5
    """
    def latlongToGridCoordinate(self, lat, long, res=1):
        lat  = lat  + 90   # Adjust from range -90 -> 90   to 0 -> 180
        long = long + 180  # Adjust from range -180 -> 180 to 0 -> 360

        # round down to index number
        roundlat  = float(res * math.ceil(lat / res ))
        roundlong = float(res * math.ceil(long / res))

        y = roundlat // res
        x = roundlong // res
        
        return int(x), int(y)

    """
    Get the index of specific grid-coordinate, given resolution

    Parameters 
        x = long, range with res = 1, 1 -> 360
        y = lat, range with res = 1, 1 -> 180
        res  = geo grid resolution where 1 x 1 degree is default, for half degree  grids use 0.5

    """
    def gridCoordinateToIndex(self, x, y, res=1): 
        return int((x-1) * 180/res + y)


    """
    Get cartesian grid coordinates, given index and resolution

    Parameters 
        index, with res = 1, 1 -> 64800
        res  = geo grid resolution where 1 x 1 degree is default, for half degree  grids use 0.5

    """
    def indexToGridCoordinate(self,index,res=1): #returns cartesian grid coordinates, given index and resolution
        lat_range  = res*180
    
        x_loc  = (index-1)//lat_range+1
        y_loc   = (index-1)%lat_range+1

        return x_loc,y_loc

    """
    Get longetude & latetude based on index

    Parameters 
        index, with res = 1, 1 -> 64800
        res  = geo grid resolution where 1 x 1 degree is default, for half degree  grids use 0.5

    """
    def indexToMapCoordinate(self,index,res=1):              
        x,y = self.indexToGridCoordinate(index,res)
        longitude = -180 + x/res 
        latitude  = -90 + y/res

        return longitude, latitude

    """
    """
    def cornerCoordinatesToAllCoordinates(self,res=1,*corners):  #tuple of tuples ->((x1,y1),(x2,y2)) or ((x1,x2))
        x1,y1 = corners[0][0],corners[0][1]
        if len(corners)==1:
            x2,y2 = corners[0][0], corners[0][1]
        else:
            x2,y2 = corners[1][0],corners[1][1]
        boxCoords  = [] #list of all coordinate tuples within the box
        boxIndexes = [] #list of all indexes within the box
        for x in range(min(x1,x2),max(x1,x2)+1):
            for y in range(min(y1,y2),max(y1,y2)+1):
                boxCoords.append((x,y))
                boxIndexes.append(self.gridCoordinateToIndex(x,y,res))
        return boxCoords,boxIndexes


    
     
