
import time
import pandas as pd
from cognite.client import CogniteClient
from multiprocessing.dummy import Pool as ThreadPool
from mapMath import mapMath


class ODPClient(CogniteClient):
    
    '''
    
    Main entrypoint into the Ocean Data Platform SDK. 
    All services are made available through this object.
    
    Example:
    
    from client import ODPClient
    
    client = ODPClient(api_key='....................',
                       project="odp", client_name="odp")
                       
    df=client.casts(longitude=[-10,35],
                    latitude=[50,80],
                    timespan=['2018-03-01','2018-09-01'],
                    depth=[0,100]) 
    
    '''
    def __init__(self, api_key=None, project='odp', client_name='ODPPythonSDK', base_url=None, max_workers=None, headers=None, timeout=None, token=None, disable_pypi_version_check=None, debug=False):


        super().__init__(api_key, project, client_name, base_url, max_workers, headers, timeout, token, disable_pypi_version_check, debug)   

        self.m=mapMath()
        
    def casts(self,longitude=[-180,180],latitude=[-90,90],timespan=['1700-01-01','2050-01-01'],depth=[0,1e9],run_threaded=True):
        
        '''
        
        Download cast data within search criteria
        
        Input:
        
        longitude: list of min and max logitude, i.e [-10,35]
        latitude : list of min and max latitude, i.e [50,80]
        timespan : list of min and max datetime string ['YYY-MM-DD'] i.e ['2018-03-01','2018-09-01']
        depth    : list of min and max depth in meters i.e [0,100]
        
        Return:
        
        Pandas dataframe with cast data

        
        '''
        
        resolution=1# Resolution of grid
        
        t0=time.time()
        print('Locating available casts..')
        
        cast_indexes=self.level1_call(longitude,latitude)
        
        cast_names_filtered=self.cast_query(longitude,latitude,timespan,depth,cast_indexes,resolution,run_threaded)
        
        if cast_names_filtered==[]:
            print('No casts found in search')
            return None   
        
        print('Downloading data from casts..')
        data=self.get_data(cast_names_filtered,run_threaded)
        
        if data.empty:
            print('No data found in casts')
            return None
        
        data=self.filter_dataframe(data,longitude,latitude,timespan,depth)
        
        print('Data retrieval completed in {:.2f}s'.format(time.time()-t0))
        return data
        
        
    def level1_call(self,longitude,latitude):
        '''
        
        Retrieves grid indexes of non-empty grid points within the specified range of 
        latitude and longitude
        
        Input:
        longitude : min and max of longitude [-10,30]
        latitude : min and max of latitude [40,6]
        
        Output:
        List of cast indexes
        
        '''
        
        df=self.sequences.data.retrieve(start=0, end=None, limit=100000, external_id='cast_count_map_2').to_pandas()
        df=df[(df['long']>=longitude[0]) & (df['long']<=longitude[1]) &
                (df['lat']>=latitude[0]) & (df['lat']<=latitude[1]) ]
        
        ind=df.apply(lambda x: self.m.latlongToIndex(x.lat, x.long,res=1), axis=1).values
        
        return pd.unique(ind)
    
    def cast_query(self,longitude,latitude,timespan,depth,cast_indexes,resolution=1,run_threaded=True):
        
        '''
        
        Locate the level_3 casts containing data within search criteria
        
        '''
        
        timespan=[pd.to_datetime(timespan[0]).value //10**6,
                  pd.to_datetime(timespan[1]).value //10**6]        
        
        
        if run_threaded == True:
            
            pool = ThreadPool(50)
            results = pool.map(self.level2_call, cast_indexes)
            
        else:
            
            results=[]
            for ind in cast_indexes:
                results.append(self.level2_call(ind))
            

        casts=[]                                                             
        for _cast in results:
            if _cast != []:
                sequence_lvl3=_cast.to_pandas()
                ind_within=sequence_lvl3['metadata'].apply(lambda x: self.check_if_sequence_is_within_query(x,longitude,latitude,timespan,depth)).values
                
                if not True in ind_within:
                    continue
                
                casts+= sequence_lvl3['name'].iloc[ind_within].to_list()                  
                

        return casts

    def level2_call(self,ind):

        return self.sequences.list(external_id_prefix='casts_wod_{}'.format(ind), limit=None)#.to_pandas()
            
    
    def get_data(self,cast_names,run_threaded=True):
        
        '''
        
        Rettrieving data from list of level 3 casts
        
        Return:
        
        Pandas data frame with cast data 
        
        '''
        if run_threaded == True:
            
            pool = ThreadPool(50)
            results = pool.map(self.level3_call, cast_names)
            
        else:
            results=[]
            for cast_name in cast_names:
                results.append(self.level3_call(cast_name))
                
        
        for index, cast in enumerate(results):
            
            _df=cast.to_pandas()
            
            cols=['temperature', 'oxygen', 'salinity','chlorophyll','pressure','nitrate','pH']           
            _df[cols] = _df[cols].apply(pd.to_numeric, errors='coerce', axis=1)            
            
            if index==0:
                df=_df
            else:
                df=pd.concat([df,_df])
                
        return df          
 
    def level3_call(self,cast_name):
        
        return self.sequences.data.retrieve(start=0,end=None,external_id=cast_name)  
    
    def filter_dataframe(self,df,longitude,latitude,timespan,depth):
        
        '''
        
        Trimming dataframe with cast data to only contain data within search criteria
        
        '''
        
        df['datetime']=pd.to_datetime(df['date'], unit='ms',origin='1970-01-01 00:00:00')
        

        df=df[(df['longitude']>=longitude[0]) & (df['longitude']<=longitude[1]) &
                (df['latitude']>=latitude[0]) & (df['latitude']<=latitude[1]) &
                (df['datetime']>=timespan[0]) & (df['datetime']<=timespan[1]) &
                (df['depth']>=depth[0]) & (df['depth']<=depth[1])]
        
        return df
    
    def check_if_sequence_is_within_query(self,seq,longitude,latitude,timespan,depth):
        
        '''
        
        Cheks if sequence is within search criteria
        
        '''
        
        param_range=timespan
        param_name='timestamp'
        if (param_range[0]<=float(seq['{}_start'.format(param_name)])) and (param_range[1]<=float(seq['{}_start'.format(param_name)])):
            return False
        elif (param_range[0]>=float(seq['{}_end'.format(param_name)])) and (param_range[1]>=float(seq['{}_end'.format(param_name)])):
            return False        
        
        param_names=['longitude','latitude','depth']
        param_ranges=[longitude,latitude,depth]
        
        for param_range,param_name in zip(param_ranges,param_names):
            if (param_range[0]<=float(seq['{}_min'.format(param_name)])) and (param_range[1]<=float(seq['{}_min'.format(param_name)])):
                return False
            elif (param_range[0]>=float(seq['{}_max'.format(param_name)])) and (param_range[1]>=float(seq['{}_max'.format(param_name)])):
                return False
                     
        
        return True    
            



                
            
            
        
            
    

        
        

    

        