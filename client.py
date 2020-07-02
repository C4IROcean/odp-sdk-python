
import time
import pandas as pd
from cognite.client import CogniteClient
from multiprocessing.dummy import Pool as ThreadPool


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
        '''
        
        Constructor. ODP client inherits all properties and functions from CogniteClient
        
        '''

        super().__init__(api_key, project, client_name, base_url, max_workers, headers, timeout, token, disable_pypi_version_check, debug)   

       
        
    def casts(self,longitude=[-180,180],latitude=[-90,90],timespan=['1700-01-01','2050-01-01'],n_threads=1):
        
        '''
        
        Download cast data within search criteria
        
        Input:
        
        longitude: list of min and max logitude, i.e [-10,35]
        latitude : list of min and max latitude, i.e [50,80]
        timespan : list of min and max datetime string ['YYYY-MM-DD'] i.e ['2018-03-01','2018-09-01']
        
        Return:
        
        Pandas dataframe with cast data

        
        '''
        
        # Time string to datetome          
        
        t0=time.time()
        print('Locating available casts..')
        casts=self.get_filtered_casts(longitude, latitude, timespan)
        cast_names_filtered=casts['extId'].tolist()
        print('-> {} casts found'.format(len(cast_names_filtered)))        
        
        
        if cast_names_filtered==[]:
            print('No casts found in search')
            return None   
        
        print('Downloading data from casts..')
        data=self.download_data_from_casts(cast_names_filtered,n_threads)
        
        if data.empty:
            print('No data found in casts')
            return None
        
        data['datetime']=pd.to_datetime(data['date'],format='%Y%m%d') #Adding a column with datetime
        
        print('-> {} data rows downloaded in {:.2f}s'.format(len(data),time.time()-t0))
        
        return data
    
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
                   (casts.datetime>timespan[0]) & (casts.datetime<timespan[1])]#.values  
        return casts
        
    def get_filtered_casts(self,longitude,latitude,timespan):
        
        '''
        
        Retrieves the available casts whitin search criteria
        
        Input:
        
        longitude: list of min and max logitude, i.e [-10,35]
        latitude : list of min and max latitude, i.e [50,80]
        timespan : list of min and max datetime string ['YYYY-MM-DD'] i.e ['2018-03-01','2018-09-01']
        
        Output:
        
        Dataframe of filtered cast
        
        '''
        
        timespan=[pd.to_datetime(timespan[0]),
                  pd.to_datetime(timespan[1])]         
        
        casts=self.get_available_casts(timespan[0].year,timespan[1].year)
        filtered_casts=self.filter_casts(casts, longitude, latitude, timespan)
        
              
        return filtered_casts
    

    
    def get_available_casts(self,year_start,year_end):
        
        '''
        
        Retrieveing RAW table of avialable casts for given years
        
        Input:
        year_start : casts from this year (int)
        year_end :   casts to this year (int)
        
        Output:
        
        Dataframe with cast id, position and time
        
        '''
        
        
        
        flag=0
        for yr in range(year_start,year_end+1):
            try:
                _df=self.raw_table_call(yr).to_pandas()
            except:
                print('No RAW table for year {}'.format(yr))
                continue
            
            if flag==0:
                df=_df
                flag=1
            else:
                df=pd.concat((df,_df))
        
        return df
            
    
    def raw_table_call(self,year):
        '''
        Retrieve RAW table for given year
        '''
        
        return self.raw.rows.list("WOD", "cast_{}".format(year), limit=-1)#.to_pandas()
    
            
    def level3_data_retrieve(self,cast_name):
        '''
        Download data from level_3 sequence by external_id
        '''
        try:
            return self.sequences.data.retrieve(start=0,end=None,external_id=cast_name).to_pandas()  
        except:
            print('Failed retrieveing {}'.format(cast_name))
            
    
    def download_data_from_casts(self,cast_names,n_threads=1):
        
        '''
        
        Rettrieving data from list of level 3 casts
        
        Return:
        
        Pandas data frame with cast data 
        
        '''
        if n_threads>1:
            
            pool = ThreadPool(n_threads)
            results = pool.map(self.level3_data_retrieve, cast_names)
            
        else:
            results=[]
            for cast_name in cast_names:
                #print(cast_name)
                results.append(self.level3_data_retrieve(cast_name))
                
        return pd.concat(results)
     




                
            
            
        
            
    

        
        

    

        