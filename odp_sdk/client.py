import time
import itertools
import logging
import json
import os

import pandas as pd
import cognite.client.data_classes as data_classes

from geomet import wkt
from datetime import datetime
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from multiprocessing.dummy import Pool as ThreadPool

from .utils.odp_geo import gcs_to_index, index_rect_members

from typing import Callable, Dict, List, Optional, Tuple, Union,Any


log = logging.getLogger("odp-sdk.log")


class ODPClient(CogniteClient):
    """
    Main entrypoint into the Ocean Data Platform SDK.
    All services are made available through this object.

    Download cast data, containing ocean measurements through the water column around the globe.

    Example::

        from odp_sdk import ODPClient

        client = ODPClient(api_key=MY_API_KEY)

        df = client.casts(longitude=[-10,35],
                          latitude=[50,80],
                          timespan=['2018-03-01','2018-09-01'])
    """

    def __init__(
            self,
            api_key: str = None,
            project: str = 'odp',
            client_name: str = 'ODPPythonSDK',
            base_url: str = None,
            max_workers: int = None,
            headers: Dict[str, str] = None,
            timeout: int = None,
            token: Union[str, Callable[[], str], None] = None,
            disable_pypi_version_check: Optional[bool] = None,
            debug: bool = False,
            info_odp: bool = True
        
    ):
        """Constructor. ODP client inherits all properties and functions from CogniteClient

        Args:
            api_key: API key
            project: Project. Defaults to project of given API key.
            client_name: A user-defined name for the client. Used to identify number of unique applications/scripts
                running on top of CDF.
            base_url: Base url to send requests to. Defaults to "https://api.cognitedata.com"
            max_workers: Max number of workers to spawn when parallelizing data fetching. Defaults to 10.
            headers: Additional headers to add to all requests.
            timeout: Timeout on requests sent to the api. Defaults to 30 seconds.
            token: A jwt or method which takes no arguments and returns a jwt to use for authentication.
                This will override any api-key set.
            disable_pypi_version_check: Don't check for newer versions of the SDK on client creation
            debug: Configures Cognite logger to log extra request details to stderr.
            info_odp: Logger info for odp-sdk
        """
        self.MAX_THREADS = 50
        
        super().__init__(api_key, project, client_name, base_url, max_workers,
                         headers, timeout, token, disable_pypi_version_check, debug)
        if info_odp:
            log.setLevel(logging.INFO)
            logging.basicConfig(level=logging.INFO)
        
        login_status = self.login.status()
        if not login_status.logged_in:
            raise ConnectionError("Failed to connect to ODP")
        else:
            log.info('Connected')
            
        log.info(f"Logged in to '{login_status.project}' as user '{login_status.user}'")        
       
    def files_search(self,
            file_name: str=None,
            longitude: Tuple[float, float] = None,
            latitude: Tuple[float, float] = None,
            timespan: Tuple[str, str] = None,
            data_source: str = None,
            search_polygon = None,
            search_metadata: Dict[str, Any]=None,
            data_set_ids: List[Dict[str,Any]] = None,
            limit: int = -1
            ) -> Optional[pd.DataFrame]:
        '''
        Search for files in the Ocean Data Platform
        
        Args:
            file_name: The name of the file if you have that, i.e FAO.zip
            longitude: list of min and max longitude, i.e [-10,35]
            latitude : list of min and max latitude, i.e [50,80]
            timespan : list of min and max datetime string ['YYYY-MM-DD'] i.e ['2018-03-01','2018-09-01']
            data_source: the source of the data, i.e NOAA
            search_polygon: Search polygon overwriting longitude and latitude bounding box. i.e [[[lon0,lat0],lon1,lat1],...,[lon0,lat0]]]
            search_metadata: Dictionary search on metadata. i.e {'year':'2017'}
            data_set_ids: Search only in certain datasets. i.e [123124124,564929034]
            limit: Limit on the number of search results
        
        Returns:
            Pandas dataframe with search results. Download of files to performed with files_download()
    
        
        '''
        
        search_area=None
        if (longitude is not None) and (latitude is not None):
            search_area=[[[longitude[0],latitude[0]],[longitude[0],latitude[1]],[longitude[1],latitude[1]],
                         [longitude[1],latitude[0]],[longitude[0],latitude[0]]]]
        elif type(search_polygon)==list:
            search_area=search_polygon
            search_area_type='Polygon'
            
        elif search_polygon is not None:
            ls = search_polygon.to_wkt()
            ls_json = wkt.loads(ls)
            search_area=ls_json['coordinates'] 
            search_area_type=search_polygon.type
            
            
        if timespan is not None:
            timespan= {"min": int(datetime.strptime(timespan[0], '%Y-%m-%d').timestamp() * 1000),
                       "max": int(datetime.strptime(timespan[1], '%Y-%m-%d').timestamp() * 1000)}
            
            
        if search_area is not None:
            geo_filter=data_classes.files.GeoLocationFilter('within',data_classes.files.GeometryFilter(search_area_type, search_area))
        else:
            geo_filter=None
        
       
        res = self.files.list(name=file_name,
                                geo_location=geo_filter,
                                metadata=search_metadata,
                                source=data_source,
                                data_set_ids=data_set_ids,
                                source_created_time=timespan,
                                limit=limit).to_pandas()                   
        
        if not res.empty:
            
            if "geoLocation" in res.keys():
                res['geometry']=res.geoLocation.apply(lambda x: x.geometry['coordinates'])
            if "sourceCreatedTime" in res.keys():
                res['datetime']=res.sourceCreatedTime.apply(lambda x: datetime.fromtimestamp(x / 1e3))  
        
        if len(res)==limit:
            log.warning('Limit on number of files returned is reached, only {} files are returned. '
                        'Try to apply more filters to reduce number of files in search or increase limit'.format(limit))
        
        return res
        
    
    
    def files_download(self,
                       ids: List[int], 
                       directory: str
                       )-> None:
        
        '''
        Download selected files to local directory
        
        Args:
            ids: List of file ids to be downloaded
            directory: local directory for placing the files
        Returns:
            None
        '''
        
        self.files.download(directory=directory, id=ids) 
    

    def casts(
            self,
            longitude: Tuple[float, float] = (-180., 180.),
            latitude: Tuple[float, float] = (-90., 90.),
            timespan: Tuple[str, str] = ('1700-01-01', '2050-01-01'),
            n_threads: int = 35,
            include_flagged_data: bool = True,
            parameters: List[str] = None
    ) -> Optional[pd.DataFrame]:
        """Download cast data within search criteria

        Args:
            longitude: list of min and max longitude, i.e [-10,35]
            latitude : list of min and max latitude, i.e [50,80]
            timespan : list of min and max datetime string ['YYYY-MM-DD'] i.e ['2018-03-01','2018-09-01']
            n_threads: Number of threads to use
            include_flagged_data : Boolean, whether flagged data that is flagged should be included or not
            parameters: List of parameters to be included in DataFrame.
                If None all column are included. I.e. parameters=['date','lon','lat','Temperature','Oxygen']

        Returns:
            Pandas DataFrame with cast data
        """

        if n_threads > self.MAX_THREADS:
            log.warning('Maximum allowable number of threads is {}'.format(self.MAX_THREADS))
            n_threads = self.MAX_THREADS
        
        t0 = time.time()
        log.info('Locating available casts..')
        
        casts = self.get_available_casts(longitude, latitude, timespan, n_threads,
                                         meta_parameters=['extId', 'lat', 'lon', 'date'])

        cast_names_filtered = casts['extId'].tolist()
        log.info('-> {} casts found'.format(len(cast_names_filtered)))

        if not cast_names_filtered:
            log.warning('No casts found in search')
            return None  
        
        # Including flag columns to remove flagged data points
        if not include_flagged_data and parameters is not None:
            parameters_with_flags = ['z', 'Oxygen', 'Temperature', 'Salinity', 'Chlorophyll', 'Nitrate', 'pH']
            parameters_org = parameters.copy()
            for p in parameters_org:
                if p in parameters_with_flags:
                    parameters += [p + '_WODflag']
        else:
            parameters_org = []
                
        log.info('Downloading data from casts..')
        data = self.download_data_from_casts(cast_names_filtered, n_threads, parameters)
        
        if data.empty:
            log.warning('No available data found in casts')
            return None

        # Setting flagged measurements to None if not include_flagged_data
        if not include_flagged_data:
            for var in data.columns:
                if var + '_WODflag' in data.columns:
                    mask = data[var + '_WODflag'] != 0
                    data.loc[mask, var] = None
            if parameters is not None:
                data = data[['externalId', 'datetime'] + parameters_org]
        
        log.info('-> {} data rows downloaded in {:.2f}s'.format(len(data), time.time()-t0))

        return data

    def filter_casts(
            self,
            casts: pd.DataFrame,
            longitude: Tuple[int, int],
            latitude: Tuple[int, int],
            timespan: Tuple[pd.Timestamp,pd.Timestamp]
    ) -> Optional[pd.DataFrame]:
        """Filtering a DataFrame of casts based on longitude, latitude and time

        Args:
            casts: DataFrame containing at least cast id, longitude, latitude and time
            longitude: Tuple of min and max longitude, i.e (-10,35)
            latitude: Tuple of min and max latitude, i.e (50,80)
            timespan: Tuple of min and max pd.Timestamp 

        Returns:
            DataFrame of filtered cast
        """

        casts = casts[(casts.lat > latitude[0]) & (casts.lat < latitude[1]) &
                      (casts.lon > longitude[0]) & (casts.lon < longitude[1]) &
                      (casts.datetime > timespan[0]) & (casts.datetime < timespan[1])]
        return casts
        
    def get_available_casts(
            self,
            longitude: Tuple[float, float],
            latitude: Tuple[float, float],
            timespan: Tuple[str, str],
            n_threads: int = 35,
            meta_parameters: List[str] = None
    ) -> pd.DataFrame:
        """Retrieves the available casts within search criteria

        Args:
            longitude: Tuple of min and max longitude, i.e (-10.11,35.33)
            latitude: Tuple of min and max latitude, i.e (50,80)
            timespan: Tuple of min and max datetime string ['YYYY-MM-DD'] i.e ('2018-03-01','2018-09-01')
            n_threads:
            meta_parameters: List of column names to be returned.
                None returns all. i.e meta_parameters=['extId','lat','lon','date', 'country', 'equpment', 'Platform']

        Returns:
            DataFrame of filtered cast
        """

        timespan = (pd.to_datetime(timespan[0]), pd.to_datetime(timespan[1]))
        
        casts = self._get_casts_from_level2(timespan, longitude, latitude,
                                            n_threads, meta_parameters)
        
        casts = self.filter_casts(casts, longitude, latitude, timespan)
        
        return casts

    def download_data_from_casts(
            self,
            cast_names: List[str],
            n_threads: int = 35,
            parameters: List[str] = None
    ) -> pd.DataFrame:
        """Retrieving data from list of level 3 casts

        Args:
            cast_names: The externalId of the cast ('extId')
            n_threads: Number of threads to be used for retrieving each cast
            parameters: List of parameters to be downloaded
                If None all column are included. I.e. parameters=['date','lon','lat','Temperature','Oxygen']

        Returns:
            Pandas data frame with cast data
        """

        if n_threads > 1:
            
            pool = ThreadPool(n_threads)
            results = pool.starmap(self._level3_data_retrieve, zip(cast_names, itertools.repeat(parameters)))
            
        else:
            results = []
            for cast_name in cast_names:
                results.append(self._level3_data_retrieve(cast_name, parameters))
                
        return pd.concat(results)

    def get_metadata(self, cast_names: List[str]) -> Union[None, pd.DataFrame]:
        """Returns the metadata associated with the particular cast

        Args:
            cast_names: List of cast names (externalId in ODP)

        Returns:
            DataFrame of casts with metadata
        """

        return self.sequences.retrieve_multiple(external_ids=cast_names).to_pandas()

    def _get_casts_from_level2(
            self,
            timespan: Tuple[pd.Timestamp, pd.Timestamp],
            longitude: Tuple[float, float],
            latitude: Tuple[float, float],
            n_threads: int = 35,
            meta_parameters: List[str] = None
    ) -> pd.DataFrame:
        """Retrieving table of available casts for given time period and boundary

        Args:
            timespan: Tuple of to and from timestamps
            longitude: Tuple of min and max logitude, i.e [-10.11,35.33]
            latitude : Tuple of min and max latitude, i.e [50,80]
            n_threads: Number of threads to be used for retrieving each cast
            meta_parameters: List of metadata parameters to be downloaded

        Returns:
            DataFrame with a list of available casts with metadata
        """

        lat = [min(latitude), max(latitude)]
        lon = [min(longitude), max(longitude)]

        i1, i2 = gcs_to_index(lat, lon)

        box_indices = index_rect_members(i1, i2)

        pool = ThreadPool(n_threads)
        results = []

        for year in range(timespan[0].year, timespan[1].year + 1):
            results += pool.starmap(self._level2_data_retrieve, zip(
                itertools.repeat(year),
                box_indices,
                itertools.repeat(meta_parameters)
            ))

        return pd.concat(results).reset_index()

    def _level2_data_retrieve(
            self,
            year: int,
            ind: int,
            parameters: List[str]
    ) -> Union[None, pd.DataFrame]:
                              
        try:
            casts = self.sequences.data.retrieve(
                external_id='cast_wod_2_{:d}_{:d}'.format(year, ind),
                column_external_ids=parameters,
                start=0,
                end=None)

            if casts is None:
                return None            
            casts=casts.to_pandas()
            casts.lon = pd.to_numeric(casts.lon)
            casts.lat = pd.to_numeric(casts.lat)
            casts['datetime'] = pd.to_datetime(casts.date, format='%Y%m%d')

            return casts
        except:
            return None

    def _level3_data_retrieve(self, cast_name: str, parameters: List[str]) -> Union[None, pd.DataFrame]:
        """Download data from level_3 sequence by external_id

        Args:
            cast_name: The external ID of the cast o level 3, i.e  'cast_wod_3_2018_82_18864723' 
            parameters: List of columns to retrieve. None if all.

        Returns:
            DataFrame of retrieved sequences
        """

        try:
            seqs = self.sequences.data.retrieve(external_id=cast_name,
                                                column_external_ids=parameters, start=0, end=None)
            if seqs is None:
                return None
            df = seqs.to_pandas()
            df["externalId"] = cast_name
            df['datetime'] = pd.to_datetime(df.date, format='%Y%m%d') 
            return df
        except CogniteAPIError as e:
            log.error(f"Failed to retrieve cast '{cast_name}' with parameters {parameters}: {e.message}")
