
import time
import itertools
import logging

import pandas as pd
from cognite.client import CogniteClient
from cognite.client.exceptions import CogniteAPIError
from multiprocessing.dummy import Pool as ThreadPool

from utils.odp_geo import gcs_to_index, index_rect_members

from typing import Callable, Dict, List, Optional, Tuple, Union

log = logging.getLogger("odp-sdk")


class ODPClient(CogniteClient):
    """
    Main entrypoint into the Ocean Data Platform SDK.
    All services are made available through this object.

    Download cast data, containing ocean measurements through the water column around the globe.

    Example:

        from client import ODPClient

        client = ODPClient(api_key=MY_API_KEY)

        df=client.casts(longitude=[-10,35],
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
            debug: Configures logger to log extra request details to stderr.
        """
        self.MAX_THREADS=50
        
        
        super().__init__(api_key, project, client_name, base_url, max_workers, headers, timeout, token, disable_pypi_version_check, debug)
        
        login_status = self.login.status()
        if not login_status.logged_in:
            raise ConnectionError("Failed to connect to ODP")
        else:
            print('Connected')
            
        log.info(f"Logged in to '{login_status.project}' as use '{login_status.user}'")        
        
    def casts(
            self,
            longitude: Tuple[int, int] = (-180, 180),
            latitude: Tuple[int, int] = (-90, 90),
            timespan: Tuple[str, str] = ('1700-01-01', '2050-01-01'),
            n_threads: int = 35,
            include_flagged_data: bool = True,
            parameters: List[str] = None
    ) -> Union[None, pd.DataFrame]:
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
            print('Maximum allowable number of threads is {}'.format(self.MAX_THREADS))
            n_threads = self.MAX_THREADS
        
        t0 = time.time()
        print('Locating available casts..')
        
        casts = self.get_available_casts(longitude, latitude, timespan, n_threads,
                                         meta_parameters=['extId', 'lat', 'lon', 'date'])

        cast_names_filtered = casts['extId'].tolist()
        print('-> {} casts found'.format(len(cast_names_filtered)))        

        if not cast_names_filtered:
            print('No casts found in search')
            return None  
        
        # Including flag columns to remove flagged data points
        if not include_flagged_data and (parameters is not None):
            parameters_with_flags = ['z', 'Oxygen', 'Temperature', 'Salinity', 'Chlorophyll', 'Nitrate', 'pH']
            parameters_org = parameters.copy()
            for p in parameters_org:
                if p in parameters_with_flags:
                    parameters += [p + '_WODflag']
        else:
            parameters_org = []
                
        print('Downloading data from casts..')
        data = self.download_data_from_casts(cast_names_filtered, n_threads, parameters)
        
        if data.empty:
            print('No available data found in casts')
            return None

        # Adding a column with datetime
        data['datetime'] = pd.to_datetime(data['date'], format='%Y%m%d')
        
        # Setting flagged measurements to None if not include_flagged_data
        if not include_flagged_data:
            for var in data.columns:
                if var + '_WODflag' in data.columns:
                    mask = data[var + '_WODflag'] != 0
                    data.loc[mask, var] = None
            if parameters is not None:
                data = data[['externalId', 'datetime'] + parameters_org]
        
        print('-> {} data rows downloaded in {:.2f}s'.format(len(data), time.time()-t0))

        return data
            
    def _get_casts_from_level2(
            self,
            timespan: Tuple[str,str]= ('1700-01-01', '2050-01-01'),
            longitude: Tuple[int, int] = (-180, 180),
            latitude: Tuple[int, int] = (-90, 90),
            n_threads: int = 35,
            meta_parameters: List[str] = None
    ) -> pd.DataFrame:
        """Retrieving table of available casts for given time period and boundary

        Args:
            year_start: Timeframe start year
            year_end: Timeframe last year
            longitude: Tuple of min and max logitude, i.e [-10,35]
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

        for year in range(timespan[0].year, timespan[1].year+1):
            results += pool.starmap(self._level2_data_retrieve, zip(
                itertools.repeat(year),
                box_indices,
                itertools.repeat(meta_parameters)
            ))
        
        return pd.concat(results).reset_index()        

    def filter_casts(
            self,
            casts: pd.DataFrame,
            longitude: Tuple[int, int],
            latitude: Tuple[int, int],
            timespan: Tuple[str, str]
    ) -> Union[None, pd.DataFrame]:
        """Filtering a DataFrame of casts based on longitude, latitude and time

        Args:
            casts: DataFrame containing at least cast id, longitude, latitude and time
            longitude: Tuple of min and max longitude, i.e (-10,35)
            latitude: Tuple of min and max latitude, i.e (50,80)
            timespan: list of min and max datetime string ['YYYY-MM-DD'] i.e ('2018-03-01','2018-09-01')

        Returns:
            DataFrame of filtered cast
        """


    
        casts = casts[(casts.lat > latitude[0]) & (casts.lat < latitude[1]) &
                      (casts.lon > longitude[0]) & (casts.lon < longitude[1]) &
                      (casts.datetime > timespan[0]) & (casts.datetime < timespan[1])]
        return casts
        
    def get_available_casts(
            self,
            longitude: Tuple[int, int],
            latitude: Tuple[int, int],
            timespan: Tuple[str, str],
            n_threads: int = 35,
            meta_parameters: List[str] = None
    ) -> pd.DataFrame:
        """Retrieves the available casts within search criteria

        Args:
            longitude: Tuple of min and max longitude, i.e (-10,35)
            latitude: Tuple of min and max latitude, i.e (50,80)
            timespan: Tuple of min and max datetime string ['YYYY-MM-DD'] i.e ('2018-03-01','2018-09-01')
            n_threads:
            meta_parameters: List of column names to be returned.
                None returns all. i.e meta_parameters=['extId','lat','lon','date']

        Returns:
            DataFrame of filtered cast
        """

        timespan = (pd.to_datetime(timespan[0]), pd.to_datetime(timespan[1]))
        
        casts = self._get_casts_from_level2(timespan, longitude, latitude,
                                           n_threads, meta_parameters)
        
        casts = self.filter_casts(casts, longitude, latitude, timespan)
        
        return casts

    def get_casts_from_raw_table(
            self,
            year_start: int,
            year_end: int,
            n_threads: int = 35
    ) -> pd.DataFrame:
        """Retrieving RAW table of available casts for given years

        Args:
            year_start: casts from this year
            year_end: casts to this year
            n_threads:

        Returns:
            DataFrame with cast id, position and time
        """

        if n_threads > 1:
            pool = ThreadPool(n_threads)
            results = pool.map(self.raw_table_call, range(year_start, year_end + 1))
            
        else:
            results = []
            for year in range(year_start, year_end + 1):
                results.append(self.raw_table_call(year))
                
        return pd.concat(results)        

    def raw_table_call(self, year: int) -> Union[None, pd.DataFrame]:
        """Retrieve RAW table for given year

        Args:
            year: Year to retrieve casts for

        Returns:
            List of available casts that year.
        """

        try:
            return self.raw.rows.list("WOD", "cast_{}".format(year), limit=-1).to_pandas()
        except:
            print('No data for year {}'.format(year))        

    def download_data_from_casts(
            self,
            cast_names: List[str],
            n_threads: int = 35,
            parameters: List[str] = None) -> pd.DataFrame :
        """Rettrieving data from list of level 3 casts

        Args:
            cast_names: The externalId of the cast
            n_threads: Number of threads to be used for retrieving each cast
            parameters: List of parameters to be downloaded

        Returns:
            Pandas data frame with cast data
        """

        if n_threads > 1:
            
            pool = ThreadPool(n_threads)
            results = pool.starmap(self._level3_data_retrieve, zip(cast_names, itertools.repeat(parameters)))
            
        else:
            results = []
            for cast_name in cast_names:
                results.append(self._level3_data_retrieve((cast_name, parameters)))
                
        return pd.concat(results)

    def get_metadata(self, cast_names: List[str]) -> Union[None, pd.DataFrame]:
        """Returns the metadata associated with the particular cast

        Args:
            cast_names: List of cast names (externalId in ODP)

        Returns:
            DataFrame of casts
        """

        return self.sequences.retrieve_multiple(external_ids=cast_names).to_pandas()

    def _level2_data_retrieve(self,
                              year: int,
                              ind: int ,
                              parameters: List[str] ) -> Union[None, pd.DataFrame]     :                  
                              
        try:
            casts=self.sequences.data.retrieve(
                external_id='cast_wod_2_{:d}_{:d}'.format(year, ind),
                column_external_ids=parameters,
                start=0,
                end=None
            ).to_pandas()
            casts.lon = pd.to_numeric(casts.lon)
            casts.lat = pd.to_numeric(casts.lat)
            casts['datetime'] = pd.to_datetime(casts.date, format='%Y%m%d')            
        except:
            return None

    def _level3_data_retrieve(self, cast_name : str, parameters : List[str]) -> pd.DataFrame:
        """Download data from level_3 sequence by external_id

        Args:
            cast_name: The external ID of the cast o level 3, i.e  'cast_wod_3_2018_82_18864723' 
            parameters: List of columns to retrieve. None if all.

        Returns:
            DataFrame of retrieved sequences
        """

        try:
            seqs = self.sequences.data.retrieve(external_id=cast_name, column_external_ids=parameters, start=0, end=None)
            if seqs is None:
                return None
            df = seqs.to_pandas()
            df["externalId"] = cast_name
            return df
        except CogniteAPIError as e:
            log.error(f"Failed to retrieve cast '{cast_name}' with parameters {parameters}: {e.message}")
