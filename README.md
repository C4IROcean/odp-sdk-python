# Python SDK for Ocean Data Platform (ODP)

Connect to the Ocean Data Platform with Python through the Python SDK. Download queried ocean data easily and efficiently into data frames, for easy exploring and further processing in your data science project.

This is an extensions package to the Cognite Python SDK.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the Ocean Data Platform Python SDK.

```bash
pip3 install odp_sdk
```
*Note: Utility functions available in CastFunctions.py are not included in the pip install package and has to be downloaded separately*


## Usage

*Note: Accessing the Ocean Data Platform requires a personal api-key. Contact ODP to require one.*

Import the Ocean Data Platform SDK 
```python
from odp_sdk import ODPClient
```
Connect to the platform
```python
client = ODPClient(api_key='your_personal_api_key_from_ODP')
```
Get dataframe of ocean data samples (casts) within search criteria

Basic usage:
```python
df=client.casts(longitude=[-10,35],
                latitude=[50,80],
                timespan=['2019-06-01','2019-06-07']) 
```

Basic usage with more options (Specifying the parameters of interest and removing flagged data):
```python
df=client.casts(longitude=[-25,35],
                latitude=[50,80],
                timespan=['2018-06-01','2018-08-31'],
                parameters=['date','lon','lat','z','Temperature','Oxygen','Salinity'],
                n_threads=35,
                include_flagged_data=False)
```


CastFunctions.py include useful features for interpolating and plotting. This package is not a part of the odp_sdk package

DataStatsfunctions.py include functions for plotting aspects of  data coverage, distributions, cast breakdowns. This package is not a part of the odp_sdk package. 

## Jupyter Notebook Examples 
Example notebooks are found in the Example folder, which includes:
- Download data, plot the casts and create a gridded map of surface temperatures 
- Download avilable casts, explore them in an interactive map and plot the cast profile
- Example Notebook Data Coverage shows how functions from DataStatsFunctions.py can help users understand the data they download from the odp_sdk. 

## WOD
The World Ocean Database is a National Centers for Environmental (NCEI) product and International Oceanographic Data and Information Exchange (IODE) project which provides a composite of publicly available ocean profile data, both historic and recent. It consists of over thousands of datasets consisting of millions of water temperatures, salinity, oxygen, and nutrient profiles (1,2).

## Data Organization in WOD and definitions

Cast: A set of one or more profiles taken concurrently or nearly concurrently. All casts from similar instruments with similar resolutions are grouped together. For example, all bathythermograph (BT) data are all part of the same data set (MBT), see below. 

Profile: A set of measurements for a single variable (i.e. temperature salinity) along a specific path, which could be vertically in the water column, horizontally along the surface, or discrete areas based on placement of buoys. 

## Data Description
The data available through the Ocean Data Platform are oceanographic measurements of physical and chemical ocean parameters (temperature, salinity, oxygen, nitrate, ph and chlorophyll), and is based on the data available through NOAA's World Ocean Database. Each cast has a sepcified latitude, longitude and time (lat, lon and datetime), and a depth profile, where each depth has measured physical and chemical parameters. Not all casts have all the the different ocean parameters, missing measurements are populated with nans. Each measurements has a WODflag parameter (i.e Nitrate_WODflag). If flag value is zero, there are no known issues with the measured value. 



Parameter|	      Unit
--- | --- 
lon/lat |        deg
Z|               m             
Oxygen|	        µmol/kg 
Temperature|	    C
Salinity|	      1e-3
Nitrate|	        µmol/kg
Chlorophyll|    µgram/l
Pressure|dbar

Dataset code | Dataset include
--- | --- 
OSD| Ocean Station Data, Low-resolution CTD/XCTD, Plankton data
CTD| High-resolution Conductivity-Temperature-Depth / XCTD data
MBT| Mechanical / Digital / Micro Bathythermograph data
XBT| Expendable Bathythermograph data
SUR| Surface-only data
APB| Autonomous Pinniped data
MRB| Moored buoy data
PFL| Profiling float data
DRB| Drifting buoy data
UOR| Undulating Oceanographic Recorder data
GLD| Glider data

See [WORLD OCEAN DATABASE 2018 User’s Manual](https://rda.ucar.edu/datasets/ds285.0/docs/WOD18-UsersManual_final.pdf) for more information about the datasets

References:
1.	Boyer, T.P., O.K. Baranova, C. Coleman, H. E., Garcia, A. Grodsky, R.A. Locarnini, A. V., Mishonov, C.R. Paver, J.R. Reagan, D. S. & I.V. Smolyar, K.W. Weathers,  and M. M. Z. World Ocean Atlas 2018, Volume 1: Temperature. Tech. Ed. NOAA Atlas NESDIS 87 (2018).
2.	Boyer, T. P. et al. World Ocean Database 2018. NOAA Atlas NESDIS 87 (2018).

