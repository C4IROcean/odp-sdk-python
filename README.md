# Python SDK for Ocean Data Platform (ODP)

Connect to the Ocean Data Platform with Python through the Python SDK. Download queried ocean data easily and efficiently into data frames, for easy exploring and further processing in your data science project.

This is an extensions package to the Cognite Python SDK.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the Ocean Data Platform Python SDK.

```bash
pip install odp_sdk
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

```python
df=client.casts(longitude=[-10,35],
                latitude=[50,80],
                timespan=['2019-06-01','2019-06-07']) 
```

CastFunctions.py include useful features for interpolating and plotting. This package is not a part of the odp_sdk package

## Jupyter Notebook Examples 
Example notebooks are found in the Example folder, which includes:
- Download data, plot the casts and create a gridded map of surface temperatures 
- Download avilable casts, explore them in an interactive map and plot the cast profile

## Data Description
The data available through the Ocean Data Platform are oceanographic measurements of physical and chemical ocean parameters (temperature, salinity, oxygen, nitrate, ph and chlorophyll), and is based on the data available through NOAA's World Ocean Database. Each cast has a sepcified latitude, longitude and time (lat, lon and datetime), and a depth profile, where each depth has measured phisical and chemical parameters. Not all casts have all the the different ocean parameters, missing measurements are populated with nans. Each measurements has a WODflag parameter (i.e Nitrate_WODflag). If flag value is zero, there are no known issues with the measured value. 

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

See WORLD OCEAN DATABASE 2018 User’s Manual for more infrmation about the dataset [link](https://rda.ucar.edu/datasets/ds285.0/docs/WOD18-UsersManual_final.pdf )
