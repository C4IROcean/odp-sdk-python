# Python SDK for Ocean Data Platform (ODP)

Connect to the Ocean Data Platform with Python through the Python SDK. Download queried ocean data easily and efficiently into data frames, for easy exploring and further processing in your data science project.

This is an extensions package to the Cognite Python SDK.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install ODP-SDK.

```bash
pip install odp-sdk
```

## Usage

Note: Acessing the Ocean Data Platform requires a personal api-key. Contact ODP to require one.

Import the Ocean Data Platform SDK 
```python
from client import ODPClient
```
Connect to the platform
```python
client = ODPClient(api_key='your_personal_api_key_from_ODP',
                       project="odp", client_name="odp")
```
Get dataframe of ocean data samples (casts) within search criteria

```python
df=client.casts(longitude=[-10,35],
                latitude=[50,80],
                timespan=['2015-01-01','2019-12-01'],
                n_threads=10) 
```
## Jupyter Notebook Examples 
- Download data, plot the casts and create a gridded map of surface temperatures [here](https://github.com/C4IROcean/ODP-SDK/blob/master/Examples/ExampleNotebook-01.ipynb)
