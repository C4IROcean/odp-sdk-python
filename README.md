# Python SDK for Ocean Data Platform (ODP)

Main entrypoint into the Ocean Data Platform SDK. All services are made available through this object.

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install ODP-SDK.

```bash
pip install odp-sdk
```

## Usage

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
