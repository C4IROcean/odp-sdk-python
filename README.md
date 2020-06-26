# ODP-SDK

Python SDK for Ocean Data Platform 

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
