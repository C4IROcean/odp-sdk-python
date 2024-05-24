<a href="https://www.oceandata.earth/">
    <img src="assets/ODP-SDK.png" alt="ODP SDK logo" title="ODP" align="right" height="100" />
</a>


# ODP Python SDK

Connect to the Ocean Data Platform with Python through the Python SDK. Download queried ocean data easily and efficiently into data frames, for easy exploring and further processing in your data science project.

## Documentation

[WIP]

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install the Ocean Data Platform Python SDK.

```bash
pip3 install odp_sdk
```
 
## Usage

*Note: Accessing the Ocean Data Platform requires an authorized account. Contact ODP to require one.*

```python
from odp_sdk.client import OdpClient

client = OdpClient()

for item in client.catalog.list():
     print(item)
```

Examples can be found in /examples. 
