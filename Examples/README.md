# Example notebooks and extra functions

## TO RUN THE EXAMPLE NOTEBOOKS YOU NEED TO MAKE SURE YOU HAVE ACCESS TO THE FUNCTION FILES AND INSTALLED THE REQUIRED PACKAGES
## We recommend you create a new environment, install odp_sdk, and proceed with the following setup:

## 1) Make sure you've cloned the repo

## 2) In order to use UtilityFunctions.py you need to install Cartopy

###  Conda Install
If you are using Anaconda, we recommend installing cartopy through conda install
```bash
conda install cartopy
```

###  Pip Install 
Alternative cartopy insatllation through pip install
```bash
pip3 install cartopy
```

If you are using a Mac, and have issues pip installing cartopy, see fix below:

Install Homebrew, https://brew.sh/

Then run: 

```bash
brew install proj geos
```
And finally:

```bash
pip3 install cartopy
```
## 3) In order to use the numeric and visual utility-functions you need to install the full distribution of the ODP SDK

To install this distribution, you can run the line below:

```bash
pip3 install -U odp_sdk[all]
```

Alternatively, you can install the requirements specified by `reruirements-utils.txt`

```bash
pip3 install -r requirements-utils.txt
```


