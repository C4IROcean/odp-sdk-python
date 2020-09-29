# Example notebooks and extra functions

## TO RUN THE EXAMPLE NOTEBOOKS YOU NEED TO MAKE SURE YOU HAVE ACCESS TO THE FUNCTION FILES AND INSTALLED THE REQUIRED PACKAGES

CastFunctions.py, DataStatsFunctions.py and DataMaps.py live in this folder. Make sure you've cloned the repo and point to the proper path in the notebooks

In order to use CastFunctions.py and DataStatsFunctions.py, certain packages are necessary. To install these packages, you can run the line below with the proper path to install these packages. 

```bash
pip3 install -r requirements_func.txt
```

In order to use DataMaps.py you need to install Cartopy
## Conda Install
```bash
conda install -c conda-forge cartopy
```

## Pip Install
If using pip, first you need to install Homebrew, https://brew.sh/

Then run: 

```bash
brew install proj geos
```

And finally:

```bash
pip3 install cartopy
```