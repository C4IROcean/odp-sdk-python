# -*- coding: utf-8 -*-
import setuptools


setuptools.setup(
    name="odp_sdk",
    version="0.3.6",  # Update this for every new version
    #py_modules = ['client'],
    author="ocean-kristian",
    author_email="kristian.authen@oceandata.earth",
    description='Python SDK for the Ocean Data Platform',
    long_description="Connect to the Ocean Data Platform with Python through the Python SDK. Download queried ocean data easily and efficiently into data frames, for easy exploring and further processing in your data science project.",
    long_description_content_type="text/markdown",
    install_requires=["cognite-sdk>=1.3",                      # Add project dependencies here
        "pandas>=0.20.0"                    # example: pandas version 0.20 or greater                          
    ],                                             
    url='https://github.com/C4IROcean/odp',  
    packages=setuptools.find_packages(),
    classifiers=(                                 # Classifiers help people find your 
        "Programming Language :: Python :: 3",    # projects. See all possible classifiers 
        "License :: OSI Approved :: Apache Software License", # in https://pypi.org/classifiers/
        "Operating System :: OS Independent",   
    ),
)


#python setup.py sdist bdist_wheel 
#twine upload --repository testpypi dist/*

