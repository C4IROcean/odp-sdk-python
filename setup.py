from setuptools import setup
setup(name='odp_sdk',
      version='0.1',
      description='Python SDK for the Ocean Data Platform',
      url='https://github.com/C4IROcean/ODP-SDK',
      author='ocean-kristian',
      author_email='kristian.authen@oceandata.earth',
      license='Apache License 2.0',
      packages=['client'],
      install_requires=['cognite-sdk>=1.8',
                        'matplotlib>=1.5'])