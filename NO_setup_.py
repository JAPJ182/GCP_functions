from setuptools import  find_packages
import setuptools

# with open("README.md", "r", encoding="utf-8") as fh:
#     long_description = fh.read()
from os import path
here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    requirements = f.read().splitlines()
setuptools.setup(
    name='GeoGCP',
    version='0.0.1',
    author='Jaime Polanco',
    author_email='jaime.polanco@javerian.edu.co',
    py_modules=['GeoGCP'],
    description='Load geographic data into GCP', 
    long_description_content_type="text/markdown",
    url='https://github.com/JAPJ182/GeoGCP',
    license='MIT',
    install_requires= 	requirements,
    #packages=find_packages("GeoGCP",  exclude=["*.test", "*.test.*", "test.*", "test"]) ,
    packages=['GeoGCP'], #,'geopandas', 'geopy', 'geojson'
    # install_requires=['requests'],
)
