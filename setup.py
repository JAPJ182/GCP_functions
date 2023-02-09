
import setuptools

# with open("README.md", "r", encoding="utf-8") as fh:
#     long_description = fh.read()
from os import path
here = path.abspath(path.dirname(__file__))
with open(path.join(here, 'requirements.txt'), encoding='utf-8') as f:
    requirements = f.read().splitlines()
setuptools.setup(
    name='GCP_functions_geography',
    version='0.0.1',
    author='Jaime Polanco',
    author_email='jaime.polanco@javerian.edu.co',
    #py_modules=['GCP_functions_geography'],
    description='Load geographic data into GCP', 
    long_description_content_type="text/markdown",
    url='https://github.com/JAPJ182/GCP_functions_geography',
    license='MIT',
    install_requires= 	requirements,
    packages=find_packages("GCP_functions_geography", exclude=["*.test", "*.test.*", "test.*", "test"]) ,
    #packages=['geopandas', 'geopy', 'geojson'],
    # install_requires=['requests'],
)
