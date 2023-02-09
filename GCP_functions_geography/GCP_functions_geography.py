#%%writefile load_geography_to_bq.py
import warnings
warnings.simplefilter("ignore")
#!pip install  pyproj
#!pip install progress
#!pip install geojson
#!pip install geopandas


from google.colab import auth

auth.authenticate_user()

# change these to try this notebook out

# Tabla = 'directions'

import time
import os
 
# os.environ['PROJECT'] = project

# BigQuery API
from google.cloud import bigquery 
import chardet
import pandas as pd
#import geopandas
import pandas_gbq as gpd
import pprint
import time
import re
from lxml import etree
import glob
import zipfile
import shutil

bq_client = bigquery.Client(project=project)
storage_client = storage.Client(project=project)

def list_blobs(bucket_name):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)
    lista = []
    for blob in blobs:
        lista.append(blob.name)
    return lista    


class import_gs:
  def __init__(self, file_in_gs = '', Bucket = ''   ):
    self.name_in_gs = file_in_gs
    self.bucket_gs = Bucket
    

  
  def move_to_here(self):
    name_document = "./content/" 
    command = ('gsutil mv gs://{}/{} .'.format(self.bucket_gs, self.name_in_gs)   ) #+ " "+name_document
    print(command)
    os.system(command)

class load_to_bq:
  import zipfile
  import glob
  import os
  import shutil
  """ hay una recurrencia en el 'for i in range(len( self.Zip_files )):'
      Se deberia hacer la funcion get_fiels con toda las operaciones que contiene adentro ese For
      Luego hacer una funcion que detecte los diferentes zip que hay y que ejecute recursivamente esa operacion
  """
  def __init__(self,  formato="",  bq_dataset ='', bq_project ='' ):

    # self.tabla = tabla
 
    self.formato = formato
    self.Dataset = bq_dataset
    self.project = bq_project
  
    bq_client = bigquery.Client(project=self.project)
    storage_client = storage.Client(project=self.project)

  def load_into_bq(self):
    self.Schema = self.esquema
    job_config = bigquery.LoadJobConfig( schema=self.Schema,  write_disposition="WRITE_TRUNCATE"  )
    if self.formato.upper()!='TXT':
      ## Aca debo configurar tambien como cargar los shp
      job = bq_client.load_table_from_dataframe( self.tabla, self.table_id, job_config=job_config ) 
      job.result()  
      table = bq_client.get_table(self.table_id)
      # Make an API request.
      print( "Loaded {} rows and {} columns to {}".format( table.num_rows, len(table.schema), self.table_id ) )
    else:
      job = bq_client.load_table_from_dataframe( self.tabla, self.table_id_st, job_config=job_config ) 
      job.result()  
      # table = bq_client.get_table(self.table_id_st)
      # Make an API request.
      # print( "Loaded {} rows and {} columns to {}".format( table.num_rows, len(table.schema), self.table_id_st ) )
      if(len(self.txt_files)!= 1):
        if(self.iteracion == (len(self.txt_files) - 1 ) ):
          query = """ CREATE OR REPLACE TABLE {}
                    PARTITION BY year
                    OPTIONS () AS 
                    SELECT  SAFE_CAST(lon AS FLOAT64) AS LON,
                            SAFE_CAST(lat AS FLOAT64) AS LAT,
                            SAFE_CAST(Emission AS FLOAT64) AS Emission , 	 parse_date("%Y" ,	year) as year		, pollutant ,	sector_name,
                            ST_GEOGPOINT(SAFE_CAST(lon AS FLOAT64), SAFE_CAST(lat AS FLOAT64)) geography
                              FROM `{}.STANGING.{}` 
                    """.format( self.table_id, self.project ,   self.tabla_in_bq+"_*")

          job_ = bq_client.query(query)
        
          for row in job_.result():
            print(row)
          table = bq_client.get_table(self.table_id_st)
          print( "Loaded from stanginf: {} rows and {} columns to {}".format( table.num_rows, len(table.schema), self.table_id ) )
      else:
          query = """ CREATE OR REPLACE TABLE {}
                    PARTITION BY year
                    OPTIONS () AS 
                    SELECT  SAFE_CAST(lon AS FLOAT64) AS LON,
                            SAFE_CAST(lat AS FLOAT64) AS LAT,
                            SAFE_CAST(Emission AS FLOAT64) AS Emission , 	 parse_date("%Y" ,	year) as year		, pollutant ,	sector_name,
                            ST_GEOGPOINT(SAFE_CAST(lon AS FLOAT64), SAFE_CAST(lat AS FLOAT64)) geography
                              FROM `{}.STANGING.{}` 
                    """.format( self.table_id, self.project ,   self.tabla_in_bq+"_*")

          job_ = bq_client.query(query)
        
          for row in job_.result():
            print(row)
          table = bq_client.get_table(self.table_id_st)
          print( "Loaded from stanginf: {} rows and {} columns to {}".format( table.num_rows, len(table.schema), self.table_id ) )


  def get_fiels(self):
      self.Zip_files = glob.glob("/content/*.zip")
      if self.formato.upper()== "TXT":

          if len( self.Zip_files ) != 0:

            for i in range(len( self.Zip_files )):
              self.temp_path = self.Zip_files[0]
              self.sector_name = self.temp_path.split('/')[-1].split('_txt')[0]

              with zipfile.ZipFile(self.temp_path, 'r') as zip_ref:
                  zip_ref.extractall('/content/temp_dir')
              self.txt_files = glob.glob("/content/temp_dir/*.txt")
              # appended_data = []
              for i in range(len(self.txt_files) ):
                self.iteracion = i
                temp_file_path = self.txt_files[i]
                year = temp_file_path.split('/')[-1].split('_'+self.sector_name)[0].split('_')[-1] 
                pollutant = temp_file_path.split('/')[-1].split('_'+self.sector_name)[0].split('_')[-2] 
                #print('the sector is: {} for the year: {} and the pollutant: {}'.format(self.sector_name,year, pollutant ) )

                df = pd.read_csv(temp_file_path, sep = ';', skiprows = 3,  names= ['lat', 'lon', 'Emission' ] )
                df['year'] = year
                df['pollutant'] = pollutant
                df['sector_name'] = self.sector_name
                # appended_data.append(df)
                
                self.tabla = df #pd.concat(appended_data)
                 # df = appended_data
                self.tabla=self.tabla.applymap(str)
                self.tabla_in_bq = self.sector_name+'_'+pollutant 
                self.table_id_st = '{}.{}.{}'.format(project, 'STANGING',self.tabla_in_bq + "_" + year )
                self.table_id = '{}.{}.{}'.format(project, Dataset ,self.tabla_in_bq )
                ##################################
                self.esquema = []
                for i in self.tabla.columns:
                  self.schema_line   =   bigquery.SchemaField("""{}""".format(i), 'STRING' , mode = 'NULLABLE')
                  self.esquema.append(self.schema_line)          

                self.carga = self.load_into_bq()
              shutil.rmtree('/content/temp_dir', ignore_errors=False, onerror=None)
             
            return print('Done')        
      elif formato.upper()== "SHP":
          if len( self.Zip_files ) != 0:
            
            for i in range(len( self.Zip_files )):
              self.temp_path = self.Zip_files[i]
              # self.sector_name = self.temp_path.split('/')[-1].split('_txt')[0]

              try:
                with zipfile.ZipFile(self.temp_path, 'r') as zip_ref:
                    zip_ref.extractall('/content/temp_dir')
                self.txt_files = glob.glob("/content/temp_dir/*.shp")
                for i in self.txt_files:
                  NOMBRE_ = i.split('/')[-1].split('.')[0]
                  
                  self.table_id_st = """  `{}.STANGING.{}` """.format(self.project, NOMBRE_)
                  self.table_id  = """ `{}.{}.{}` """.format(self.project, self.Dataset, NOMBRE_)
                  shp_file = geopandas.read_file( i )
                  shp_file = shp_file.to_crs(4326)
                  shp_file['centroid']  = shp_file.centroid
                  shp_file=shp_file.applymap(str )
                  schema_ = []
                  
                  for j in shp_file.columns:
                    if j.upper() == 'GEOMETRY':
                      self.VAR_ = j
                    else:
                      pass

                    schema_line   =   bigquery.SchemaField("""{}""".format(j), 'STRING' , mode = 'NULLABLE')
                    print(schema_line)
                    schema_.append(schema_line)

                  job_config = bigquery.LoadJobConfig(  schema = schema_, write_disposition="WRITE_TRUNCATE", )
                  
                  job = bq_client.load_table_from_dataframe( shp_file, table_id.format(NOMBRE_), job_config=job_config )  # Make an API request.
                  job.result()
                
                  # if self.VAR_ in locals():
                  query = """ CREATE OR REPLACE TABLE {}
                                        OPTIONS () AS 
                                        SELECT  * except(GEOMETRY, centroid) , 
                                        safe.st_geogfromtext(geometry) GEOMETRY,
                                        safe.st_geogfromtext(centroid) CENTROID,
                                                  FROM {}
                                        """.format( self.table_id , self.table_id_st  )
                  # else:
                  #   query = """ CREATE OR REPLACE TABLE {}
                  #                 OPTIONS () AS 
                  #                 SELECT  *  FROM {}
                  #                 """.format( self.table_id , self.table_id_st  )

                  job_ = bq_client.query(query)
                  for row in job_.result():
                    print(row)
                  table_dest_bq = "{}.{}.{}".format(self.project , self.Dataset  , NOMBRE_  )
                  table = bq_client.get_table(table_dest_bq)
                  print( "Loaded from stanginf: {} rows and {} columns to {}".format( table.num_rows, len(table.schema), self.table_id ) )
                  shutil.rmtree('/content/temp_dir', ignore_errors=False, onerror=None)
              except:
                shutil.rmtree('/content/temp_dir', ignore_errors=False, onerror=None)
              os.system("rm {}".format(self.temp_path))

          else:
            self.txt_files = glob.glob("/content/*.shp")

          #Queda pendiente leer shapes, tomar https://colab.research.google.com/drive/11d5y2_NM1ftZwQk0f_n3gTHuCjyFDdl3#scrollTo=HDvW11fLV9Ky
          for i in txt_files:
            if(len(glob.glob("/content/*.dbf"))>= 1):
              NOMBRE_ = i.split('/')[-1].split('.')[0]

              self.table_id_st = """  `{}.STANGING.{}` """.format(self.project, NOMBRE_)
              self.table_id  = """ `{}.{}.{}` """.format(self.project, self.Dataset, NOMBRE_)
              
              shp_file = geopandas.read_file( i )
              shp_file = shp_file.to_crs(4326)
              shp_file['centroid']  = shp_file.centroid
              shp_file=shp_file.applymap(str )
              schema_ = []
              print(NOMBRE_)
              for j in shp_file.columns:
    
                if j.upper()== 'GEOMETRY':
                  self.VAR_ = j
                else:
                  pass
                schema_line   =   bigquery.SchemaField("""{}""".format(j), 'STRING' , mode = 'NULLABLE')
                # print(schema_line)
                schema_.append(schema_line)

              job_config = bigquery.LoadJobConfig(  schema = schema_, write_disposition="WRITE_TRUNCATE", )
              
              job = bq_client.load_table_from_dataframe( shp_file, table_id.format(NOMBRE_), job_config=job_config )  # Make an API request.
              job.result()
            
              # if self.VAR_ in locals():
              query = """ CREATE OR REPLACE TABLE {}
                              OPTIONS () AS 
                              SELECT  * except(GEOMETRY, centroid) , 
                              safe.st_geogfromtext(geometry) GEOMETRY,
                              safe.st_geogfromtext(centroid) CENTROID,
                                        FROM {}
                              """.format( self.table_id , self.table_id_st  )
              # else:
              #   query = """ CREATE OR REPLACE TABLE {}
              #                   OPTIONS () AS 
              #                   SELECT  *  FROM {}
              #                   """.format( self.table_id , self.table_id_st  )

              job_ = bq_client.query(query)
              for row in job_.result():
                print(row)
              table_dest_bq = "{}.{}.{}".format(self.project , self.Dataset  , NOMBRE_  )
              table = bq_client.get_table(table_dest_bq)
              print( "Loaded from stanginf: {} rows and {} columns to {}".format( table.num_rows, len(table.schema), self.table_id ) )
            os.system("rm ".format(i))


      else:
          pass




# def loading(self):
#   para_cargar = self.get_fiels()
#   self.load_into_bq()


#   load_to_bq(tabla= para_cargar[0], table_id_st=para_cargar[1], table_id=para_cargar[2]).load_into_bq()
 
