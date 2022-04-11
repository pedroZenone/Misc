import sys, os 
prj_path = os.path.dirname(os.getcwd())
utils_path = prj_path + '/utils'
if  not utils_path in sys.path:
    print('adding utils to path ')
    sys.path.insert(1, utils_path)

    
import yaml
import logging
from google_cloud import BigQuery, Storage

import base64
import glob
import time

import pandas as pd
import joblib
import numpy as np
from typing import Union
import string
import random
import re
import shutil
import pickle
from pathlib import Path
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

def read_yaml_file(filename):
    with open(filename, 'r') as stream:
        try:
            r = yaml.safe_load(stream)
            return r
        except yaml.YAMLError as exc:
            print(exc)
            raise exc

def load_conf(files):
    conf = dict()
    for file in files:
        conf.update(read_yaml_file(file))
    return conf

files = glob.glob(f"{utils_path}/*.yml") 
conf = load_conf(files)

AUTH_BIGQUERY = base64.b64decode(os.environ['SECRET_AUTH_BIGQUERY_MODEL'])
bq = BigQuery(AUTH_BIGQUERY)
storage =Storage(AUTH_BIGQUERY)

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def backbone_table_production(table_name: str,
                              site: str,
                              conf: dict(),
                              periods: Union[list,tuple]
                             ):
    
    """ BACKBONE TABLE.
    In general this table is for model population.
    table_name: name of the table (declared on conf.yml)
    site: palce, e.g: MLA, MLB ..
    periods: Dates to be processed in %Y-%m-%d format.
    """
    logger.info(f"Create table {table_name}_{site} if not exist")
    bq.execute(conf['BACKBONE_CREATE'].format(table_name = table_name,site = site).replace('\n',''))
    
    logger.info(f"Truncate table {table_name}_{site}")
    bq.execute(conf['BACKBONE_TRUNCATE'].format(table_name = table_name, site = site).replace('\n',''))
    
    if isinstance(periods,list):
        for p in periods:
            logger.info(f"Backbone for New Buyers date {p}")
            bq.execute(conf['BACKBONE'].format(
                table_name = table_name,
                site = site,
                date_intial = p,
                date_end = p ).replace('\n','')
                      )
    elif isinstance(periods,tuple):
        logger.info(f"Backbone for New Buyers: from {periods[0]} to {periods[1]}")
        bq.execute(conf['BACKBONE'].format(
            table_name = table_name,
            site = site,
            date_intial = periods[0],
            date_end = periods[1]).replace('\n','')
                  )
    
    logger.info(f"End Backbone building {table_name}_{site}")

def QueryParamatrized(query_: str,
                      query_param: dict(),
                      conf: dict()
                     ):
    """ Parameterized query.
    query_: Query declared on queries.yml
    query_param: Parameters in form of dictionary
    conf: Reference of a dictionary with queries.
    """

    logger.info(f"Excecuting {query_} with params: {query_param}")
    query = conf[f'{query_}'].format(**query_param).replace('\n','')
    return query

def Var2GCP(variable: str, 
            conf: dict(), 
            query_:str, 
            query_param: dict(),
            type_: str,
           ):
    """This function is usefull for production porpouses, is a wrapper for a 
    bq.fast_export for variable creation from a query to GCP.
    
    NOTE: Use the run date declared in conf file. For iteration change this value.
    
    variable: Name of the variable. (eg. WVAR related with params.yml)
    conf: instance of params.yml
    query_: query to run (defined in queries.yml)
    query_param: paramaters.
    type_: Use for subdirectory on gcp bucket to store 
    (by definition only train and apply)."""
    
    prj, bu, model = conf['PROYECT'],conf['BUSINESS_UNIT'],conf['MODEL']
    site = conf['SITE']
    bucket_path = f'{prj}/{bu}/{model}/{site}/'
    master_vars_path = conf['MASTERS_VARS_PATHS']
    
    if type_ in ['train','apply']:
        logger.info(f"Start creating variable : {variable} for site {site}")
        vars_ = list(master_vars_path.keys())   
        if variable in vars_:

            bucket_dataset_path = f"{bucket_path}{type_}{master_vars_path[f'{variable}']}"
            logger.info(f"Writing on : {bucket_dataset_path}")

            bq.fast_export(
                query = QueryParamatrized(query_,query_param,conf),
                destionatio_path = f"{bucket_dataset_path}"
                )
            logger.info(f"End process for variable : {variable}")
        else:
            logger.info(f"Variable {variable} need to be in : {list(master_vars_path.keys())}")
    else:
        logger.info(f"Argument {type_} need to be 'train' or 'apply' ")
        
def download_(temporal_dir: str,
              bucket_path:str,
              bucket_var:str,
              is_prod: bool = False):
    """Download a parquet file from a GCP bicket.
    temporal_dir: Temporal directory to download the parquets files.
    bucket_path: GCP path 1. (eg. 'gs://marketing-modelling/MP/EARLY_LTV/MLB/')
    bucket_var:  GCP path 2. (eg. '/dataset/WINDOW_VARS')
    is_prod: Indicate if the function is in production (apply sub dir) or
    training (train sub dir) in the GCP.
    
    Return a content of the GCP path inside a local directory.
    """
    logger.info(f"Looking in: bucket_path: {bucket_path} and subdir {bucket_var}")
    if is_prod == True:
        try:
            storage.download_folder(
                gs_source_path = f"{bucket_path}apply{bucket_var}",
                destination_folder= f"{temporal_dir}/")
            logger.info(f"Calling to {bucket_path}apply{bucket_var}")
        except Exception as e:
            logger.info(f"Error traying to donwload from {bucket_path}apply{bucket_var}")
            logger.info(f"{e}") 
            raise e
    else:
        try:
            storage.download_folder(
                gs_source_path = f"{bucket_path}train{bucket_var}",
                destination_folder= f"{temporal_dir}/")
        except Exception as ee:
            logger.info(f"Error traying to donwload from {bucket_path}train{bucket_var}")
            logger.info(f"{ee}")
            raise ee
    
    return None

def list_files(temporal_dir: str):
    """ List files inside a folder"""
    logger.info(f"Listed files on : {temporal_dir}")
    listOfFiles = list()
    for (dirpath, dirnames, filenames) in os.walk(temporal_dir):
        listOfFiles += [os.path.join(dirpath, file) for file in filenames]
    
    for file in listOfFiles:
        logger.info(f"file : {file}")
    
    return listOfFiles

def omnesprouno(temporal_dir: str,
                parquet_name:str,
                filter_: str = None):
    
    """ Read all the parquet files in a directory an append 
    in a single parquet.
    TODO: check .parquets with same structure.
    temporal_dir: directory where we wannt to download the parquet files.
    parquet_name: name of the final parquet file.
    filter_: some filter to apply (in the sense of pandas query) eg. CUS_CUST_ID = 123456
    """
    #Listed files
    listOfFiles = list_files(temporal_dir)
    
    logger.info(f"Reading and load to memory")
    df = pd.concat([pd.read_parquet(f) for f in listOfFiles])
    df.reset_index(drop = True, inplace = True)
    logger.info(f"Total Concat DataFrame size : {df.shape}")
        
    if isinstance(filter_,str):
        try:
            df_ = df.query(filter_).copy()
            logger.info(f"Filter apply {filter_}")
        except Exception as e:
            logger.info(f"No filter apply error: {e}")
            df_ = df.copy()
            raise e
    else:
        df_ = df.copy()
    
    del df
    df = df_.copy()
    
    logger.info(f"Total DataFrame size to write: {df.shape}")
    logger.info(f"Writing on  : {temporal_dir}/{parquet_name}.parquet")
    df.to_parquet(f'{temporal_dir}/{parquet_name}.parquet')
    return df

def remove_content(dir_path: str,
                   exceptions: list):
    """Remove all the content in a path."""
    
    content = [ f for f in os.listdir(dir_path) if f not in exceptions ]
    for f in content:
        path = os.path.join(dir_path, f)
        if os.path.isdir(path):
            shutil.rmtree(path)
        elif os.path.isfile(path):
            os.remove(path)
        else:
            logger.info(f"Can't remove {path}")
            
def random_name(n: int):
    """ Random ascci letters generator.
    n: number of letters.
    """
    return ''.join(random.choice(string.ascii_letters) for x in range(n))

def constant_imputer(serie: pd.Series,
                     value: (int,str)):
    """Imputer NaN by a constatn value."""
    logger.info(f"% OF NaN in {serie.name}: {np.round(serie.isnull().mean(),5)}")
    return serie.fillna(value = value)


def cast_column(serie: pd.Series,
                type_:str):
    """Cast a column type. """
    return serie.astype(type_)


def remap_column(serie,dict_):
    """Replace values on a column, using a dictionary.
    wrapper of a map function from pandas."""
    return serie.map(arg = dict_)


def categorical_imputer(series = pd.Series,
                        proportions = dict()
                       ):
    """ Gererate a random variable from a data distribution 
    and impute NaN values by a realization.
    
    series: Pandas Series to be imputed.
    proportions: Dictionary with the distribution of every class 
    to be used in de imputation procedure. 
    eg: {calss 1: p1, calss 2: p2, ...classn: pn} with p1+..+pn = 1 """
    
    x = np.round(series.isnull().mean(),2)
    logger.info(f"% OF NaN in {series.name}: {x}")
    rv = np.random.choice(a = [*proportions.keys()],
                          size = series.isna().sum(),
                          p = [*proportions.values()]
                         ) 
    series.loc[series.isna()] = rv
   
    return series

def prefix_source_info(df: pd.DataFrame, 
                       prefix: str,
                       exceptions: list()
                      ):
    """ Add a prefix on all columns, With exception.
    df: Pandas DataFrame.
    prefix: Prefix for the columns of the dataframe.
    exceptions: list of column names without prefix (like user id, or target).
    
    Keep in mind this prefix is in relation to the source of information.
    """
    
    if not df.empty :
        logger.info(f"Adding prefix {prefix}_ to DataFrame")
        keep_same = set(exceptions)
        df.columns = [ c if c in keep_same else  prefix + '_' + c for c in df.columns]
        return df
    else: 
        return None
    
def outlier_clipping(series:pd.Series,q:float = .95):
    """Replace the values over the percentile 95 with the value
    of this percentile."""
    
    top = np.nanquantile(series,q)
    logger.info(f"{series.name} - MAX : {np.max(series)} WILL BE REPALCED BY {top}")
    series.loc[series > top] = top
    
    return series  

def model_population(backbone_table:str,site:str):
    """Call the model population from DB.
    backbone_table: table with the model population on GCP (eg. 'meli-marketing.TEMP45.CY_NB_ML_BACKBONE').
    site: site (e.g 'MLB')
    """
    logger.info(f'calling: SELECt * FROM {backbone_table}_{site}')
    df = bq.execute_response(f'SELECt * FROM {backbone_table}_{site}')
    df['SENT_DATE'] = pd.to_datetime(df.SENT_DATE)
    df['CONVERSION'] = df['CONVERSION'].astype(np.int64)
    return df

def master_processing(conf:dict,
                      site: str,
                      proc_funs: dict,
                      to_parquet: bool = True
                     ):
    """Create a master table for training a model.
    - Call all the variables stored on a gcp bucket and join to a 
    model population.
    - Run a set of processing function declared on processing_functions.py 
    and stored in a dictionary (e.g {variable: fun}, where variable need to be declared 
    in the conf file 'PUSH_VARS': proc_pushvars})
    Parameters:
    conf: dictionary with parameters.
    to_parquet: Flag True will write the file in a local path.
   """
    #Some configurations.
    backbone_table = conf['BACKBONE_TABLE_NAME']
    master_vars_path = conf['MASTERS_VARS_PATHS']
    mater_path_gcp = conf['MASTER_BUCKET_PATH']
    master_local_path = conf['MASTER_LOCAL_PATH']
    prj, bu, model = conf['PROYECT'],conf['BUSINESS_UNIT'],conf['MODEL']
    bucket_path = f'{prj}/{bu}/{model}/{site}/'
    vars_ = conf['PREFIX_MASTERS'].keys()
    temporal_dir = random_name(10)
    logger.info(f'Ancilary Temporal dir: {temporal_dir}')

    for var in vars_:
        logger.info(f">>> DOWNLOAD {var}")
        temporal_subdir = f"{temporal_dir}/{var}"
        os.makedirs(f"{temporal_subdir}",exist_ok=True)
        logger.info(f"Temporal subdir created: {temporal_subdir}.")

        download_(temporal_dir = temporal_subdir,
              bucket_path = bucket_path,
              bucket_var = master_vars_path[var],
              is_prod = False)

        omnesprouno(temporal_dir = temporal_subdir,
                    parquet_name = f'{var}')

        logger.info("Remove temp files.")
        remove_content(dir_path = temporal_subdir, 
                       exceptions = f'{var}.parquet')
       
    vars_ = conf['PREFIX_MASTERS'].keys()
    logger.info(f'>>> DOWNLOAD MODEL POPULATION.')
    master = model_population(backbone_table = backbone_table, site = site)
    logger.info(f'>>> MODEL POPULATION SIZE {master.CUS_CUST_ID.nunique()}.')
    logger.info(f'>>> PROCESSING VARS')
    for var in vars_:
        temporal_subdir = f"{temporal_dir}/{var}"
        df = pd.read_parquet(temporal_subdir +f'/{var}.parquet')
        df = proc_funs[var](df.copy())
        master = master.merge(df, on = ['CUS_CUST_ID','SENT_DATE'],
                              how = 'left',
                              validate='1:1')
        del df
    
    logger.info(f"-------- Final NaN Report --------")
    report = master.isnull().sum()
    report = report.loc[report>0]
    logger.info(f"{list(report.to_dict().items())}")

    master['W'] = remap_column(serie = master['EVENT_TYPE'],dict_ = {'sent':1, 'control':0})
    master.drop(columns='EVENT_TYPE', inplace=True)
    master.rename(columns={"CONVERSION": "Y"}, inplace=True)
    
    if to_parquet:
        output_path = f"{os.getcwd()}{master_local_path}"
        os.makedirs(output_path,exist_ok=True)
        format_time = datetime.now().strftime('%Y%m%d%H%M')
        logger.info(f"Writing master ABT on {output_path}{format_time}.pkl")
        master.to_parquet(f"{output_path}{format_time}.parquet")
    
    #Ready for write ABT on GCP.
    subdir = 'train'      
    logger.info(f"Writing master ABT on {bucket_path}{subdir}{mater_path_gcp}/{format_time}.pkl") 
    storage.upload_file(source_file = f"{output_path}{format_time}.pkl",
                        gs_destination_path = f"{bucket_path}{subdir}{mater_path_gcp}/{format_time}.pkl" )
    
    logger.info("rm -r {temporal_dir}")    
    shutil.rmtree(temporal_dir)
    shutil.rmtree(output_path)
    
    return master

def daily_consumption(budget:float,
                      eop:str,
                      date:str,
                      Fj:float,
                      verbose = True):
    """Estimacion del consumo diario necesrio para consumir B en los proximos dias del mes.
    budget: Budget for the campain.
    eop: Last day of the campain. Date for 100% of the budget need to be consumed (e.g '2022-04-30')
    date: today.
    Fj: accumulated consumption of the budget until day j (i - 1).
                        fi = (B-F(t=i-1))/(n-i+1)
    ----------------------------
    Output:
    fi: daily consumption estimation for date (today)
    """
     #How manny days left until the budget need to be consumet (n-i+1)
    date = pd.to_datetime(date)
    n = (pd.to_datetime(eop) - date).days
    #predefined daily consumption.
    fi = (budget - Fj)/(n)  
    if verbose:
        print(f'date of consumption: {(date).strftime("%Y-%m-%d")} n(i) = {n}, F(i-1) = {Fj}, f(i) = {fi}')

    return fi

def consumtion_expand(site:str,bop:str,eop:str):
    """ETL for New Buyers consumtion between bop and eop.
    site: site 
    bop: begening of the period of the campaing.
    eop: end of the period for the campaing.
    """
    q = QueryParamatrized(query_ = 'NB_CONSUMTION',
                          query_param = {'bop':bop ,'eop':eop, 'site':site},
                          conf  = conf
                         )
    df = bq.execute_response(q)
    logger.info(f"Size of the dataframe {df.shape}")

    if df.empty: 
        sys.exit(f"Check, no information on consumtion between {bop} and {eop}")

    #CAST
    df['AMOUNT'] = df.AMOUNT.astype(float)
    df['pay_created_dt'] = pd.to_datetime(df.pay_created_dt)
    #TIME EXPAND
    time_series = pd.date_range(start = bop, end = eop,freq = 'D')
    month_seq = pd.DataFrame({'pay_created_dt':time_series})
    #BACKBONE
    df = month_seq.merge(df[['pay_created_dt','AMOUNT']],on = ['pay_created_dt'],how = 'left')
    logger.info(f"Complet the dataframe (id is necessary) - new size: {df.shape}")
    df.AMOUNT.fillna(value = 0, inplace = True)
    df['ACC_AMOUNT'] = df['AMOUNT'].cumsum()
    logger.info(f"Total amount of consumtion is {np.nanmax(df.ACC_AMOUNT)}")
    return df