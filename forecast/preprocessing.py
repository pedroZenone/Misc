#import dask.dataframe as dd
#import dask.array as da
import datetime
#import pandas as pd

#import modin.pandas as pd_modin

import pandas as pd

import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
import boto3
import pickle
import numpy as np
import joblib
import os
import seaborn as sns
import warnings
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from lifetimes import ModifiedBetaGeoFitter
from lifetimes import GammaGammaFitter
from dateutil.relativedelta import relativedelta
from sklearn.model_selection import StratifiedKFold
import s3fs
import re
from unicodedata import normalize
from sklearn.cluster import KMeans
from sklearn.pipeline import Pipeline
import random
import time
import gc

s3 = boto3.client('s3')
s3.download_file("fury-data-apps", "marketing-utils/pzenone/utils.py","utils.py")
import utils

import sys

sys.path.append(os.path.dirname(os.path.expanduser("../defines.py")))
from defines import *

def read_gfs_parquet(gcps_path_in, gstorage, folder, modin = False):
    gstorage.download_folder(gcps_path_in + folder, f"./{folder}")
    
    df = pd.read_parquet(f'./{folder}')
    return df

def read_merge(gcps_path_in, gstorage, folder, summary_cal, modin = False):
    right = read_gfs_parquet(gcps_path_in, gstorage, folder, modin = False)
    
    if modin:
        summary_cal = pd_modin.concat([summary_cal.set_index('CUS_CUST_ID'),right.set_index('CUS_CUST_ID')], axis=1).reset_index()
    else:
        summary_cal = pd.concat([summary_cal.set_index('CUS_CUST_ID'),right.set_index('CUS_CUST_ID')], axis=1).reset_index()

    del right
    gc.collect()
    return summary_cal

def orders_frequencies(bqs,gcps_path_in,summary_cal,train_test):
    
    float_vars = ['ASP_long', 'ASP_short', 'productos_long', 'productos_short',
 'ordenes_SUM_long', 'ordenes_MEAN_long', 'ordenes_SUM_short', 'ordenes_MEAN_short', 'freq_D_long',
 'freq_D_short', 'freq_M_long', 'freq_M_short', 'freq_W_long', 'freq_W_short', 'freq_7D', 'freq_180D',
 'my_first_in_windows', 'my_recency', 'SI_SUM_long', 'SI_MEAN_long', 'SI_SUM_short', 'SI_MEAN_short',
 'franquero', 'money_mean', 'money_max', 'money_min', 'money_CV', 'money_sum', 'size_0', 'size_1',
 'size_2', 'sales_0', 'sales_1', 'sales_2', 'recency_0', 'recency_1', 'recency_2']
    float_dict = dict.fromkeys(float_vars, 'float')
    summary_cal = summary_cal.astype(float_dict)
    summary_cal = summary_cal.fillna({'ASP_long':FILL_FLOAT,
                     'ASP_short':FILL_FLOAT,
                     'productos_long':FILL_FLOAT,
                     'productos_short':FILL_FLOAT,
                     'ordenes_SUM_long':FILL_FLOAT,
                     'ordenes_MEAN_long':FILL_FLOAT,
                     'ordenes_SUM_short':FILL_FLOAT,
                     'ordenes_MEAN_short':FILL_FLOAT,
                     'freq_D_long':FILL_FLOAT,
                     'freq_D_short':FILL_FLOAT,
                     'freq_M_long':FILL_FLOAT,
                     'freq_M_short':FILL_FLOAT,
                     'freq_W_long':FILL_FLOAT,
                     'freq_W_short':FILL_FLOAT,
                     'freq_7D':FILL_FLOAT,
                     'freq_180D':FILL_FLOAT,
                     'my_first_in_windows':FILL_RECENCY,
                     'my_recency':FILL_RECENCY,
                     'SI_SUM_long':FILL_FLOAT,
                     'SI_MEAN_long':FILL_FLOAT,
                     'SI_SUM_short':FILL_FLOAT,
                     'SI_MEAN_short':FILL_FLOAT,
                     'franquero':FILL_FLOAT,
                     'money_mean':FILL_FLOAT,
                     'money_max':FILL_FLOAT,
                     'money_min':FILL_FLOAT,
                     'money_CV':FILL_FLOAT,
                     'money_sum':FILL_FLOAT,
                     'size_0':FILL_FLOAT,
                     'size_1':FILL_FLOAT,
                     'size_2':FILL_FLOAT,
                     'sales_0':FILL_FLOAT,
                     'sales_1':FILL_FLOAT,
                     'sales_2':FILL_FLOAT,
                     'recency_0':FILL_RECENCY,
                     'recency_1':FILL_RECENCY,
                     'recency_2':FILL_RECENCY})
    
    return summary_cal

def asp_pareto(bqs,gcps_path_in,summary_cal,train_test):

    summary_cal['ASP_PARETO'] = summary_cal['ASP_PARETO'].astype('float')
    summary_cal['ASP_PARETO'] = summary_cal['ASP_PARETO'].fillna(FILL_FLOAT)
    return summary_cal

def pareto(bqs,gcps_path_in,summary_cal,train_test):
    
    if(train_test == "train_train"):
        # ASP
        ggf = GammaGammaFitter(penalizer_coef = 0)
        ggf.fit(summary_cal['freq_D_long'],summary_cal['ASP_PARETO'])

        # Guardo modelos
        ggf.save_model("ASP.pkl")
        bqs.upload_file('ASP.pkl',gcps_path_out + 'ASP.pkl')

    if(train_test == "test"):
        bqs.download_file(gcps_path_out + 'ASP.pkl','ASP.pkl')
        
        ggf = GammaGammaFitter(penalizer_coef = 0)
        ggf.load_model("ASP.pkl")

    # Aplico modelos. Caso de train no porque tiene que ir precargandose cada segmento
    if(train_test != "train"):
        summary_cal["ASP_forecast"] = ggf.conditional_expected_average_profit(summary_cal['freq_D_long'],summary_cal['ASP_PARETO'])
        del ggf
    
    return summary_cal


def IPT(bqs,gcps_path_in,summary_cal,train_test):
    
    float_vars = ['IPT_sum_long',
             'IPT_sum_short',
             'IPT_mean_long',
             'IPT_mean_short',
             'IPT_std_long',
             'IPT_std_short']
    float_dict = dict.fromkeys(float_vars, 'float')
    summary_cal = summary_cal.astype(float_dict)
    
    # si no tiene ipt, le imputo valor alto
    summary_cal = summary_cal.fillna({'IPT_sum_long':FILL_IPT,
         'IPT_sum_short':FILL_IPT,
         'IPT_mean_long':FILL_IPT,
         'IPT_mean_short':FILL_IPT,
         'IPT_std_long':FILL_IPT,
         'IPT_std_short':FILL_IPT})

    return summary_cal

def visitas_short(bqs,gcps_path_in,summary_cal,train_test):
    
    float_vars = ['recency_date_90d',
         'fist_date_90d',
         'recency_date',
         'first_date',
         'cant_dias_active',
         'cant_dias_active_90d',
         'bookmarks']
    float_dict = dict.fromkeys(float_vars, 'float')
    summary_cal = summary_cal.astype(float_dict)
    
    summary_cal = summary_cal.fillna({'recency_date_90d':FILL_FLOAT,
                 'fist_date_90d':FILL_FLOAT,
                 'recency_date':FILL_FLOAT,
                 'first_date':FILL_FLOAT,
                 'cant_dias_active':FILL_FLOAT,
                 'cant_dias_active_90d':FILL_FLOAT,
                 'bookmarks':FILL_FLOAT})
    
    
    return summary_cal
    

def locations(bqs,gcps_path_in,summary_cal,train_test):
    
    float_vars = ['SHP_ADD_ZIP_CODE', 'MOST_SEEN']
    
    str_vars = ['location']
    
    float_dict = dict.fromkeys(float_vars, 'float')
    str_dict = dict.fromkeys(str_vars, np.dtype('O'))
    summary_cal = summary_cal.astype(float_dict)
    summary_cal = summary_cal.astype(str_dict)
    
    summary_cal = summary_cal.fillna({'location':'other_state',
                                      'SHP_ADD_ZIP_CODE':FILL_FLOAT,
                                      'MOST_SEEN':FILL_FLOAT})
    
    
    summary_cal['location'] = np.where(np.isin(summary_cal['location'],STATES[PAIS]), summary_cal['location'], 'other_state')
        
    summary_cal = my_cat_imputer(summary_cal,"location","other_state")
    
    return summary_cal



def demograficos(bqs,gcps_path_in,summary_cal,train_test):

    float_vars = ['DAYS_FROM_REGISTRATION',
 'DAYS_FROM_FIRST_PUBLICATION',
 'DAYS_FROM_FIRST_BUY',
 'DAYS_FROM_FIRST_SELL']
    
    float_dict = dict.fromkeys(float_vars, 'float')
    summary_cal = summary_cal.astype(float_dict)
    
    # Relleno datos faltantes que no debiesen ser faltantes y corrijo menores a 0

    summary_cal["DAYS_FROM_FIRST_BUY"] = np.where(summary_cal["DAYS_FROM_FIRST_BUY"].isna(),summary_cal["my_recency"],summary_cal["DAYS_FROM_FIRST_BUY"])
    summary_cal["DAYS_FROM_REGISTRATION"] = np.where(summary_cal["DAYS_FROM_REGISTRATION"].isna(),summary_cal["my_recency"],summary_cal["DAYS_FROM_REGISTRATION"])
    
    # En caso de que haya cosas raras, corrijo
    summary_cal["DAYS_FROM_FIRST_BUY"] = np.where(summary_cal["DAYS_FROM_FIRST_BUY"] < 0,summary_cal["my_first_in_windows"],summary_cal["DAYS_FROM_FIRST_BUY"])
    summary_cal["DAYS_FROM_REGISTRATION"] = np.where(summary_cal["DAYS_FROM_REGISTRATION"] < 0,summary_cal["DAYS_FROM_FIRST_BUY"],summary_cal["DAYS_FROM_REGISTRATION"])
    summary_cal["DAYS_FROM_FIRST_PUBLICATION"] = np.where(summary_cal["DAYS_FROM_FIRST_PUBLICATION"] < 0, FILL_RECENCY,summary_cal["DAYS_FROM_FIRST_PUBLICATION"])
    summary_cal["DAYS_FROM_REGISTRATION"] = np.where(summary_cal["DAYS_FROM_REGISTRATION"] < summary_cal["DAYS_FROM_FIRST_BUY"],summary_cal["my_first_in_windows"],summary_cal["DAYS_FROM_REGISTRATION"])
    
    summary_cal = summary_cal.fillna({'DAYS_FROM_REGISTRATION':FILL_RECENCY,
             'DAYS_FROM_FIRST_PUBLICATION':FILL_RECENCY,
             'DAYS_FROM_FIRST_BUY':FILL_RECENCY,
             'DAYS_FROM_FIRST_SELL':FILL_RECENCY})
    
    return summary_cal

def reg_data(bqs,gcps_path_in,summary_cal,train_test):  # solo mlb!!
        
        float_vars = ['REG_CUST_SCHOOL', 'REG_CUST_INCOMES', 'REG_CUST_BIRTHDATE']
        str_vars = ['REG_DATA_TYPE','REG_CUST_GENDER','REG_CUST_PROFESSION','REG_CUST_MATERIAL_STATUS']
        float_dict = dict.fromkeys(float_vars, 'float')
        str_dict = dict.fromkeys(str_vars, np.dtype('O'))
        summary_cal = summary_cal.astype(float_dict)
        summary_cal = summary_cal.astype(str_dict)
        
        # tipo persona
        summary_cal["REG_DATA_TYPE"] = np.where(summary_cal["REG_DATA_TYPE"].isin(['person','company']),summary_cal["REG_DATA_TYPE"],"NA")
        
        # Genero
        summary_cal["REG_CUST_GENDER"] = summary_cal["REG_CUST_GENDER"].str.strip()
        summary_cal["REG_CUST_GENDER"] = np.where(summary_cal["REG_CUST_GENDER"].isin(['MASCULINO', 'FEMININO']),summary_cal["REG_CUST_GENDER"],"NA")

        # edad
        summary_cal["REG_CUST_BIRTHDATE"] = np.where((summary_cal["REG_CUST_BIRTHDATE"] <= 0) | (summary_cal["REG_CUST_BIRTHDATE"] > 100),FILL_RECENCY,summary_cal["REG_CUST_BIRTHDATE"])
        
        # Estado civil
        summary_cal["REG_CUST_MATERIAL_STATUS"] = summary_cal["REG_CUST_MATERIAL_STATUS"].str.strip()
        summary_cal['REG_CUST_MATERIAL_STATUS'] = summary_cal['REG_CUST_MATERIAL_STATUS'].replace({"UNIAO ESTAVEL":"CASADO(A)","DIVORCIADO(A)":"CASADO(A)","VIUVO(A)":"CASADO(A)"})  
        
        # Por las dudas
        summary_cal["REG_CUST_MATERIAL_STATUS"] = np.where(summary_cal["REG_CUST_MATERIAL_STATUS"].isin(['CASADO(A)', 'SOLTEIRO(A)','DIVORCIADO(A)','VIUVO(A)']),summary_cal["REG_CUST_MATERIAL_STATUS"],"NA")

        # imputacion de valores con distribucion categorica
        summary_cal = my_cat_imputer(summary_cal,"REG_DATA_TYPE","NA")
        summary_cal = my_cat_imputer(summary_cal,"REG_CUST_GENDER","NA")
        summary_cal = my_cat_imputer(summary_cal,"REG_CUST_BIRTHDATE",0.0)
        summary_cal = my_cat_imputer(summary_cal,"REG_CUST_MATERIAL_STATUS","NA")
        summary_cal = my_cat_imputer(summary_cal,"REG_CUST_INCOMES",0.0)
        summary_cal = my_cat_imputer(summary_cal,"REG_CUST_SCHOOL",-1.0)
        
        return summary_cal

def prepaid(bqs,gcps_path_in,summary_cal,train_test):
    
    float_vars = ['HAS_PREPAID',
         'ppd_recency',
         'ppd_sum_money',
         'ppd_mean_money',
         'ppd_min_money',
         'ppd_max_money',
         'ppd_first_activation']
    float_dict = dict.fromkeys(float_vars, 'float')
    summary_cal = summary_cal.astype(float_dict)

    summary_cal["ACTIVO_PREPAID"] = np.where(summary_cal.ppd_first_activation.isna(),0.0,1.0)
    
    summary_cal["ppd_recency"] = np.where(summary_cal.ppd_recency < summary_cal.ppd_first_activation, summary_cal.ppd_first_activation, summary_cal.ppd_recency)
    
    summary_cal["ppd_recency"] = np.where(summary_cal.ppd_recency < 0, np.nan, summary_cal.ppd_recency)
    summary_cal["ACTIVO_PREPAID"] = np.where(summary_cal.ppd_first_activation < 0, 0.0, 1.0)
    summary_cal["ppd_first_activation"] = np.where(summary_cal.ppd_first_activation < 0, np.nan, summary_cal.ppd_first_activation)
    
    
    summary_cal = summary_cal.fillna({'HAS_PREPAID': FILL_FLOAT,
         'ppd_recency': FILL_RECENCY,
         'ppd_sum_money': FILL_FLOAT,
         'ppd_mean_money': FILL_FLOAT,
         'ppd_min_money': FILL_FLOAT,
         'ppd_max_money': FILL_FLOAT,
         'ppd_first_activation': FILL_RECENCY,
         'ACTIVO_PREPAID': FILL_FLOAT})
    
    return summary_cal


def compras_shipping(bqs,gcps_path_in,summary_cal,train_test):   
    
    float_vars = ['CANT_COMPRAS_CARRITO',
     'CPG',
     'vendedores_distintos_30',
     'vendedores_distintos_60_30',
     'vendedores_distintos_90_60',
     'vendedores_distintos_long',
     'DESKTOP_PURCHASE',
     'MOBILE_WEB_PURCHASE',
     'ANDROID_PURCHASE',
     'IOS_PURCHASE',
     'CANTIDAD_COMPRAS_ITEMS_NUEVOS',
     'CANTIDAD_COMPRAS_ITEMS_USADOS',
     'CANTIDAD_COMPRAS_FREE_SHIPPING',
     'CANTIDAD_COMPRAS_NO_ENVIOS',
     'CANTIDAD_COMPRAS_NO_CARRITO',
     'CANTIDAD_COMPRAS_ENVIO_DROP_OFF']
    
    str_vars = ['DISPOSITIVO_MAS_USADO']
    
    float_dict = dict.fromkeys(float_vars, 'float')
    float_fillna = dict.fromkeys(float_vars, FILL_FLOAT)
    str_dict = dict.fromkeys(str_vars, np.dtype('O'))
    
    summary_cal = summary_cal.astype(float_dict)
    summary_cal = summary_cal.astype(str_dict)
    
    summary_cal = summary_cal.fillna({'CANT_COMPRAS_CARRITO': FILL_FLOAT,
    'CPG': FILL_FLOAT,
    'vendedores_distintos_30': FILL_FLOAT,
    'vendedores_distintos_60_30': FILL_FLOAT,
    'vendedores_distintos_90_60': FILL_FLOAT,
    'vendedores_distintos_long': FILL_FLOAT,
    'DESKTOP_PURCHASE': FILL_FLOAT,
    'MOBILE_WEB_PURCHASE': FILL_FLOAT,
    'ANDROID_PURCHASE': FILL_FLOAT,
    'IOS_PURCHASE': FILL_FLOAT,
    'CANTIDAD_COMPRAS_ITEMS_NUEVOS': FILL_FLOAT,
    'CANTIDAD_COMPRAS_ITEMS_USADOS': FILL_FLOAT,
    'CANTIDAD_COMPRAS_FREE_SHIPPING': FILL_FLOAT,
    'CANTIDAD_COMPRAS_NO_ENVIOS': FILL_FLOAT,
    'CANTIDAD_COMPRAS_NO_CARRITO': FILL_FLOAT,
    'CANTIDAD_COMPRAS_ENVIO_DROP_OFF': FILL_FLOAT,
    'DISPOSITIVO_MAS_USADO': 'DESKTOP_PURCHASE'})
    
    return summary_cal

def sellers(bqs,gcps_path_in,summary_cal,train_test):
    
    float_vars = ['FIRST_SELLER','RECENCY_SELLER', 'SALES_SELLER']
    float_dict = dict.fromkeys(float_vars, 'float')
    summary_cal = summary_cal.astype(float_dict)
    
    summary_cal["FIRST_SELLER"] = summary_cal["FIRST_SELLER"].fillna(FILL_RECENCY)
    summary_cal["RECENCY_SELLER"] = summary_cal["RECENCY_SELLER"].fillna(FILL_RECENCY)
    summary_cal = summary_cal.fillna({'FIRST_SELLER':FILL_RECENCY,
                                     'RECENCY_SELLER':FILL_RECENCY,
                                     'SALES_SELLER':FILL_FLOAT})
    return summary_cal

def payments(bqs,gcps_path_in,summary_cal,train_test):
    
    float_vars = ['AVG_CUOTAS', 'MAX_CUOTAS', 'CSI', 'CCI', 'SUM_TICKET', 'SUM_DINERO_CUENTA', 'SUM_TD',
       'SUM_TC']
    
    float_dict = dict.fromkeys(float_vars, 'float')
    summary_cal = summary_cal.astype(float_dict)
    
    summary_cal = summary_cal.fillna({'AVG_CUOTAS': FILL_FLOAT,
                                      'MAX_CUOTAS': FILL_FLOAT,
                                      'CSI': FILL_FLOAT,
                                      'CCI': FILL_FLOAT,
                                      'SUM_TICKET': FILL_FLOAT,
                                      'SUM_DINERO_CUENTA': FILL_FLOAT,
                                      'SUM_TD': FILL_FLOAT,
                                      'SUM_TC': FILL_FLOAT})
    
    return summary_cal


def tarjetas(bqs,gcps_path_in,summary_cal,train_test):
    
    float_vars = ['DEBIT_PAY', 'CREDIT_PAY', 'ACCOUNT_PAY', 'TURISTA', 'TIPO_TARJETA', 'PREPAID']
    float_dict = dict.fromkeys(float_vars, 'float')
    summary_cal = summary_cal.astype(float_dict)
    
    summary_cal = summary_cal.fillna({'DEBIT_PAY': FILL_FLOAT,
                                      'CREDIT_PAY': FILL_FLOAT,
                                      'ACCOUNT_PAY': FILL_FLOAT,
                                      'TURISTA': FILL_FLOAT,
                                      'TIPO_TARJETA': FILL_FLOAT,
                                      'PREPAID': FILL_FLOAT})
    
    return summary_cal


def asset_mgm(bqs,gcps_path_in,summary_cal,train_test):
    
    summary_cal['asset_mgmt_stat_id'] = summary_cal['asset_mgmt_stat_id'].astype(np.dtype('O'))
    
    return summary_cal


def install_ml(bqs,gcps_path_in,summary_cal,train_test):

    float_vars = ['days_last_install_ml','days_first_install_ml']

    str_vars = ['tipo_device']

    float_dict = dict.fromkeys(float_vars, 'float')
    str_dict = dict.fromkeys(str_vars, np.dtype('O'))

    summary_cal = summary_cal.astype(float_dict)

    summary_cal = summary_cal.fillna({'days_last_install_ml': FILL_RECENCY,
                   'days_first_install_ml': FILL_RECENCY,
                   'tipo_device': 'OTHER_DEVICE'})
    
    summary_cal = summary_cal.astype(str_dict)
    
    summary_cal["reinstall_ml"] = np.where(summary_cal.days_last_install_ml > summary_cal.days_first_install_ml,1.0,0.0)
    return summary_cal


def install_mp(bqs,gcps_path_in,summary_cal,train_test):

    float_vars = ['days_last_install_mp','days_first_install_mp']


    float_dict = dict.fromkeys(float_vars, 'float')

    summary_cal = summary_cal.astype(float_dict)

    summary_cal = summary_cal.fillna({'days_last_install_mp': FILL_RECENCY,
                   'days_first_install_mp': FILL_RECENCY})
    
    summary_cal["reinstall_mp"] = np.where(summary_cal.days_last_install_mp > summary_cal.days_first_install_mp,1,0)
    return summary_cal

def target(bqs,gcps_path_in,summary_cal,train_test):
    
    float_vars = ['FREQ_TARGET','GMV_TARGET']
    float_dict = dict.fromkeys(float_vars, 'float')
    summary_cal = summary_cal.astype(float_dict)
    summary_cal = summary_cal.fillna({'FREQ_TARGET': FILL_FLOAT,
                   'GMV_TARGET': FILL_FLOAT})
    
    summary_cal = summary_cal.rename(columns = {"FREQ_TARGET":"frequency_eval"})
    
    return summary_cal

def generate_clusters(bqs,gcps_path_in,summary_cal,train_test):
    
    if(train_test == "train_train"): # por si lo meto en el pipeline de test
        
        
        kmeans = Pipeline([('scale', StandardScaler()), ('kmeans', KMeans(n_clusters = N_CLUSTERS,n_jobs=20))])
        kmeans = kmeans.fit(summary_cal[["freq_D_long","my_first_in_windows","my_recency"]])

        
        with open("clusters.pkl", 'wb') as file:
            pickle.dump(kmeans, file)

        bqs.upload_file('clusters.pkl',gcps_path_out + 'clusters.pkl')
        summary_cal["cluster"] = kmeans.predict(summary_cal[["freq_D_long","my_first_in_windows","my_recency"]])
        
    if(train_test in ["test","train"]): # asumo que ya existe el objecto cluster, sino pasarle train_train para fitear en nuevo sites
        bqs.download_file(gcps_path_out + 'clusters.pkl','clusters.pkl')
        with open("clusters.pkl", 'rb') as file:
            kmeans = pickle.load(file)
            
        summary_cal["cluster"] = kmeans.predict(summary_cal[["freq_D_long","my_first_in_windows","my_recency"]])
    
    return summary_cal


def my_cat_imputer(x,col,impute_search):
    x[col] = x[col].fillna(impute_search)
    aux = x.loc[x[col] != impute_search][[col]]
    ws = aux[col].value_counts()/aux.shape[0]
    x.loc[x[col] == impute_search,col] = random.choices( population=ws.index,weights=ws.values, k= x.shape[0] - aux.shape[0] )
    return x