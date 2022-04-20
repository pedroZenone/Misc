from scipy import stats
from scipy.stats import power_divergence
import pandas as pd 
import numpy as np
np.seterr(invalid='ignore')
import joblib
import warnings
import pickle
from scipy import stats
import os
import requests
import matplotlib.pyplot as plt



from evidently.dashboard import Dashboard
from evidently.dashboard.tabs import (
    DataDriftTab,
    CatTargetDriftTab
)

from evidently.pipeline.column_mapping import ColumnMapping
from evidently.dashboard.tabs import DataDriftTab, NumTargetDriftTab

from evidently.model_profile import Profile
from evidently.model_profile.sections import DataDriftProfileSection, NumTargetDriftProfileSection

from base64 import b64decode
import sys
sys.path.append(os.path.dirname(os.path.expanduser(".")))
sys.path.append(os.path.dirname(os.path.expanduser("..")))
sys.path.append("../.")
from google_cloud import Storage

AUTH_BIGQUERY = b64decode(os.environ['SECRET_AUTH_BIGQUERY_MODEL'])
NUMERIC_DTYPES = [np.dtype('float32'),np.dtype('float64'), np.dtype('int32'),np.dtype('int64')]
RANDOM_STATE = 42
THRESHOLD_CONTINUOUS = 2
THRESHOLD_CATEGORICAL = 0.05



class Drift(object):

    def __init__(self,setpoint_df,apply_df,target = None):        
        self.setpoint_df = setpoint_df
        self.apply_df = apply_df

        self.predictions_setpoint = None
        self.predictions_apply = None

        self.categorical_variables = None
        self.continuous_variables = None

        self.continuous_scores = None
        self.categorical_scores = None

        self.target = target

        self.drifted_continuous = None
        self.drifted_categorical = None
        
        self.report_url = None
    
    def getReportUrl(self):
        return self.report_url
    
    def getCategoricalVariables(self):
        return self.categorical_variables
    
    def getContinuousVariables(self):
        return self.continuous_variables
    
    def getContinuousScores(self):
        return self.continuous_scores
    
    def getCategoricalScores(self):
        return self.categorical_scores
    
    def getDriftedContinuous(self):
        return self.drifted_continuous
    
    def getDriftedCategorical(self):
        return self.drifted_categorical
    
    def getDriftedTotal(self):
        return self.drifted_continuous + self.drifted_categorical
    
    def remove_outliers_continuous(self):
        stddev = 3
        
#         self.setpoint_df = self.setpoint_df[
#     (np.abs(stats.zscore(self.setpoint_df.select_dtypes(include=NUMERIC_DTYPES))) < stddev).all(axis=1)
# ]

#         self.apply_df = self.apply_df[
#     (np.abs(stats.zscore(self.apply_df.select_dtypes(include=NUMERIC_DTYPES))) < stddev).all(axis=1)
# ]
        self.setpoint_df = self.setpoint_df[
    (np.abs(stats.zscore(self.setpoint_df.select_dtypes(include=NUMERIC_DTYPES))) < stddev)]

        self.apply_df = self.apply_df[
    (np.abs(stats.zscore(self.apply_df.select_dtypes(include=NUMERIC_DTYPES))) < stddev)]


    def apply_continuous(self):
        """
        Aplica Kolmogorov Smirnov, Test de medias,
        divergencia de Kulback Leibner para features continuos

        """
        # Store resultados 
        store = {"feature":[], "tt_pvalue":[], "tt_estadistico":[], "ks_pvalue":[], "ks_estadistico":[]}

        # Itero sobre los features
        for x in self.continuous_variables:
            #print(x)
            tt = stats.ttest_ind(self.setpoint_df[x], self.apply_df[x])
            ks = stats.ks_2samp(self.setpoint_df[x], self.apply_df[x])
            store["tt_pvalue"].append(tt.pvalue)
            store["tt_estadistico"].append(tt.statistic)
            store["feature"].append(x)
            store["ks_pvalue"].append(ks.pvalue)
            store["ks_estadistico"].append(ks.statistic)

        self.continuous_scores = pd.DataFrame(store)

    def apply_categorical(self):
        """
        Aplica divergencia de Cressie-Read a variables categoricas

        """    
        store = {"feature":[], "dif_weighted":[]}

        for x in self.categorical_variables:
            #print(x)
            C = pd.DataFrame(self.setpoint_df[x].value_counts(normalize=True)).reset_index()
            D = pd.DataFrame(self.apply_df[x].value_counts(normalize=True)).reset_index()

            aux = pd.merge(C, D, on = "index")

            # Variacion porcentual
            aux["diferencia"] = abs(aux.iloc[:,1] - aux.iloc[:,2])/ aux.iloc[:,2]

            #Variacion porcentual pesada por la proporcion de la variable en apply
            # Para evitar incurrir en asignarle una gran variacion a una categoria que tiene
            # muy pocas observaciones
            aux["diferencia_weighted_apply"] = abs(aux.iloc[:,1] - aux.iloc[:,2]) * aux.iloc[:,2]


            # Suma de variaciones porcentuales pesadas por las proporciones de las categorias en Apply
            dif_weight_sum = aux["diferencia_weighted_apply"].sum()

            store["feature"].append(x)
            store["dif_weighted"].append(dif_weight_sum)

        self.categorical_scores = pd.DataFrame(store)
    
    def set_variable_types(self):
        
        total_cols = list(self.setpoint_df.columns)
        if('CUS_CUST_ID' in total_cols): total_cols.remove('CUS_CUST_ID')
        
        # Evitar filtrar la variable target
        if(self.target in total_cols): total_cols.remove(self.target)
        
        self.continuous_variables = [x for x in total_cols if(self.setpoint_df.dtypes[x] in NUMERIC_DTYPES)]
        self.categorical_variables = [x for x in total_cols if(self.setpoint_df.dtypes[x] not in NUMERIC_DTYPES)]
    
    def apply_drift(self):
        self.set_variable_types()
        self.remove_outliers_continuous()
        self.apply_continuous()
        self.apply_categorical()

        final_cont = {}
        final_cate = {}
        # Media, desvio estandar y limites
        std_n = self.continuous_scores["tt_estadistico"].std() * THRESHOLD_CONTINUOUS
        mu = self.continuous_scores["tt_estadistico"].mean()
        upbound = mu + std_n
        downbound = mu - std_n

        # Filtro y guardo en store_check los nombres de los features que inspeccionar
        self.drifted_continuous = list(self.continuous_scores.loc[(self.continuous_scores["tt_estadistico"] > upbound) | (self.continuous_scores["tt_estadistico"] < downbound)].feature.values)

        # Filtro y guardo en store_check los nombres de los features que inspeccionar
        self.drifted_categorical = list(self.categorical_scores.loc[self.categorical_scores["dif_weighted"] > THRESHOLD_CATEGORICAL].feature.values)

    def generate_report(self,name = "drift_report.html"):

        column_mapping = ColumnMapping()

        column_mapping.target = self.target
        column_mapping.numerical_features = self.drifted_continuous
        column_mapping.categorical_features = self.drifted_categorical

        data_and_target_drift_dashboard = Dashboard(tabs=[DataDriftTab()])


        data_and_target_drift_dashboard.calculate(self.setpoint_df[self.getDriftedTotal()], 
                                                  self.apply_df[self.getDriftedTotal()])
        
        data_and_target_drift_dashboard.save(name)
    

    
    def plot_histogram(self,variable, binsize="auto", rotation = 0, title="Histograma"):
        """
        Plotea histograma o grafico de barras, dependiendo de si es categorica o continua
        """
        
        x_tr = self.setpoint_df[variable]
        x_te = self.apply_df[variable]
        if variable in self.categorical_variables:
            # Feature de prueba
            f, (ax1, ax2, ax3) = plt.subplots(1,3)
            f.autofmt_xdate(rotation=rotation)
            #plt.setp(rotation=[90,90,90])
            f.set_figwidth(20)
            data_tr = dict(x_tr.value_counts())
            data_te = dict(x_te.value_counts())
            x_tr.value_counts().plot(kind="bar", color="r", alpha=1, ax=ax1)
            x_te.value_counts().plot(kind="bar", color="g", alpha=1, ax=ax2)
            x_te.value_counts().plot(kind="bar", color="g", alpha=1, ax=ax3)
            x_tr.value_counts().plot(kind="bar", color="r", alpha=0.6, ax=ax3)

            f.suptitle(f"{title}", y=1.1, fontsize=16)
            ax1.set_title("Train")
            ax2.set_title("Test")
            ax3.set_title("Train/Test")
            ax3.legend()
            
        else:
            # Calculo bin size 
            if binsize=="auto":
                y, x = np.histogram(x_tr, bins="auto")
                binsize = y.shape[0]
            
            # Feature de prueba
            f, (ax1, ax2, ax3) = plt.subplots(1,3)
            f.autofmt_xdate(rotation=rotation)
            #plt.setp(rotation=[90,90,90])
            f.set_figwidth(20)
            ax1.hist(x_tr, color="r", alpha=1, label="train", bins=np.arange(0,binsize,1), density=True)
            ax2.hist(x_te, color="g", alpha=1, label="test", bins=np.arange(0,binsize,1), density=True)
            ax3.hist(x_tr, color="r", alpha=0.4, label="train", bins=np.arange(0,binsize,1), density=True)
            ax3.hist(x_te, color="g", alpha=0.4, label="test", bins=np.arange(0,binsize,1), density=True )

            f.suptitle(f"{title}", y=1.1, fontsize=16)
            ax1.set_title("Train")
            ax2.set_title("Test")
            ax3.set_title("Train/Test")
            ax3.legend()
        
        return f

def send_mail(subject,body,mail,mode="mail"):
    if mode=="mail":
        data = {"from": "xx",
                   "to": mail,
                   "subject": subject,
                   "body": body
                  }
        url = "http://xx.xx.com/bi/send_mail"
        r = requests.post(url = url, json = data) 
