import pandas as pd
from melitk.analytics.connectors.core.authentication import Authentication
from melitk.analytics.connectors.teradata import ConnTeradata
from melitk.analytics.connectors.presto import ConnPresto
from melitk.analytics.connectors.hive import ConnHive
import boto3
import os
import numpy as np

def _write_dataframe_to_csv_on_s3(df, path_s3,sep = "|"):
    import boto3
    from io import StringIO
    """ Write a dataframe to a CSV on S3 """
    a = path_s3.split('//')
    b = a[1].split('/')
    bucket = b[0]
    c = path_s3.split(bucket+'/')
    path = c[1]
    
    buffer = StringIO()
    df.to_csv(buffer,index=False,sep = sep)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, path).put(Body=buffer.getvalue())
    return None

def read_csv_from_s3(path,sep = "|",error=False,shape = False):
    import boto3
    import pandas as pd
    import os

    basedir = os.path.dirname(os.path.abspath(__file__))
    
    bucket = path.split('/')[2]
    resto = '/'.join(path.split('/')[3:])

    s3_resource = boto3.resource("s3")
    s3_resource.Object(bucket, resto).download_file(os.path.join(basedir, "aux.csv")) # Python 3.6+
    if error == True:
        df = pd.read_csv(os.path.join(basedir, "aux.csv"),sep = sep, engine='python',error_bad_lines=False)
    else:
        df = pd.read_csv(os.path.join(basedir, "aux.csv"),sep = sep)
    
    if(shape == True):
        print("Shape of {}: {}".format( path.split('/')[-1],df.shape))
              
    os.remove(os.path.join(basedir, "aux.csv"))
    return df

def read_pickle_csv(path):
    import boto3
    import pandas as pd
    import os
    s3 = boto3.client('s3')
    bucket = path.split('/')[2]
    resto = '/'.join(path.split('/')[3:])
    s3.download_file(bucket, resto,"aux.pkl")
    pp = pd.read_pickle("aux.pkl")
    os.remove("aux.pkl")
    return pp

def dummy_etl(df = pd.DataFrame([])):
    import pickle
    from melitk.fda import workspace
    import pandas as pd
    
    if(df.shape[0] == 0):
        df = pd.DataFrame([1,2,4,5],columns = ["hola"])
        
    serialized_dataset = pickle.dumps(df)
    workspace.save_etl_file("traspaso", serialized_dataset)
    
def dummy_train(save_metrics = True,save_model = True):
    import pickle
    from melitk.fda import workspace
    serialized_dataset = workspace.load_etl_file("traspaso")
    dataset = pickle.loads(serialized_dataset)  # Because in the ETL we pickled the pandas dataframe
    
    if(save_model == True):
        workspace.save_raw_model(serialized_dataset)
    
    if(save_metrics == True):
        metrics_dict = {
            'train_metrics': 0.7,
            'test_metrics': 0.1
        }
        workspace.save_metrics(metrics_dict)
    return dataset

def check_connection(teradata_user, teradata_pass,tera=None,type_con = "ldap",conector ="teradata"):
    """
        Funcion que checkea si no se perdio la conexion con teradata,presto o hive. En caso de
        perderse, se reincia la conexion.
        Args:
        - tera: conexion existente. En caso de None, se conececta por primera vez
        - type_con: 'ldap' o 'app'. Aplica para teradata. En caso de presto o hive no importa
        - conector: 'teradata','presto','hive'
    """
    try:
        tera.execute("select CURRENT_DATE")
    except:
        if(conector == "teradata"):
            if(type_con == "app"):
                tera = ConnTeradata(teradata_user, teradata_pass, auth_method=Authentication.APP)
            else:
                tera = ConnTeradata(teradata_user, teradata_pass, auth_method=Authentication.LDAP)
                
        if(conector == "presto"):
            tera = ConnPresto(teradata_user, teradata_pass)
            
        if(conector == "hive"):
            tera = ConnHive(teradata_user, teradata_pass)
    return tera

def drop_table(table,tera):
    try:
        tera.execute("drop table "+table)
    except:
        pass
    
    
def my_download_file(s3_path,file_name = None):
    import boto3
    import pandas as pd
    import os

    bucket = s3_path.split('/')[2]
    resto = '/'.join(s3_path.split('/')[3:])

    s3_resource = boto3.resource("s3")
    if(file_name == None):
        file = s3_path.split('/')[-1]
    else:
        file = file_name
        
    s3_resource.Object(bucket, resto).download_file(file) # Python 3.6+
    
    
def my_upload_file(file_name,s3_path):
    import boto3
    import pandas as pd
    import os

    bucket = s3_path.split('/')[2]
    resto = '/'.join(s3_path.split('/')[3:])
    
    s3 = boto3.client('s3')
    s3.upload_file(file_name,bucket, resto)
    
def subir_pandas_dataframe_a_teradata(df_, nombre_tabla, col_indice, cols_varchar, tera, tabla_preexiste=False):
    """Sube un dataframe a una tabla en Teradata, que puede o no preexistir.
    df: dataframe a subir
    nombre_tabla: nombre de la tabla en Teradata donde se subirán los datos
    col_indice: la columna índice que tiene la tabla (o tendrá si esta no fue creada). Se usa sólo si la tabla en 
    Teradata no preexiste.
    col_varchar: un diccionario donde las claves son los nombres de las columnas de la tabla en teradata (que deben 
    coincidir exactamente con las del dataframe) y los valores son el tipo de dato seguido (si existe) de la 
    longitud del campo correspondiente (ver ejemplo debajo). Si le pasas None, te crea todas las columnas com varchar
    tera: conexión a Teradata
    tabla_preexiste: indica si la tabla en Teradata a la que se subirán los datos del dataframe preexiste o no. Si no
    preexiste, la función la crea automáticamente. 
    Ejemplo: si nombre_tabla = 'MKT_CORP.TABLA_PRUEBA', cols_varchar={'col1':'VARCHAR(10)','col2':'DECIMAL(10,5)',
    'col3':'INTEGER'} y col_indice='col1', sube el dataframe df a una tabla en teradata de nombre 
    'MKT_CORP.TABLA_PRUEBA' con columnas 'col1', 'col2' y 'col3' que son de tipo VARCHAR(10) y DECIMAL(10,5) e INTEGER
    respectivamente, usando 'col1' como índice.
    """
    import os
    
    df = df_.copy()
    df.columns = [x.replace(' ','__') for x in df.columns]
    # Creo la query para crear la tabla y casteo todas las columnas de ser necesario
    str_col_variables = ''

    if(cols_varchar == None): 
        for col in df.columns:
            str_col_variables = str_col_variables + '{} {},'.format(col, "varchar(200)")
    else:
        for col in df.columns:
            tipo_variable = cols_varchar[col.replace(' ','__')]
            if('int' in tipo_variable.lower()):
                df[col] = pd.to_numeric(df[col],errors = "coerce",downcast = "integer")
            if('varchar' in tipo_variable.lower()):
                if(pd.__version__ > '1.0'):
                    df[col] = df[col].astype("string")
                else:
                    df[col] = df[col].astype(str, skipna = True).replace({"nan":np.nan})
            if('decimal' in tipo_variable.lower()):
                n = int(tipo_variable.split(",")[-1].replace(")",''))
                df[col] = pd.to_numeric(df[col],errors = "coerce",downcast = "integer")
                df[col] = df[col].apply(lambda x: round(x,n))
            if('date' in tipo_variable.lower()):
                df[col] = pd.to_datetime(df[col],infer_datetime_format=True,errors = "coerce")

            str_col_variables = str_col_variables + '{} {},'.format(col, tipo_variable)

    if(tabla_preexiste == False): # Creo la tabla
        str_col_variables = str_col_variables[:-1]
        query_crear_tabla = """
                                CREATE MULTISET TABLE {},
                                 NO BEFORE JOURNAL,
                                 NO AFTER JOURNAL,
                                 CHECKSUM = DEFAULT,
                                 DEFAULT MERGEBLOCKRATIO ({}) PRIMARY INDEX ({})
                            """.format(nombre_tabla, str_col_variables, col_indice)
        tera.execute(query_crear_tabla)
       
    filename_csv_temporal = 'df_temporal_para_subir_a_teradata.csv'
    df.to_csv(filename_csv_temporal, index=False, sep=';')
    tera.fast_load(file_path=filename_csv_temporal, table=nombre_tabla, sep=';')
    os.remove(filename_csv_temporal)
    
def subir_pandas_dataframe_a_hive(df_, nombre_tabla, path_tabla_hive, cols_varchar, hive):
    """Sube un dataframe a una tabla en hive, que debe no existir.
    df: dataframe a subir
    nombre_tabla: nombre de la tabla
    col_varchar: un diccionario donde las claves son los nombres de las columnas de la tabla en teradata (que deben 
    coincidir exactamente con las del dataframe) y los valores son el tipo de dato seguido (si existe) de la 
    longitud del campo correspondiente (ver ejemplo debajo). Si le pasas None, te crea todas las columnas com varchar
    tera: conexión a Teradata
    Ejemplo: si nombre_tabla = 'MKT_INSIGHTS.TABLA_PRUEBA', cols_varchar={'col1':'VARCHAR(10)','col2':'DECIMAL(10,5)',
    'col3':'INTEGER'} , sube el dataframe df a una tabla en hive de nombre 
    'MKT_INSIGHTS.TABLA_PRUEBA' con columnas 'col1', 'col2' y 'col3' que son de tipo VARCHAR(10) y DECIMAL(10,5) e INTEGER
    respectivamente
    """
    
    import os
    
    path_tabla_hive = "MKT/"+path_tabla_hive
    df = df_.copy()
    df.columns = [x.replace(' ','__') for x in df.columns]
    # Creo la query para crear la tabla y casteo todas las columnas de ser necesario
    str_col_variables = ''

    if(cols_varchar == None): 
        for col in df.columns:
            str_col_variables = str_col_variables + '{} {},'.format(col, "varchar(200)")
    else:
        for col in df.columns:
            tipo_variable = cols_varchar[col.replace(' ','__')]
            if('int' in tipo_variable.lower()):
                df[col] = pd.to_numeric(df[col],errors = "coerce",downcast = "integer")
            if('varchar' in tipo_variable.lower()):
                if(pd.__version__ > '1.0'):
                    df[col] = df[col].astype("string")
                else:
                    df[col] = df[col].astype(str, skipna = True).replace({"nan":np.nan})
            if('string' in tipo_variable.lower()):
                if(pd.__version__ > '1.0'):
                    df[col] = df[col].astype("string")
                else:
                    df[col] = df[col].astype(str, skipna = True).replace({"nan":np.nan})
            if('decimal' in tipo_variable.lower()):
                n = int(tipo_variable.split(",")[-1].replace(")",''))
                df[col] = pd.to_numeric(df[col],errors = "coerce",downcast = "integer")
                df[col] = df[col].apply(lambda x: round(x,n))
            if('date' in tipo_variable.lower()):
                df[col] = pd.to_datetime(df[col],infer_datetime_format=True,errors = "coerce")

            str_col_variables = str_col_variables + '{} {},'.format(col, tipo_variable)


    str_col_variables = str_col_variables[:-1]
    query_crear_tabla = """
                            CREATE EXTERNAL TABLE """+nombre_tabla+"""(
                                """+str_col_variables+"""
                               )
                            ROW FORMAT SERDE
                             'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
                            WITH SERDEPROPERTIES (
                             'field.delim'='\;',
                             'line.delim'='\n',
                             'serialization.format'='\;')
                            STORED AS INPUTFORMAT
                             'org.apache.hadoop.mapred.TextInputFormat'
                            OUTPUTFORMAT
                             'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
                            LOCATION
                             's3://melilake.sandbox/"""+path_tabla_hive+"""'
                            TBLPROPERTIES (
                             'skip.header.line.count'='1',
                             'transient_lastDdlTime'='1571146865')
                        """.format( str_col_variables)
    
    df.to_csv("to_hive.csv",index=False,sep=";")
    sesion = boto3.session.Session()
    client = sesion.client('s3',aws_access_key_id = "",  aws_secret_access_key = "" )
    client.upload_file("to_hive.csv", "melilake.sandbox",path_tabla_hive+"/to_hive.csv")
    
    hive.execute(query_crear_tabla)
    
class connection:
    """
        Esta clase es el rejunte de todas las funciones de conexion para presto, hive y teradata
        Antes de hacer cualquier consulta revisa la conexion
    """
    def __init__(self,user,pas,conector = "teradata",type_con="ldap"):
        self.user = user
        self.pas = pas
        self.conector = conector
        self.type_con = type_con
        
        self.con = check_connection(self.user,self.pas,None,type_con = self.type_con,conector=self.conector)
        
    def drop_table(self,tbl_name):
        self.con = check_connection(self.user,self.pas,self.con,type_con = self.type_con,conector=self.conector)
        drop_table(tbl_name,self.con)
    
    def execute(self,query):
        self.con = check_connection(self.user,self.pas,self.con,type_con = self.type_con,conector=self.conector)
        return self.con.execute(query)
    
    def execute_response(self,query):
        self.con = check_connection(self.user,self.pas,self.con,type_con = self.type_con,conector=self.conector)
        return pd.DataFrame(self.con.execute_response(query))
    
    def fast_export(self,query,s3_path,sep="|"):
        self.con = check_connection(self.user,self.pas,self.con,type_con = self.type_con,conector=self.conector)
        if(self.conector == "teradata"):
            self.con.fast_export(query,s3_path,sep)
        elif(self.conector in ["presto","hive"]):
            pd.DataFrame(self.con.execute_response(query)).to_csv("aux.csv",index=False,sep=sep)
            my_upload_file("aux.csv",s3_path)
            os.remove("aux.csv")
            print("Exported to:",s3_path)
        else:
            print("No fast export for conector:",self.conector)
    
    def fast_load(self,file_path, table, sep=';'):
        self.con.fast_load(file_path=file_path, table=table, sep=sep)
            

def prints3(*kargs,log_path='',silent = False):
    """
    Funcion simil a print comun pero con agregado de escribir en s3 y mostrar resultados en stdout
    log_path: path en s3 donde guardar el log
    silent: si vale True no printea en stdout
    """
    msg = ' '.join([str(x) for x in kargs])
    file1 = open("log.txt","a")#append mode 
    file1.write(msg) 
    file1.write("\n") 
    file1.close()

    if(silent == False):
        print(msg)
    
    if(log_path != ''):
        my_upload_file("log.txt",log_path)    
        

def spark_connect(spark_user, spark_pass, auth="LDAP"):
    """Crea y devuelve conexión Spark, donde:
    sparh_user / spark_pass: usuario y pass de Spark
    auth: por ahora solo LDAP (no aplicativos)."""
    from impala.dbapi import connect
    if auth == "LDAP":
        auth_mechanism = "PLAIN"
        
    spark_host = "internal-md-spark-sql-cluster-1051871508.us-east-1.elb.amazonaws.com"
    spark_port = 10001
    spark_db = "default"

    connection = connect(spark_host, port=spark_port, user=spark_user, password=spark_pass, 
                         database=spark_db, auth_mechanism=auth_mechanism)
#     return connection
    return connection.cursor()

def spark_execute(spark, query, reintentos=1, close_connection=False, verbose=False):
    """Ejecuta query en spark y devuelve el resultado en un DataFrame
    spark: objeto de conexión de Spark (ver spark_connect)
    query: query a ejecutar
    reintentos: reintentos a la hora de ejecutar la query
    close_conection (bool): para cerrar o no la conexión luego de que se devuelve el resultado
    verbose (bool): imprime progreso"""
    import pandas as pd
    
    if verbose:
        print('Query a ejecutar en Spark =  \n {}'.format(query))
    

    for i in range(reintentos):
        try:
            spark.execute(query)
            res = spark.fetchall()
#             cursor = spark.cursor()
#             cursor.execute(query)
#             res = cursor.fetchall()
#             res = pd.read_sql(query, spark)
        except Exception as e:
            if i < reintentos - 1:
                if verbose:
                    print('Hubo un error, reintentando...')
                continue
            else:
                raise e
        break

    if close_connection:
        cursor.close()
    
    return res

def AB_grupos(grupo,size,frac=None):
    """
    Te arma los grupos de control y test. Cada grupo tiene la misma cantidad de muestras, 
    a menos que especifiques en frac (fraccion del grupo[0] que queres que haya)
    
    SHUFLEAR ANTES DE USAR
    
    Forma de uso: 
            df = df.sample(frac=1.)
            df["GRUPO"] = AB_grupos(["TG","CG"],df.shape[0],frac=[0.5,0.5])  
                ===
            df["GRUPO"] = AB_grupos(["TG","CG"],df.shape[0])
    """
    if(frac == None):
        frac = [1/len(grupo) for x in grupo]
        
    size_grupo = int(size/len(grupo))
    l_ = []

    for s,g in zip(frac,grupo[:-1]):
        l_ += int(s*size)*[g]
    l_ += (size-len(l_))*[grupo[-1]]
    return l_


def write_dependencies(site,bu,iniciativa,fecha):
    import os
    df_dep = read_csv_from_s3("s3://fury-data-apps/ltv-ml/dependencies.csv")

    if(df_dep.loc[(df_dep.SITE == site) & (df_dep.BU == bu) & (df_dep.INICIATIVA == iniciativa)].shape[0] == 0):
        aux = pd.DataFrame([{"SITE":site,"BU":bu,"INICIATIVA":iniciativa,"FECHA_END":fecha}])[["SITE","BU","INICIATIVA","FECHA_END"]]
        df_dep = df_dep.append(aux) # nueva iniciativa

    else:
        df_dep.loc[(df_dep.SITE == site) & (df_dep.BU == bu) & (df_dep.INICIATIVA == iniciativa),"FECHA_END"] = fecha

    _write_dataframe_to_csv_on_s3(df_dep,"s3://fury-data-apps/ltv-ml/dependencies.csv")

    
def check_dependencies(site,bu,iniciativa):
    df_dep = read_csv_from_s3("s3://fury-data-apps/ltv-ml/dependencies.csv")
    return df_dep.loc[(df_dep.SITE == site) & (df_dep.BU == bu) & (df_dep.INICIATIVA == iniciativa)]["FECHA_END"].values[0]


class Substatus(object):

    def __init__(self, credential, mail_to="maximo.ripani@mercadolibre.com,pedro.zenone@mercadolibre.com"):        
        self.credential = credential
        self.mail_to = mail_to
    
    def get_tabla(self, PHOTO_ID = '*'):
        """
        Obtiene la tabla completa o para un PHOTO_ID especifico
        """

        # Instancio BigQuery
        bqte = BigQueryTransferEtl(self.credential)
        bqte.get_gcp_credential() # Setup service account credential for the project
        bqte.create_clients()  # Create the BigQuery clients used in the project
        
        if PHOTO_ID == '*':
            Q = """
                SELECT *
                FROM `meli-marketing.MODELLING.JOBS_MONITOR` 
                """
            bqQuery = bqte.bqClient.query(Q)
            df = bqQuery.to_dataframe()
        else:
            Q = f"""
                SELECT *
                FROM `meli-marketing.MODELLING.JOBS_MONITOR` 
                WHERE PHOTO_ID = DATE('{PHOTO_ID}')
                """
            bqQuery = bqte.bqClient.query(Q)
            df = bqQuery.to_dataframe()
        
        return df



    def BQ_update_time(self, mode, time, SITE, BU, INICIATIVA, JOB, PHOTO_ID):
        """
        BIGQUERY
        Updatea el START o END time del proceso
        mode: START/END
        time: i.e 2021-04-07 08:00:00
        """

        # Instancio BigQuery
        bqte = BigQueryTransferEtl(self.credential)
        bqte.get_gcp_credential() # Setup service account credential for the project
        bqte.create_clients()  # Create the BigQuery clients used in the project
            
        Q = """
                UPDATE `meli-marketing.MODELLING.JOBS_MONITOR`
                SET R_"""+mode.upper()+"""_DATE = TIMESTAMP('"""+time+"""')
                WHERE SITE = '"""+SITE+"""'
                AND INICIATIVA = '"""+INICIATIVA+"""'
                AND BU = '"""+BU+"""'
                AND JOB = '"""+JOB+"""'
                AND PHOTO_ID = DATE('"""+PHOTO_ID+"""')
                """

        BQ = bqte.bqClient.query(Q)
        
        
    def BQ_insert_time(self, time, SITE, BU, INICIATIVA, JOB, PHOTO_ID):
        """
        BIGQUERY
        Inserta el START de un proceso
        """
        # Instancio BigQuery
        bqte = BigQueryTransferEtl(self.credential)
        bqte.get_gcp_credential() # Setup service account credential for the project
        bqte.create_clients()  # Create the BigQuery clients used in the project
            
        # For INSERT
        Q = """
            INSERT `meli-marketing.MODELLING.JOBS_MONITOR` (SITE, BU, INICIATIVA, JOB,R_START_DATE, PHOTO_ID)
            VALUES('"""+SITE+"""', '"""+BU+"""', '"""+INICIATIVA+"""', '"""+JOB+"""', TIMESTAMP('"""+time+"""'),DATE('"""+PHOTO_ID+"""'))
            """
        bqQuery = bqte.bqClient.query(Q)
        


    def subscriber(self, mode, SITE, BU, INICIATIVA, JOB):
        """
        Suscribe a la tabla de BQ el END/START [mode] de un proceso
        """
        import pandas as pd
        from pytz import timezone
        from datetime import timedelta
        import datetime
            
        # ID del proceso
        PROCESS_ID = SITE+ '-' + BU + '-' + INICIATIVA + '-' + JOB
        
        # Fecha de hoy
        today = datetime.datetime.now(timezone('America/Argentina/Buenos_Aires'))
        
        mode = mode.lower()
        
        # IF MODE='START'
        if mode=='start':
            PHOTO_ID = today.date().strftime('%Y-%m-%d')

            # 1. Chequear si ese SITE-BU-INICIATIVA-JOB existe en la tabla para PHOTO_ID == TODAY
            A = self.get_tabla(PHOTO_ID)
            A = A.loc[((A.SITE + '-' + A.BU + '-' + A.INICIATIVA + '-' + A.JOB) == PROCESS_ID)]

            # Horario a inputar
            now = datetime.datetime.now(timezone('America/Argentina/Buenos_Aires')).strftime('%Y-%m-%d %H:%M:%S')

            # 1.1 Existe: UPDATE
            if not A.empty:
                # UPDATE
                self.BQ_update_time(mode, now, SITE, BU, INICIATIVA, JOB, PHOTO_ID)

            # 1.2 No existe: INSERT
            if A.empty:
                # INSERT
                self.BQ_insert_time(now, SITE, BU, INICIATIVA, JOB, PHOTO_ID)



        # IF MODE='END'
        if mode=='end':
            # Le resto 2 horas al horario por si algun proceso términa a las 2 de la mañana
            PHOTO_ID = (datetime.datetime.now(timezone('America/Argentina/Buenos_Aires')) - datetime.timedelta(hours=2)).date().strftime('%Y-%m-%d')
            # Horario a inputar
            now = datetime.datetime.now(timezone('America/Argentina/Buenos_Aires')).strftime('%Y-%m-%d %H:%M:%S')
            # UPDATE
            self.BQ_update_time(mode, now, SITE, BU, INICIATIVA, JOB, PHOTO_ID)




    def send_alert(self,subject,body,mode="mail"):
        import requests
        if mode=="mail":
            data = {"from": "maximo.ripani@mercadolibre.com",
                       "to": self.mail_to,
                       "subject": subject,
                       "body": subject
                      }
            url = "http://internal.mercadolibre.com/bi/send_mail"
            r = requests.post(url = url, json = data)

            
            
            
class BigQueryTransferEtl(object):
    
    def __init__(self, credential):
        self.gsCredentials = None
        self.bqClient = None
        self.gsClient = None
        self.credential = credential
        
    def get_gcp_credential(self):
        import json
        from google.oauth2 import service_account
        
        gcp_credentials = None
        self.gsCredentials = None

        try:
            gcp_credentials = self.credential
            gcp_credentials = json.loads(gcp_credentials, strict=False)
            self.gsCredentials = service_account.Credentials.from_service_account_info(gcp_credentials)
        except Exception as e:
            exception_message = "get_gcp_credential: {}".format(e)
            logger.info(exception_message)

        return
    
    def create_clients(self):
        """ Setup the BigQuery client needed in the ETL process
        """
        from google.cloud import bigquery
        from google.cloud import storage
        
        if self.gsCredentials is None:
            self.get_gcp_credential()
        
        bq_project = self.gsCredentials.project_id
        
        self.bqClient = bigquery.Client(project=bq_project,credentials=self.gsCredentials)
        self.gsClient = storage.Client(project=bq_project,credentials=self.gsCredentials)

    def getSchemaJson(self):
        from google.cloud import bigquery
        
        return [
            bigquery.SchemaField("KEYWORD","STRING",mode="NULLABLE"),
            bigquery.SchemaField("ORDEN","STRING",mode="NULLABLE"),
            bigquery.SchemaField("SIMILARITIES","STRING",mode="NULLABLE"),
            bigquery.SchemaField("CAT_CATEG_ID_L1","STRING",mode="NULLABLE"),
            bigquery.SchemaField("CAT_CATEG_NAME_L1","STRING",mode="NULLABLE"),
            bigquery.SchemaField("CAT_CATEG_ID_L7","STRING",mode="NULLABLE"),
            bigquery.SchemaField("CAT_CATEG_NAME_L7","STRING",mode="NULLABLE"),
            bigquery.SchemaField("SIT_SITE_ID","STRING",mode="NULLABLE")
        ]


    def runStorageImport(self,import_table_id, storage_file_uri, delimiter, quote):
        from google.cloud import bigquery
        
        #dataset_ref = bqClient.dataset(dataset)
        job_config = bigquery.LoadJobConfig()
        job_config.schema = self.getSchemaJson()
        job_config.skip_leading_rows=1
        job_config.max_bad_records=0

        job_config.ignore_unknown_values=True

        #job_config.labels
        job_config.write_disposition='WRITE_TRUNCATE'
        job_config.create_disposition='CREATE_IF_NEEDED'
        #job_config.source_format =  bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        job_config.source_format = bigquery.SourceFormat.CSV

        load_job = self.bqClient.load_table_from_uri(
            storage_file_uri, import_table_id, job_config=job_config
        ) 
        
        #print("Starting job {}".format(load_job.job_id))
        logger.info("Starting job {}".format(load_job.job_id))
        
        try:
            load_job.result()
        except Exception as e:
            exception_message = "runStorageImport: {}".format(load_job.errors)
            logger.info(exception_message)
            #print(load_job.errors)

        logger.info("Job finished")

        try:
            import_target_table = self.bqClient.get_table(import_table_id)
            logger.info("Loaded {} rows into target {}".format(import_target_table.num_rows, import_table_id))
        except Exception as e:
            exception_message = "runStorageImport: {}".format(e)
            logger.info(exception_message)


    def show_blob_metadata(blob_object):
        """Prints out a blob's metadata."""
        # bucket_name = 'your-bucket-name'
        # blob_name = 'your-object-name'

        #storage_client = storage.Client()
        #bucket = storage_client.bucket(bucket_name)
        #blob = bucket.get_blob(blob_name)

        print("Blob: {}".format(blob_object.name))
        print("Bucket: {}".format(blob_object.bucket.name))
        print("Storage class: {}".format(blob_object.storage_class))
        print("ID: {}".format(blob_object.id))
        print("Size: {} bytes".format(blob_object.size))
        print("Updated: {}".format(blob_object.updated))
        print("Generation: {}".format(blob_object.generation))
        print("Metageneration: {}".format(blob_object.metageneration))
        print("Etag: {}".format(blob_object.etag))
        print("Owner: {}".format(blob_object.owner))
        print("Component count: {}".format(blob_object.component_count))
        print("Crc32c: {}".format(blob_object.crc32c))
        print("md5_hash: {}".format(blob_object.md5_hash))
        print("Cache-control: {}".format(blob_object.cache_control))
        print("Content-type: {}".format(blob_object.content_type))
        print("Content-disposition: {}".format(blob_object.content_disposition))
        print("Content-encoding: {}".format(blob_object.content_encoding))
        print("Content-language: {}".format(blob_object.content_language))
        print("Metadata: {}".format(blob_object.metadata))
        print("Custom Time: {}".format(blob_object.custom_time))
        print("Temporary hold: ", "enabled" if blob_object.temporary_hold else "disabled")
        print(
            "Event based hold: ",
            "enabled" if blob_object.event_based_hold else "disabled",
        )
        if blob_object.retention_expiration_time:
            print(
                "retentionExpirationTime: {}".format(
                    blob_object.retention_expiration_time
                )
            )
            
            
    def check_bucket_upload(self, gs_bucket_name, destination_file_name, show_metadata=False):
        """ Upload File Storage subfunction.
        Check if the uploaded file exists. 
        params:
        gsClient = Google Storage Client with credentials and project defined
        gs_bucket_name = "name of google storage bucket"
        destination_file_name = "storage_object_name"
        """
        bucket_blob_name = ''  # Nombre del archivo subido al bucket de Google Storage
        try:
            gs_bucket = self.gsClient.get_bucket(gs_bucket_name)
            gs_blob = gs_bucket.get_blob(destination_file_name)
            bucket_blob_name = gs_blob.name
            
            logger.info("File Upload Successful: {}".format(bucket_blob_name))

            if show_metadata:
                show_blob_metadata(gs_blob)

        except Exception as e:
            exception_message = "check_bucket_upload: {}".format(e)

        return 

    def upload_file_storage(self, gs_bucket_name, source_file_name, destination_file_name):
        """ Upload file to Google Storgae Bucket 
        params:
        gsClient = Google Storage Client with credentials and project defined
        gs_bucket_name = "name of google storage bucket"
        source_file_name = "local/path/to/file"
        destination_file_name = "storage_object_name"
        """
        gs_bucket = self.gsClient.bucket(gs_bucket_name)
        gs_blob = gs_bucket.blob(destination_file_name)

        try:
            gs_blob.upload_from_filename(source_file_name)
            self.check_bucket_upload(gs_bucket_name, destination_file_name, show_metadata=False)
        except Exception as e:
            exception_message = "upload_file_storage: {}".format(e)
