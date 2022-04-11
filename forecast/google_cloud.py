import os
class BigQuery():
    
    def __init__(self,credential):
        self.credential = credential
        self.gcCredentials = None
        self.bqClient = None
    
    def get_gcp_credential(self):
        """
        Obtengo las credenciales de Google Cloud.
        """
        
        import json
        from google.oauth2 import service_account
        
        gcp_credentials = None
        self.gcCredentials = None

        try:
            gcp_credentials = self.credential
            gcp_credentials = json.loads(gcp_credentials, strict=False)
            self.gcCredentials = service_account.Credentials.from_service_account_info(gcp_credentials)
        except Exception as e:
            exception_message = "get_gcp_credential: {}".format(e)
            print(exception_message)

        return
    
    
    def create_clients(self):
        """ 
        Creo el cliente para Google BigQuery
        """
        from google.cloud import bigquery
        
        if self.gcCredentials is None:
            self.get_gcp_credential()
        
        bq_project = self.gcCredentials.project_id
        
        self.bqClient = bigquery.Client(project=bq_project,credentials=self.gcCredentials)
    
    
    def create_table_from_q(self, query, table_id):
        """
        Subo resultados de una query a una tabla
        table_id: tabla destino
        query: query a ejecutar
        """
        from google.cloud import bigquery

        if self.gcCredentials is None:
            self.get_gcp_credential()
        
        if self.bqClient is None:
            self.create_clients()
            

        job_config = bigquery.QueryJobConfig(destination=table_id)

        # Start the query, passing in the extra configuration.
        query_job = self.bqClient.query(query, job_config=job_config)  # Make an API request.
        query_job.result()  # Wait for the job to complete.

        print("Query results loaded to the table {}".format(table_id))
    
    def drop_table(self, table_id):
        """
        Elimina la tabla proporcionada
        table_id: id de la tabla a eliminar 
        """

        from google.cloud import bigquery
        
        if self.gcCredentials is None:
            self.get_gcp_credential()
        
        if self.bqClient is None:
            self.create_clients()

        # If the table does not exist, delete_table raises
        # google.api_core.exceptions.NotFound unless not_found_ok is True.
        self.bqClient.delete_table(table_id, not_found_ok=True)  # Make an API request.
        print("Deleted table '{}'.".format(table_id))
        
    def subir_df(self, df, table_id, schema):
        """
        Subir pandas dataframe a bigquery
        df: datframe a subir
        table_id: id de la tabla en la que se cargara el dataframe 
        """
        from google.cloud import bigquery
        import time
        

        if self.gcCredentials is None:
            self.get_gcp_credential()
        
        if self.bqClient is None:
            self.create_clients()
        
        # BQ Schema
        TYPE = {'STRING':bigquery.enums.SqlTypeNames.STRING,
                'DATE':bigquery.enums.SqlTypeNames.DATE,
                'INTEGER':bigquery.enums.SqlTypeNames.INTEGER,
                'FLOAT':bigquery.enums.SqlTypeNames.FLOAT,
                'DATETIME':bigquery.enums.SqlTypeNames.DATETIME,
                'NUMERIC':bigquery.enums.SqlTypeNames.NUMERIC}
        
        SCHEMA = [bigquery.SchemaField(k, TYPE[v]) for k, v in schema.items()]
        
        job_config = bigquery.LoadJobConfig(
            schema=SCHEMA)
    
        # Subo a BigQuery
        print("Uploading to BigQuery")
        job = self.bqClient.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
            
    
    def upload_df_to_bigquery(self, df, table,schema):
        """
        Subir pandas dataframe a bigquery
        df: datframe a subir
        table: <proyecto>.<dataset>.<tabla> ejemplo: meli-marketing.TEMP45.PEPITO
        schema: {"col1":typeCol}   typeCols: STRING,DATE,INTEGER,FLOAT,DATETIME,NUMERIC
        """
        splits = table.split(".")
        if(len(splits) != 3):
            print("Formato erroneo de table: <proyecto>.<dataset>.<tabla>")
            return
        else:
            self.subir_df(df,splits[2],splits[1],schema)
            
    
    def upload_csv_to_bigquery(self,table,filename):
        from google.cloud import bigquery

        splits = table.split(".")
        if(len(splits) != 3):
            print("Formato erroneo de table: <proyecto>.<dataset>.<tabla>")
            return
        
        dataset_id = splits[1]
        table_id = splits[2]

        if self.gcCredentials is None:
            self.get_gcp_credential()
        
        if self.bqClient is None:
            self.create_clients()
            
        client = self.bqClient

        # tell the client everything it needs to know to upload our csv
        dataset_ref = client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = True

        # load the csv into bigquery
        with open(filename, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

        job.result()  # Waits for table load to complete.
        
            
    def export_table(self, destination, tabla, dataset='TEMP45',project = None):
        """
        Exporta tabla a bucket de GCP.
        destination: gs://bucket/algo
        tabla: nombre de la tabla que queremos exportar.
        """
        from google.cloud import bigquery
        import secrets

        if self.gcCredentials is None:
            self.get_gcp_credential()
        
        if self.bqClient is None:
            self.create_clients()

        if(project == None): # si no especifica que use el de la SA
            project = self.gcCredentials.project_id
            
        if((destination[:5] != "gs://") | (len(destination.split("/")) < 3)):
            print("Error de formato del bucket. Formato esperado: gs://bucket/algo")
            return
            
        # Nombre del bucket
        bucket_name = destination.split("/")[2]
        # Conjunto de datos donde se encuentra la tabla (En dataset temporales)
        dataset_id = dataset
        # Tabla a exportar con nombre random 
        table_id = tabla

        # URI del bucket 
        destination_uri = destination+"/*.parquet"
        dataset_ref = bigquery.DatasetReference(project, dataset_id)
        table_ref = dataset_ref.table(table_id)

        # Tipo de compresión
        job_config = bigquery.job.ExtractJobConfig()
        job_config.destination_format = bigquery.SourceFormat.PARQUET

        # Instanciamos extraccion
        extract_job = self.bqClient.extract_table(
            table_ref,
            destination_uri,
            # Location tiene que ser igual a la location de la tabla 
            location="US",
            job_config=job_config
        )  

        # API request
        extract_job.result()  # Waits for job to complete.

        print(
            "Exporto {}:{}.{} a {}".format(project, dataset_id, table_id, destination_uri)
        )
        

    def fast_export(self,query,destionatio_path):
        """
        Exporta la query a un bucket de GCP en parquet.
        destionatio_path: debe tener un formato gs://bucket/algo
        ouput: carga la query en gs://bucket/algo/*.parquet
        """
        storage = Storage(self.credential)
        
        storage.deleteStorageFolder(destionatio_path)
        
        if((destionatio_path[:5] != "gs://") | (len(destionatio_path.split("/")) < 3)):
            print("Error de formato del bucket. Formato esperado: gs://bucket/algo")
            return
        
        if self.gcCredentials is None:
            self.get_gcp_credential()
            
        import random
        import string

        # Creo una tabla con nombre random
        letters = string.ascii_lowercase
        rand_name = ''.join(random.choice(letters) for i in range(10))
        table_name = self.gcCredentials.project_id+".TEMP45."+rand_name
        self.create_table_from_q(query, table_name)
        
        
        
        # esa tabla la mando a un bucket
        self.export_table(destionatio_path,rand_name,dataset="TEMP45")

        #borro la tabla random
        self.drop_table(table_name)
        
        
    def execute_response(self, q):
        """
        Query (execute) to dataframe (response).
        """
        import time
        from google.cloud import bigquery
        
        if self.gcCredentials is None:
            self.get_gcp_credential()
        
        if self.bqClient is None:
            self.create_clients()
        
        job = self.bqClient.query(q)
        
        job.result()
        
        df = job.to_dataframe()
        
        return df
    
    
    def execute(self, q):
        """
        Query execute without response. Returns query object.
        """
        from google.cloud import bigquery
        import time
        
        if self.gcCredentials is None:
            self.get_gcp_credential()
        
        if self.bqClient is None:
            self.create_clients()
            
        job = self.bqClient.query(q)
        
        job.result()
        
        return job 




class Storage():
    
    def __init__(self, credential):
        self.credential = credential
        self.gsClient = None
        self.gcCredential = None

    
    def get_gcp_credential(self):
        """
        Obtengo las credenciales
        """
        
        import json
        from google.oauth2 import service_account
        
        gcp_credentials = None
        self.gcCredentials = None

        try:
            gcp_credentials = self.credential
            gcp_credentials = json.loads(gcp_credentials, strict=False)
            self.gcCredentials = service_account.Credentials.from_service_account_info(gcp_credentials)
        except Exception as e:
            exception_message = "get_gcp_credential: {}".format(e)
            print(exception_message)

        return
    
    def create_clients(self):
        """ 
        Creo el cliente para Google Storage.
        """
        from google.cloud import storage

        if self.gcCredentials is None:
            self.get_gcp_credential()

        gc_project = self.gcCredentials.project_id
        self.gsClient = storage.Client(project=gc_project,credentials=self.gcCredentials)
    
    
    def check_upload(self, bucket_name,destination_file):
        """
        Devuelve False si no se subio el file, True si subió.
        """

        gs_bucket = self.gsClient.get_bucket(bucket_name)
        gs_blob = gs_bucket.get_blob(destination_file)

        if (gs_blob == None):
            return False
        return True
        
        
    def upload(self,bucket_name, destination_file, source_file,make_public = True):
        """
        Sube el csv indicado a GCP y lo hace público
        """

        if (self.gcCredential == None):
            self.get_gcp_credential()
 
        self.create_clients()

        gs_bucket = self.gsClient.get_bucket(bucket_name) # me paro en mi bucket
        gs_blob = gs_bucket.blob(destination_file) # instancio el archivo a subir
        gs_blob.upload_from_filename(source_file) # subo el file

        if(self.check_upload(bucket_name,destination_file) == False):
            print(f'GCP | Fallo la carga a {bucket_name}:{destination_file}')
        else:
            if(make_public):
                # Lo hago publico
                gs_blob.make_public()

            
    def upload_file(self,source_file,gs_destination_path):
        """
        Sube el archivo indicado a GCP
        """
        
        if((gs_destination_path[:5] != "gs://") | (len(gs_destination_path.split("/")) < 3)):
            raise("Error de formato del bucket en upload_file. Formato esperado: gs://bucket/algo")
        
        self.upload(gs_destination_path.split("/")[2],'/'.join(gs_destination_path.split("/")[3:]),source_file,make_public = False)
        
                
    def delete_blob(self, bucket_name, blob_name):
        """
        Deletes a blob from the bucket.
        """

        if (self.gcCredential == None):
            self.get_gcp_credential()
        
        self.create_clients()

        bucket = self.gsClient.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()

        print(f"Blob {blob_name} deleted.")
    
    def delete_bucket(self,bucket_name):
        """Deletes a bucket. The bucket must be empty."""
        # bucket_name = "your-bucket-name"

        if (self.gcCredential == None):
            self.get_gcp_credential()
        
        self.create_clients()

        bucket = self.gsClient.bucket(bucket_name)
        bucket.delete()

        print("Bucket {} deleted".format(bucket.name))
    

    def deleteStorageFolder(self,gs_source_path):
        """
        This function deletes from GCP Storage
        :param bucketName: The bucket name in which the file is to be placed
        :param folder: Folder name to be deleted
        :return: returns nothing
        """
        if((gs_source_path[:5] != "gs://") | (len(gs_source_path.split("/")) < 3)):
            raise("Error de formato del bucket en upload_file. Formato esperado: gs://bucket/algo")
            
        if (self.gcCredential == None):
            self.get_gcp_credential()
 
        self.create_clients()
        
        #gs_bucket = self.gsClient.get_bucket(gs_source_path.split("/")[2]) # me paro en mi bucket
        folder = "/".join(gs_source_path.split("/")[3:])
        blobs = self.gsClient.get_bucket(gs_source_path.split("/")[2]).list_blobs(prefix=folder)
        for blob in blobs:
            blob.delete()
    
    def download_file(self,gs_source_path,destination_file):
        if((gs_source_path[:5] != "gs://") | (len(gs_source_path.split("/")) < 3)):
            raise("Error de formato del bucket en upload_file. Formato esperado: gs://bucket/algo")
            
        if (self.gcCredential == None):
            self.get_gcp_credential()
 
        self.create_clients()

        
        gs_bucket = self.gsClient.get_bucket(gs_source_path.split("/")[2]) # me paro en mi bucket
        gs_blob = gs_bucket.blob('/'.join(gs_source_path.split("/")[3:])) # instancio el archivo a subir
        gs_blob.download_to_filename(destination_file) # subo el file
    
    def download_folder(self, gs_source_path, destination_folder):
        """
        This function helps you download all blobs from a folder, ideal for parquets
        :param gs_source_path: The bucket and folder name you want to download the files from
        :param destination_folder: The local path of folder that you want to download files to
        :return: returns nothing
        """
        if((gs_source_path[:5] != "gs://") | (len(gs_source_path.split("/")) < 3)):
            raise("Error de formato del bucket en upload_file. Formato esperado: gs://bucket/algo")
            
        if (self.gcCredential == None):
            self.get_gcp_credential()
 
        self.create_clients()
    
        gs_bucket = self.gsClient.get_bucket(gs_source_path.split("/")[2]) # me paro en mi bucket
        folder = "/".join(gs_source_path.split("/")[3:])
        blobs = self.gsClient.get_bucket(gs_source_path.split("/")[2]).list_blobs(prefix=folder)
        
        if not os.path.isdir(destination_folder):
            os.mkdir(destination_folder)
        
        for blob in blobs:
            file_name = blob.name.split("/")[len(blob.name.split("/"))-1]
            blob.download_to_filename(destination_folder + "/" + file_name)


    def print_gcps(self, *kargs, bq_log_path = '', silent = False):
        msg = ' '.join([str(x) for x in kargs])
        file1 = open("log.txt","a")#append mode 
        file1.write(msg) 
        file1.write("\n") 
        file1.close()

        if(silent == False):
            print(msg)

        if(bq_log_path != ''):
            self.upload_file('log.txt',bq_log_path)
