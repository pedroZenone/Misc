from multiprocessing import cpu_count
from melitk import melipass
from base64 import b64decode
from os import environ
import datetime
from pytz import timezone
# Defines
PAIS = melipass.get_fda_parameter('SIT_SITE_ID', default="pepito")
if(PAIS == "pepito"):
    PAIS = "MLA" # Caso de hacerlo manual cargar aca!!
    print("NO se cargo ningun pais por defecto se carga:",PAIS)    

FECHA = melipass.get_fda_parameter('FECHA', default="")

if FECHA == "":
    FECHA = datetime.datetime.now(timezone('America/Argentina/Buenos_Aires')).strftime("%Y_%m_%d")
    FECHA_END = datetime.datetime.now(timezone('America/Argentina/Buenos_Aires'))
else:
    FECHA_END = datetime.datetime.strptime(FECHA, "%Y_%m_%d")




FREQ_PAST = 4

cores = 39 # Para que no rompa cuando paraleliza
PRINT_SHAPE = True

# Puntero para guardar tablas y referencia de carpetas
initiative = "FORECAST_3M"
BU = "ML"


# Tablas para etls
gcps_path = "gs://marketing-modelling/ML/LTV_3M"
gcps_path_in = gcps_path + f"/{PAIS}/train/DAY{FECHA}/dataset/"
gcps_path_out = gcps_path + f"/{PAIS}/model/DAY{FECHA}/"
bq_log_path = gcps_path + f"/{PAIS}/train/DAY{FECHA}/log.txt"

temp45_base_path = "meli-marketing.TEMP45.TRAIN_"+PAIS+"_"+BU+"_"+initiative+"_"
tabla_pivot2bq = temp45_base_path + "PIVOT"

# tabla_pivot = "mkt_corp.PZ_TMP_PIVOT_TRAINING_"+PAIS+"_"+BU+"_"+initiative
# tabla_pivot2 = "mkt_corp.PZ_TMP_PIVOT_UNIQUE_BUYERS_TRAINING_"+PAIS+"_"+BU+"_"+initiative

# tabla_destroy = "mkt_corp.PZ_DESTROY_ME_TRAINING_"+PAIS+"_"+BU+"_"+initiative
# volatile = "PZ_TMP_SELLERS_PAYERS_TRAINING_"+PAIS+"_"+BU+"_"+initiative
# volatile2 = "PZ_TMP_SELLERS_PAYERS2_TRAINING_"+PAIS+"_"+BU+"_"+initiative

# hive_tbl = "mkt_insights.PZ_TMP_PIVOT_TRAINING_"+PAIS+"_"+BU+"_"+initiative
# path_hive_table = "MKT/pzenone/training/"+PAIS+"/"+BU+"/"+initiative
# tabla_destroy_hive = "mkt_insights.PZ_DESTROY_ME_TRAINING_"+PAIS+"_"+BU+"_"+initiative

# tmp_visits_tbl = "MKT_INSIGHTS.PZ_TMP_TRAINING_"+PAIS+"_"+BU+"_"+initiative+"_VISITS"
# path_tmp_visits_tbl="pzenone/tmp/training/"+PAIS+"/"+BU+"/"+initiative+"/"+tmp_visits_tbl[13:]
    
# Datos de modelo+
FORECAST_DAYS = 160
FORECAST_MONTHS = 3
TRAINING_MONTHS = 12  # meses que le voy a dar para entrenar
PAST_MONTHS = 12 # meses para ver que paso en el pasado (quintil past)
PAST_MP_MONTHS = 3 # meses para ver que paso en el pasado (quintil past)
SHORT_PAST = 3 # meses para atras en el corto plazo

country = {
        "MLM":"mx",
        "MLA":"ar",
        "MCO":"co",
        "MLB":"br",
        "MLC":"cl"
}

# Fillna
FILL_RECENCY = -10.0
FILL_FLOAT = 0.0
FILL_IPT = -10
N_CLUSTERS = 4 # Cantidad de clusters para hacer RFM inicial

# Provincias
#### Agregar MCO cuando tenga los estados
STATES = {
    "MLU":['montevideo', 'maldonado', 'tacuarembo', 'lavalleja', 'flores',
                   'salto', 'colonia', 'canelones', 'artigas', 'san jose', 'paysandu',
                   'rivera', 'rocha', 'florida', 'soriano', 'rio negro',
                   'treinta y tres', 'cerro largo', 'durazno'],
    
    "MLM":['estado de méxico','distrito federal', 'jalisco', 'veracruz', 'nuevo león', 'guanajuato',
 'chihuahua', 'puebla', 'tamaulipas', 'michoacán', 'coahuila', 'sinaloa', 'baja california', 'sonora',
 'querétaro', 'quintana roo', 'san luis potosí', 'oaxaca', 'hidalgo', 'chiapas', 'guerrero', 'morelos', 'tabasco',
 'yucatán'],
    
    "MLB":['são paulo','minas gerais','rio de janeiro','paraná','rio grande do sul','bahia',
 'santa catarina','goiás','pernambuco','espírito santo','distrito federal','ceará','mato grosso',
 'mato grosso do sul','pará','maranhão'],
    
    "MLC":['rm (metropolitana)','valparaíso','biobío','los lagos','la araucanía',
 'maule',"libertador b. o'higgins",'coquimbo','antofagasta','los ríos','atacama','ñuble'],
    
    "MLA":['buenos aires','capital federal','córdoba','santa fe','mendoza','entre ríos',
 'río negro','neuquén','tucumán', 'misiones', 'salta', 'corrientes', 'chubut', 'chaco', 'san luis',
    'san juan'],
    
    "MCO":['bogotá d.c.','antioquia', 'valle del cauca', 'cundinamarca', 'atlantico',
 'santander', 'bolivar', 'boyaca', 'tolima', 'risaralda', 'caldas', 'huila', 'meta',
 'nariño', 'magdalena', 'norte de santander', 'córdoba', 'cesar']
}

N_ITER = 200
n_jobs_train = 10

SELECT_VARIABLES_STEPS = 5
FULL_TRAINING_STEPS = 200

# n_iter = 2
# n_jobs_train = 10

AUTH_BIGQUERY = b64decode(environ['SECRET_AUTH_BIGQUERY_MODEL'])



#gcps_path_in = 'gs://marketing-modelling/ML/LTV_3M/MCO/train/DAY2022_01_07/dataset/'
#gcps_path_out = 'gs://marketing-modelling/ML/LTV_3M/MCO/model/DAY2022_01_07/'