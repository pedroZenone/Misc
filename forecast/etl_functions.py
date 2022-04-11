import os
import pandas as pd
import s3fs

import datetime
from dateutil.relativedelta import relativedelta
import gc

import boto3
s3 = boto3.client('s3')
s3.download_file("fury-data-apps", "marketing-utils/pzenone/utils.py","utils.py")
import sys
sys.path.append(os.path.dirname(os.path.expanduser(".")))
# sys.path.append(os.path.dirname(os.path.expanduser("./utils.py")))
import utils

import sys
sys.path.append(os.path.dirname(os.path.expanduser(".")))
# sys.path.append(os.path.dirname(os.path.expanduser("./defines.py")))
sys.path.append(os.path.dirname(os.path.expanduser("../defines.py")))
from defines import *
from random import random
from math import floor

ETL_DEBUG = True

def etl_full_table(gcps_path_in, bq_app,date_from,date_to,sample = False, n_sample = 300000):
    """
    Generate user base and export it to BigQuery temporary table.
    gcps_path_in: full Google Storage path (gs://meli-marketing/ML/LTV_3M/.../)}
    bq_app: BigQuery object invocated through BigQuery local Library (./google_cloud.py)
    date_from: from what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    date_to: to what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    sample: Whether you want to do a sample (300.000) of the userbase or not
    """
    
        
    # Sampleo constante de 300.000 usuarios, independientemente de cuantos usuarios tenga la
    # base de cada pais. Para el caso de Arg. y Brasil sera un sampleo pequenio en relacion
    # a la cantidad de usuarios, pero para chile o colombia sera muy parecido a la base total (con las 14 fechas programadas en el ETL)
    if(sample == True):
        sample_str = f"WHERE RAND() < ({n_sample} / (SELECT COUNT(*) FROM USERS))"
    else:
        sample_str = ""
    
    
    def query(from_, to_, table_path):
        return f"""
            CREATE TABLE {tabla_pivot2bq} AS (
            WITH USERS AS (
            SELECT DISTINCT a11.CUS_CUST_ID_BUY as CUS_CUST_ID
            FROM `meli-marketing.MODELLING.V_BT_ORD_ORDER` a11 
            WHERE a11.order_status = 'paid' AND a11.SIT_SITE_ID = '{PAIS}'
            AND a11.GMV_USD	< 10000

            AND a11.ORDER_CLOSED_DT between date'{from_}' and date '{to_}'
            AND ORDER_CREATED_DT between DATE_SUB(ORDER_CLOSED_DT, INTERVAL 4 DAY) AND ORDER_CREATED_DT
            and FLAG_AUTO_OFFER = false and FLAG_BONIF=False)
            SELECT * FROM USERS
            {sample_str}
            ORDER BY CUS_CUST_ID
            )
            """
    
    table_path = 'PIVOT'
    bq_app.execute(f"DROP TABLE IF EXISTS {tabla_pivot2bq}")
    bq_app.execute(query(date_from.strftime("%Y-%m-%d"),date_to.strftime("%Y-%m-%d"), table_path))
    
    if ETL_DEBUG:
        print(f"{table_path}: READY")

def etl_asp_pareto(gcps_path_in,bq_app,date_from,date_to,tables):
    """
    gcps_path_in: full Google Storage path (gs://meli-marketing/ML/LTV_3M/.../)}
    bq_app: BigQuery object invocated through BigQuery local Library (./google_cloud.py)
    date_from: from what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    date_to: to what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    """
    def query(from_, to_, table_path):
        return f"""
            CREATE TABLE {temp45_base_path + table_path} as (
            WITH country_cal as (SELECT 
            a11.CUS_CUST_ID_BUY as CUS_CUST_ID,
            a11.ORDER_CREATED_DT as datee,
            (CASE WHEN (SUM(a11.GMV_USD) / SUM(a11.ITEMS_TOTAL)) > 350 
                THEN (350 * SUM(a11.ITEMS_TOTAL)) 
                ELSE SUM(a11.GMV_USD) END) as sales_trunc

            FROM `meli-marketing.MODELLING.V_BT_ORD_ORDER` a11 
            WHERE a11.order_status = 'paid' AND a11.SIT_SITE_ID = '{PAIS}'
            AND a11.GMV_USD	< 10000
            AND a11.ORDER_CLOSED_DT between date'{from_}' and date '{to_}'
            AND a11.ORDER_CREATED_DT between DATE_SUB(ORDER_CLOSED_DT, INTERVAL 4 DAY) AND ORDER_CREATED_DT
            AND order_status = 'paid' and FLAG_AUTO_OFFER = false and FLAG_BONIF=False
            AND EXISTS(SELECT 1 FROM {tabla_pivot2bq} a WHERE a.CUS_CUST_ID = a11.CUS_CUST_ID_BUY)
            GROUP BY 1,2)
            select CUS_CUST_ID, 
            (SUM(sales_trunc) / COUNT(datee)) as ASP_PARETO

            FROM country_cal
            GROUP BY CUS_CUST_ID
            )
        """
    table_path = 'ASP_PARETO'
    bq_app.execute(f"DROP TABLE IF EXISTS {temp45_base_path + table_path}")
    bq_app.execute(query(date_from.strftime("%Y-%m-%d"),date_to.strftime("%Y-%m-%d"), table_path))
    tables.append(table_path)
    
    if ETL_DEBUG:
        print(f"{table_path}: READY")


def etl_orders_frequencies(gcps_path_in,bq_app,date_from,date_to,tables):
    """
    gcps_path_in: full Google Storage path (gs://meli-marketing/ML/LTV_3M/.../)}
    bq_app: BigQuery object invocated through BigQuery local Library (./google_cloud.py)
    date_from: from what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    date_to: to what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    """
    
    def query(from_, to_, table_path):
        return f"""
            CREATE TABLE {temp45_base_path + table_path} as (
            WITH TOTAL as 
            (SELECT 
            a11.ORDER_CREATED_DT as datee,
            a11.CUS_CUST_ID_BUY as CUS_CUST_ID,
            SUM(a11.GMV_USD) as sales,
            SUM(a11.ITEMS_TOTAL) as SI,
            COUNT(DISTINCT CASE WHEN a11.ORDER_PACK_ID IS NOT NULL THEN a11.ORDER_PACK_ID ELSE a11.ORDER_ID END) as ordenes

            FROM `meli-marketing.MODELLING.V_BT_ORD_ORDER` a11 
            WHERE a11.order_status = 'paid' AND a11.SIT_SITE_ID = '{PAIS}'
            AND a11.GMV_USD	< 10000
            AND a11.ORDER_CLOSED_DT between date'{from_}' and date '{to_}'
            AND a11.ORDER_CREATED_DT between DATE_SUB(ORDER_CLOSED_DT, INTERVAL 4 DAY) AND ORDER_CREATED_DT
            AND order_status = 'paid' and FLAG_AUTO_OFFER = false and FLAG_BONIF=False
            AND EXISTS(SELECT 1 FROM {tabla_pivot2bq} a WHERE a.CUS_CUST_ID = a11.CUS_CUST_ID_BUY)
            GROUP BY 1,2)

            SELECT TOTAL.CUS_CUST_ID,
            SUM(TOTAL.sales)/SUM(TOTAL.SI) as ASP_long,
            COALESCE(SUM(total_90d.sales)/SUM(total_90d.SI),0) as ASP_short,
            AVG(TOTAL.SI / TOTAL.ordenes) as productos_long,
            COALESCE(AVG(total_90d.SI / total_90d.ordenes),0) as productos_short,
            SUM(TOTAL.ordenes) as ordenes_SUM_long,
            AVG(TOTAL.ordenes) as ordenes_MEAN_long,
            COALESCE(SUM(total_90d.ordenes),0) as ordenes_SUM_short,
            COALESCE(AVG(total_90d.ordenes),0) as ordenes_MEAN_short,
            COUNT(DISTINCT TOTAL.datee) as freq_D_long,
            COUNT(DISTINCT total_90d.datee) as freq_D_short,
            COUNT (DISTINCT DATE_TRUNC(TOTAL.datee, MONTH)) as freq_M_long,
            COUNT (DISTINCT DATE_TRUNC(total_90d.datee, MONTH)) as freq_M_short,
            COUNT (DISTINCT DATE_TRUNC(TOTAL.datee, WEEK)) as freq_W_long,
            COUNT (DISTINCT DATE_TRUNC(total_90d.datee, WEEK)) as freq_W_short,
            COUNT(DISTINCT total_6d.datee) as freq_7D,
            COUNT(DISTINCT total_6m.datee) as freq_180D,
            DATE_DIFF('{to_}', MIN(TOTAL.datee),  DAY) as my_first_in_windows,
            DATE_DIFF('{to_}', MAX(TOTAL.datee),  DAY) as my_recency,
            SUM(TOTAL.SI) as SI_SUM_long,
            AVG(TOTAL.SI) as SI_MEAN_long,
            COALESCE(SUM(total_90d.SI),0) as SI_SUM_short,
            COALESCE(AVG(total_90d.SI),0) as SI_MEAN_short,
            --Franquero: si compra viernes, sabado o domingo
            AVG(CASE WHEN EXTRACT(DAYOFWEEK FROM TOTAL.DATEE) IN (1,6,7) THEN 1 ELSE 0 END) as franquero,
            AVG(TOTAL.sales) as money_mean,
            MAX(TOTAL.sales) as money_max,
            MIN(TOTAL.sales) as money_min,
            (CASE WHEN STDDEV(TOTAL.sales) = 0 THEN 0 ELSE (AVG(TOTAL.sales)/STDDEV(TOTAL.sales)) END) as money_CV,
            SUM(TOTAL.sales) as money_sum,
            SUM(CASE WHEN total_90d.datee BETWEEN DATE_SUB('{to_}', INTERVAL 30 DAY) AND DATE'{to_}' THEN 1 ELSE 0 END) as size_0,
            SUM(CASE WHEN total_90d.datee BETWEEN DATE_SUB('{to_}', INTERVAL 60 DAY) AND DATE_SUB('{to_}', INTERVAL 30 DAY) THEN 1 ELSE 0 END) as size_1,
            SUM(CASE WHEN total_90d.datee BETWEEN DATE_SUB('{to_}', INTERVAL 90 DAY) AND DATE_SUB('{to_}', INTERVAL 60 DAY) THEN 1 ELSE 0 END) as size_2,
            SUM(CASE WHEN total_90d.datee BETWEEN DATE_SUB('{to_}', INTERVAL 30 DAY) AND DATE'{to_}' THEN total_90d.sales ELSE 0 END) as sales_0,
            SUM(CASE WHEN total_90d.datee BETWEEN DATE_SUB('{to_}', INTERVAL 60 DAY) AND DATE_SUB('{to_}', INTERVAL 30 DAY) THEN total_90d.sales ELSE 0 END) as sales_1,
            SUM(CASE WHEN total_90d.datee BETWEEN DATE_SUB('{to_}', INTERVAL 90 DAY) AND DATE_SUB('{to_}', INTERVAL 60 DAY) THEN total_90d.sales ELSE 0 END) as sales_2,
            MIN(CASE WHEN total_90d.datee BETWEEN DATE_SUB('{to_}', INTERVAL 30 DAY) AND DATE'{to_}' 
                THEN DATE_DIFF(DATE'{to_}',total_90d.datee, DAY)
                ELSE NULL END) AS recency_0,

            MIN(CASE WHEN total_90d.datee BETWEEN DATE_SUB('{to_}', INTERVAL 60 DAY) AND DATE_SUB('{to_}', INTERVAL 30 DAY) 
                THEN DATE_DIFF(DATE'{to_}', total_90d.datee, DAY)
                ELSE NULL END) as recency_1,

            MIN(CASE WHEN total_90d.datee BETWEEN DATE_SUB('{to_}', INTERVAL 90 DAY) AND DATE_SUB('{to_}', INTERVAL 60 DAY) 
                THEN DATE_DIFF(DATE'{to_}', total_90d.datee, DAY)
                ELSE NULL END) AS recency_2


            FROM TOTAL
            LEFT JOIN (SELECT * FROM TOTAL WHERE datee >= DATE_SUB('{to_}', INTERVAL 180 DAY)) total_6m 
                on total_6m.CUS_CUST_ID = TOTAL.CUS_CUST_ID and total_6m.datee = TOTAL.datee 
            LEFT JOIN (SELECT * FROM TOTAL WHERE datee >= DATE_SUB('{to_}', INTERVAL 90 DAY)) total_90d 
                on total_90d.CUS_CUST_ID = TOTAL.CUS_CUST_ID and total_90d.datee = TOTAL.datee 
            LEFT JOIN (SELECT * FROM TOTAL WHERE datee >= DATE_SUB('{to_}', INTERVAL 6 DAY)) total_6d 
                on total_6d.CUS_CUST_ID = TOTAL.CUS_CUST_ID and total_6d.datee = TOTAL.datee 
            WHERE EXISTS(SELECT 1 FROM {tabla_pivot2bq} a WHERE a.CUS_CUST_ID = TOTAL.CUS_CUST_ID)
            GROUP BY TOTAL.CUS_CUST_ID
            )
        """
    table_path = 'ORDERS_FREQUENCIES'
    bq_app.execute(f"DROP TABLE IF EXISTS {temp45_base_path + table_path}")
    bq_app.execute(query(date_from.strftime("%Y-%m-%d"),date_to.strftime("%Y-%m-%d"),table_path))
    tables.append(table_path)
    
    if ETL_DEBUG:
        print(f"{table_path}: READY")

def etl_compras_shipping(gcps_path_in,bq_app,date_from,date_to,tables):
    """
    gcps_path_in: full Google Storage path (gs://meli-marketing/ML/LTV_3M/.../)}
    bq_app: BigQuery object invocated through BigQuery local Library (./google_cloud.py)
    date_from: from what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    date_to: to what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    """

    def query(from_, to_, table_path):
        return f"""
            CREATE TABLE {temp45_base_path + table_path} as (
            WITH PURCHASES AS (
            SELECT a11.CUS_CUST_ID_BUY as CUS_CUST_ID,
                COUNT(DISTINCT ship.CRT_PURCHASE_ID) CANT_COMPRAS_CARRITO,
                COUNT(CASE WHEN a11.FLAG_SUPERMARKET = TRUE THEN 1 ELSE 0 END) CPG,
                COUNT(DISTINCT CASE WHEN a11.ORDER_CREATED_DT >= DATE_SUB('{to_}', INTERVAL 30 DAY) THEN cus_cust_id_sel ELSE NULL END) vendedores_distintos_30,
                COUNT(DISTINCT CASE WHEN (a11.ORDER_CREATED_DT >= DATE_SUB('{to_}', INTERVAL 60 DAY)) AND (a11.ORDER_CREATED_DT < DATE_SUB('{to_}', INTERVAL 30 DAY)) THEN cus_cust_id_sel ELSE NULL END) vendedores_distintos_60_30,
                COUNT(DISTINCT CASE WHEN (a11.ORDER_CREATED_DT >= DATE_SUB('{to_}', INTERVAL 90 DAY)) AND (a11.ORDER_CREATED_DT < DATE_SUB('{to_}', INTERVAL 60 DAY)) THEN cus_cust_id_sel ELSE NULL END) vendedores_distintos_90_60,
                COUNT(DISTINCT a11.CUS_CUST_ID_SEL) vendedores_distintos_long,
                SUM (CASE WHEN app.MAPP_MOBILE_FLAG = 'Desktop' THEN 1 ELSE 0 END) DESKTOP_PURCHASE,
                SUM(CASE WHEN app.MAPP_MOBILE_FLAG = 'Mobile-WEB' THEN 1 else 0 END) MOBILE_WEB_PURCHASE,
                SUM(CASE WHEN app.MAPP_MOBILE_FLAG= 'Mobile-APP' AND app.MAPP_APP_DESC = 'android' THEN 1 else 0 END) ANDROID_PURCHASE,
                SUM(CASE WHEN app.MAPP_MOBILE_FLAG = 'Mobile-APP' AND app.MAPP_APP_DESC = 'iphone' THEN 1 else 0 END) IOS_PURCHASE,
                SUM(CASE WHEN a11.ORDER_ITEM_CONDITION = 'new' THEN 1 ELSE 0 END) AS CANTIDAD_COMPRAS_ITEMS_NUEVOS,
                SUM(CASE WHEN a11.ORDER_ITEM_CONDITION = 'used' THEN 1 ELSE 0 END) AS CANTIDAD_COMPRAS_ITEMS_USADOS,
                COUNT(DISTINCT CASE WHEN COALESCE(SHP_FREE_FLG,0) >= 1 THEN ORDER_ID ELSE NULL END) AS CANTIDAD_COMPRAS_FREE_SHIPPING,
                COUNT(DISTINCT CASE WHEN ship.SHP_SHIPMENT_ID IS NULL THEN ORDER_ID ELSE NULL END) AS CANTIDAD_COMPRAS_NO_ENVIOS,
                COUNT(DISTINCT CASE WHEN COALESCE(ship.SHP_CART_TYPE,0) = 0 THEN ORDER_ID ELSE NULL END) AS CANTIDAD_COMPRAS_NO_CARRITO,
                COUNT(DISTINCT CASE WHEN ship.SHP_PICKING_TYPE_ID = 'drop_off' THEN ORDER_ID ELSE NULL END) AS CANTIDAD_COMPRAS_ENVIO_DROP_OFF,


            FROM `meli-marketing.MODELLING.V_BT_ORD_ORDER` a11

            LEFT JOIN `meli-bi-data.WHOWNER.LK_MAPP_MOBILE` app
                on COALESCE(app.MAPP_APP_ID, '-1') = a11.APPLICATION_ID

            LEFT JOIN `meli-bi-data.WHOWNER.BT_SHP_SHIPMENTS` ship
                on ship.SHP_SHIPMENT_ID = a11.ORDER_SHIPPING_ID
                AND a11.SIT_SITE_ID = ship.SIT_SITE_ID
                AND ship.SHP_Type = 'forward'
                AND ship.SHP_DATE_CREATED_ID BETWEEN DATE '{from_}' AND DATE'{to_}'

            WHERE a11.order_status = 'paid'
                AND a11.SIT_SITE_ID = '{PAIS}' 
                and a11.FLAG_BONIF = False
                and FLAG_AUTO_OFFER = False
                AND a11.GMV_USD	< 10000
                AND a11.ORDER_CLOSED_DT between date'{from_}' and date '{to_}'
                AND ORDER_CREATED_DT between DATE_SUB(ORDER_CLOSED_DT, INTERVAL 4 DAY) AND ORDER_CREATED_DT
                AND EXISTS(SELECT 1 FROM {tabla_pivot2bq} u WHERE a11.CUS_CUST_ID_BUY = u.CUS_CUST_ID)
            GROUP BY 1)
            SELECT PURCHASES.*,
                (CASE GREATEST(DESKTOP_PURCHASE, MOBILE_WEB_PURCHASE, ANDROID_PURCHASE, IOS_PURCHASE) 
                    when DESKTOP_PURCHASE then 'DESKTOP_PURCHASE' 
                    when MOBILE_WEB_PURCHASE then 'MOBILE_WEB_PURCHASE' 
                    when ANDROID_PURCHASE then 'ANDROID_PURCHASE'
                    when IOS_PURCHASE then 'IOS_PURCHASE'
                    end) as DISPOSITIVO_MAS_USADO
            FROM PURCHASES
            )
            """
    table_path = 'COMPRAS_SHIPPING'
    bq_app.execute(f"DROP TABLE IF EXISTS {temp45_base_path + table_path}")
    bq_app.execute(query(date_from.strftime("%Y-%m-%d"),date_to.strftime("%Y-%m-%d"),table_path))
    tables.append(table_path)
    
    if ETL_DEBUG:
        print(f"{table_path}: READY")

def etl_demo(gcps_path_in,bq_app,date_from,date_to,tables):
    """
    gcps_path_in: full Google Storage path (gs://meli-marketing/ML/LTV_3M/.../)}
    bq_app: BigQuery object invocated through BigQuery local Library (./google_cloud.py)
    date_from: from what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    date_to: to what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    """
    
    def query(to_, table_path):
        return f"""
        CREATE TABLE {temp45_base_path + table_path} as (
        SELECT a.CUS_CUST_ID as CUS_CUST_ID,
        DATE_DIFF(DATE'{to_}', CAST(a.CUS_RU_SINCE_DT as DATE), DAY) as DAYS_FROM_REGISTRATION,
        DATE_DIFF(DATE'{to_}', CAST(a.CUS_FIRST_PUBLICATION as DATE), DAY) as DAYS_FROM_FIRST_PUBLICATION,
        DATE_DIFF(DATE'{to_}', CAST(b.CUS_FIRST_BUY_NO_BONIF_AUTOOF as DATE), DAY) as DAYS_FROM_FIRST_BUY,
        DATE_DIFF(DATE'{to_}', CAST(b.CUS_FIRST_SEL_NO_BONIF_AUTOOF as DATE), DAY) as DAYS_FROM_FIRST_SELL

        FROM `meli-bi-data.WHOWNER.LK_CUS_CUSTOMERS_DATA` a
        left join `meli-bi-data.WHOWNER.LK_CUS_CUSTOMER_DATES` b
            on a.CUS_CUST_ID = b.CUS_CUST_ID and b.SIT_SITE_ID = '{PAIS}'
        WHERE a.SIT_SITE_ID_CUS = '{PAIS}'
        AND EXISTS (SELECT 1 FROM {tabla_pivot2bq} u WHERE a.CUS_CUST_ID = u.CUS_CUST_ID)
        )
        """
    table_path = 'DEMO'
    bq_app.execute(f"DROP TABLE IF EXISTS {temp45_base_path + table_path}")
    bq_app.execute(query(date_to.strftime("%Y-%m-%d"),table_path))
    tables.append(table_path)
    
    if ETL_DEBUG:
        print(f"{table_path}: READY")

def etl_sellers(gcps_path_in,bq_app,date_from,date_to,tables):
    """
    gcps_path_in: full Google Storage path (gs://meli-marketing/ML/LTV_3M/.../)}
    bq_app: BigQuery object invocated through BigQuery local Library (./google_cloud.py)
    date_from: from what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    date_to: to what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    """
    
    def query(from_, to_, table_path):
        return f"""
        CREATE TABLE {temp45_base_path + table_path} as (
        SELECT a11.CUS_CUST_ID_SEL as CUS_CUST_ID,
            SUM(a11.GMV_USD) as SALES_SELLER,
            DATE_DIFF(DATE'{to_}',MAX(a11.ORDER_CREATED_DT), DAY) as RECENCY_SELLER,
            DATE_DIFF(DATE'{to_}',MIN(a11.ORDER_CREATED_DT), DAY) as FIRST_SELLER

        FROM `meli-marketing.MODELLING.V_BT_ORD_ORDER` a11

        WHERE a11.order_status = 'paid'
            AND a11.SIT_SITE_ID = '{PAIS}'
            AND a11.GMV_USD < 10000
            and a11.FLAG_BONIF = False
            and FLAG_AUTO_OFFER = false
            AND a11.ORDER_CLOSED_DT between date'{from_}' and date '{to_}'
            AND ORDER_CREATED_DT between DATE_SUB(ORDER_CLOSED_DT, INTERVAL 4 DAY) AND ORDER_CREATED_DT
            --GMV_FLAG??
            AND EXISTS(SELECT 1 FROM {tabla_pivot2bq} u WHERE a11.CUS_CUST_ID_SEL = u.CUS_CUST_ID)
        GROUP BY 1
        )
        """
    table_path = 'SELLERS'
    bq_app.execute(f"DROP TABLE IF EXISTS {temp45_base_path + table_path}")
    bq_app.execute(query(date_from.strftime("%Y-%m-%d"),date_to.strftime("%Y-%m-%d"),table_path))
    tables.append(table_path)
    
    if ETL_DEBUG:
        print(f"{table_path}: READY")

def etl_payments(gcps_path_in,bq_app,date_from,date_to,tables):
    """
    gcps_path_in: full Google Storage path (gs://meli-marketing/ML/LTV_3M/.../)}
    bq_app: BigQuery object invocated through BigQuery local Library (./google_cloud.py)
    date_from: from what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    date_to: to what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    """
    
    def query(from_, to_, table_path):
        return f"""
        CREATE TABLE {temp45_base_path + table_path} as (
        SELECT pay.cus_cust_id_buy as CUS_CUST_ID,
            AVG(CASE WHEN pay.pay_ccd_installments_qty > 1 THEN pay.pay_ccd_installments_qty ELSE NULL END) AVG_CUOTAS,
            MAX(pay.pay_ccd_installments_qty) MAX_CUOTAS,

            SUM(CASE WHEN (pay.pay_total_paid_dol_amt / pay.pay_transaction_dol_amt) = 1.0 AND pay.pay_ccd_installments_qty > 1 THEN 1 ELSE 0 END) AS CSI,
            SUM(CASE WHEN (pay.pay_total_paid_dol_amt / pay.pay_transaction_dol_amt) > 1.0 AND (pay.pay_ccd_installments_qty > 1) THEN 1 ELSE 0 END) AS CCI,

            SUM(CASE WHEN LOWER(pay.PAY_PAYMENT_METHOD_TYPE_ID)= 'ticket' THEN 1 ELSE 0 END) AS SUM_TICKET,       

            SUM(CASE WHEN pay.PAY_PAYMENT_METHOD_TYPE_ID= 'account_money' THEN 1 ELSE 0 END) AS SUM_DINERO_CUENTA, --DINERO EN CUENTA

            SUM(CASE WHEN pay.PAY_PAYMENT_METHOD_TYPE_ID IN('debit_card') THEN 1 ELSE 0 END) AS SUM_TD,-- TD            
            SUM(CASE WHEN pay.PAY_PAYMENT_METHOD_TYPE_ID IN('credit_card') THEN 1 ELSE 0 END) AS SUM_TC -- TC 
        FROM `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS` pay
        LEFT JOIN `meli-bi-data.WHOWNER.BT_ORD_ORDER_BONIF` bonif
            ON pay.ORD_ORDER_ID = bonif.ORD_ORDER_ID and pay.SIT_SITE_ID = bonif.SIT_SITE_ID
        WHERE pay.PAY_MOVE_DATE between date'{from_}' and date '{to_}'
        AND TRIM(pay.SIT_SITE_ID) = '{PAIS}'
        AND TRIM(pay.PAY_OPE_MLIBRE_FLAG) = 'Y'
        AND pay.PAY_STATUS_ID = 'approved'
        and pay.ord_order_id is not NULL
        AND pay.pay_total_paid_dol_amt < 10000
        AND COALESCE(bonif.ORD_FVF_BONIF_FLG,True) = False
        AND EXISTS(SELECT 1 FROM {tabla_pivot2bq} u WHERE pay.CUS_CUST_ID_BUY = u.CUS_CUST_ID)
        GROUP BY 1
        )"""
    table_path = 'PAYMENTS'
    bq_app.execute(f"DROP TABLE IF EXISTS {temp45_base_path + table_path}")
    bq_app.execute(query(date_from.strftime("%Y-%m-%d"),date_to.strftime("%Y-%m-%d"),table_path))
    tables.append(table_path)
    
    if ETL_DEBUG:
        print(f"{table_path}: READY")

def etl_tarjetas(gcps_path_in,bq_app,date_from,date_to,tables):
    """
    gcps_path_in: full Google Storage path (gs://meli-marketing/ML/LTV_3M/.../)}
    bq_app: BigQuery object invocated through BigQuery local Library (./google_cloud.py)
    date_from: from what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    date_to: to what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    """
    
    def query(from_, to_, table_path):
        return f"""
            CREATE TABLE {temp45_base_path + table_path} as (
            SELECT
            pay.cus_cust_id_buy as CUS_CUST_ID,
            --SUM(CASE WHEN pay_marketplace_id LIKE 'MP%' THEN 1 ELSE 0 END) AS PAY_APP_MP,
            --SUM(CASE WHEN pay_marketplace_id LIKE 'MELI%' THEN 1 ELSE 0 END) AS PAY_APP_ML,
            SUM(CASE WHEN PAY_PAYMENT_METHOD_TYPE_ID = 'debit_card' THEN 1 ELSE 0 END) as DEBIT_PAY,
            SUM(CASE WHEN PAY_PAYMENT_METHOD_TYPE_ID = 'credit_card' THEN 1 ELSE 0 END) as CREDIT_PAY,
            SUM(CASE WHEN PAY_PAYMENT_METHOD_TYPE_ID = 'account_money' THEN 1 ELSE 0 END) as ACCOUNT_PAY,
            MAX(CASE WHEN bin.COUNTRY <> 'ARGENTINA' THEN 1 ELSE 0 END) as TURISTA,
            MAX(CASE WHEN (bin.CATEGORY in ('CLASSIC','GOLD','BUSINESS')) THEN 1
                    WHEN (bin.CATEGORY = 'PLATINUM') THEN 2
                    WHEN (bin.CATEGORY IN ('SIGNATURE', 'INFINITE','CENTURION')) THEN 3
                    ELSE 0 END) as TIPO_TARJETA,
            MAX(CASE WHEN bin.CARD_TYPE = 'PREPAID' THEN 1 ELSE 0 END) as PREPAID

            FROM `meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS` pay

            left JOIN `meli-marketing.MODELLING.BINES_TARJETA_FULL` bin
            ON bin.BIN = CAST(pay.pay_ccd_first_six_digits as NUMERIC)
            AND bin.CARD_TYPE = 'CREDIT'

            WHERE TRIM(pay.sit_site_id) = '{PAIS}'
                AND pay.PAY_STATUS_ID = 'approved' 
                AND pay.tpv_flag = 1 
                AND pay.pay_move_date BETWEEN DATE '{from_}' AND DATE '{to_}'
                AND EXISTS(SELECT 1 FROM {tabla_pivot2bq} u WHERE pay.cus_cust_id_buy = u.CUS_CUST_ID)
            GROUP BY 1)"""
    table_path = 'TARJETAS'
    bq_app.execute(f"DROP TABLE IF EXISTS {temp45_base_path + table_path}")
    bq_app.execute(query(date_from.strftime("%Y-%m-%d"),date_to.strftime("%Y-%m-%d"),table_path))
    tables.append(table_path)
    
    if ETL_DEBUG:
        print(f"{table_path}: READY")

def etl_installs(gcps_path_in,bq_app,date_from,date_to,tables):
    """
    gcps_path_in: full Google Storage path (gs://meli-marketing/ML/LTV_3M/.../)}
    bq_app: BigQuery object invocated through BigQuery local Library (./google_cloud.py)
    date_from: from what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    date_to: to what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    """
    
    def query(from_, to_, table_path):
        return f"""
            CREATE TABLE {temp45_base_path + table_path} as (
            WITH ADJUST AS (SELECT CUS_CUST_ID,
                DATE_DIFF(DATE'{to_}',MAX(INSTALL_DT), DAY) as days_last_install_ml,
                DATE_DIFF(DATE'{to_}',MIN(INSTALL_DT), DAY) as days_first_install_ml,
                MAX(OS_NAME) as tipo_device
            FROM `meli-marketing.APPINSTALL.BT_INSTALL_EVENT_CUST` ad
            WHERE INSTALL_DT <= DATE'{to_}'
                AND BU = 'ML'
                AND TRIM(SIT_SITE_ID) = '{PAIS}'
                AND EXISTS(SELECT 1 FROM {tabla_pivot2bq} u WHERE ad.CUS_CUST_ID = u.CUS_CUST_ID)
            group by 1)
            , APP_DEVICES as (
                SELECT a.CUS_CUST_ID,
                    DATE_DIFF(DATE'{to_}',MAX(DATE_CREATED), DAY) as days_last_install_ml,
                    DATE_DIFF(DATE'{to_}',MIN(DATE_CREATED), DAY) as days_first_install_ml,
                    MIN(PLATFORM) as tipo_device
                FROM meli-bi-data.WHOWNER.APP_DEVICES a

                WHERE SIT_SITE_ID = '{PAIS}'
                    AND ACTIVE = 'YES'
                    AND MARKETPLACE IN ('MERCADOLIBRE') AND PLATFORM IN ('ios','android')
                    AND TOKEN IS NOT NULL
                    AND status IN ('ACTIVE','NOT_ENGAGED')
                    AND date_created <= DATE '2019-11-01'
                    AND EXISTS(SELECT 1 FROM {tabla_pivot2bq} u WHERE a.CUS_CUST_ID = u.CUS_CUST_ID) 
                GROUP BY 1),
            TOTAL AS (
                SELECT 
                ADJUST.CUS_CUST_ID,
                ADJUST.days_last_install_ml,
                ADJUST.days_first_install_ml,
                ADJUST.tipo_device

                FROM ADJUST 
                UNION ALL 
                SELECT 
                APP_DEVICES.CUS_CUST_ID,
                APP_DEVICES.days_last_install_ml,
                APP_DEVICES.days_first_install_ml,
                APP_DEVICES.tipo_device
                FROM APP_DEVICES
            )
            SELECT 
            TOTAL.CUS_CUST_ID,
            MAX(TOTAL.days_last_install_ml) as days_last_install_ml,
            MIN(TOTAL.days_first_install_ml) as days_first_install_ml,
            MAX(TOTAL.tipo_device) as tipo_device

            FROM TOTAL
            GROUP BY 1
            )"""
    table_path = 'INSTALLS'
    bq_app.execute(f"DROP TABLE IF EXISTS {temp45_base_path + table_path}")
    bq_app.execute(query(date_from.strftime("%Y-%m-%d"),date_to.strftime("%Y-%m-%d"),table_path))
    tables.append(table_path)
    
    if ETL_DEBUG:
        print(f"{table_path}: READY")

def etl_visits(gcps_path_in,bq_app,date_from,date_to,tables):
    """
    gcps_path_in: full Google Storage path (gs://meli-marketing/ML/LTV_3M/.../)}
    bq_app: BigQuery object invocated through BigQuery local Library (./google_cloud.py)
    date_from: from what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    date_to: to what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    """
    
    def query(from_, to_, table_path):
        return f"""
            CREATE TABLE {temp45_base_path + table_path} as (
            SELECT CUS_CUST_ID,
            DATE_DIFF(DATE'{to_}',MAX(LOG_DATE), DAY) as recency_date_90d,
            DATE_DIFF(DATE'{to_}',MIN(LOG_DATE), DAY) as fist_date_90d,
            DATE_DIFF(DATE'{to_}',MAX(CASE WHEN LOG_DATE >= DATE_SUB(DATE '{to_}', INTERVAL 30 DAY) THEN LOG_DATE ELSE NULL END), DAY) as recency_date,
            DATE_DIFF(DATE'{to_}',MIN(CASE WHEN LOG_DATE >= DATE_SUB(DATE '{to_}', INTERVAL 30 DAY) THEN LOG_DATE ELSE NULL END), DAY) as first_date,
            SUM(CASE WHEN LOG_DATE >= DATE_SUB(DATE '{to_}', INTERVAL 30 DAY) THEN 1 ELSE 0 END) as cant_dias_active,
            COUNT(1) as cant_dias_active_90d,
            SUM(CASE WHEN LOG_DATE >= DATE_SUB(DATE '{to_}', INTERVAL 30 DAY) THEN BOOKMARK_ITEM_COUNT ELSE 0 END) as bookmarks,

            from `meli-marketing.MODELLING.LK_EVENT_CUSTOMER_ITEMS_AGG` ag
            where SIT_SITE_ID = '{PAIS}'
                AND LOG_DATE >= DATE_SUB(DATE '{to_}', INTERVAL 90 DAY)
                AND LOG_DATE <= DATE '{to_}'
                AND EXISTS(SELECT 1 FROM {tabla_pivot2bq} u WHERE ag.CUS_CUST_ID = u.CUS_CUST_ID)
            GROUP BY CUS_CUST_ID
            )"""
    table_path = 'VISITAS_SHORT'
    bq_app.execute(f"DROP TABLE IF EXISTS {temp45_base_path + table_path}")
    bq_app.execute(query(date_from.strftime("%Y-%m-%d"),date_to.strftime("%Y-%m-%d"),table_path))
    tables.append(table_path)
    
    if ETL_DEBUG:
        print(f"{table_path}: READY")

def etl_target(gcps_path_in,bq_app,date_from,date_to,tables):
    """
    gcps_path_in: full Google Storage path (gs://meli-marketing/ML/LTV_3M/.../)}
    bq_app: BigQuery object invocated through BigQuery local Library (./google_cloud.py)
    date_from: from what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    date_to: to what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    """
    
    def query(from_, to_, table_path):
        return f"""
            CREATE TABLE {temp45_base_path + table_path} as (
            SELECT a11.CUS_CUST_ID_BUY as CUS_CUST_ID,
            COUNT(DISTINCT ORDER_CREATED_DT) as FREQ_TARGET,
            SUM(a11.GMV_USD) as GMV_TARGET,

            FROM `meli-marketing.MODELLING.V_BT_ORD_ORDER` a11 
            WHERE a11.order_status = 'paid' AND a11.SIT_SITE_ID = '{PAIS}'
            AND a11.GMV_USD	< 10000
            AND a11.ORDER_CLOSED_DT between date'{from_}' and date '{to_}'
            AND a11.ORDER_CREATED_DT between DATE_SUB(ORDER_CLOSED_DT, INTERVAL 4 DAY) AND ORDER_CREATED_DT
            AND order_status = 'paid' and FLAG_AUTO_OFFER = false and FLAG_BONIF=False
            AND EXISTS(SELECT 1 FROM {tabla_pivot2bq} u WHERE a11.CUS_CUST_ID_BUY = u.CUS_CUST_ID)
            GROUP BY a11.CUS_CUST_ID_BUY
            )"""
    table_path = 'TARGET'
    bq_app.execute(f"DROP TABLE IF EXISTS {temp45_base_path + table_path}")
    bq_app.execute(query(date_from.strftime("%Y-%m-%d"),date_to.strftime("%Y-%m-%d"),table_path))
    tables.append(table_path)
    
    if ETL_DEBUG:
        print(f"{table_path}: READY")

def etl_IPT(gcps_path_in,bq_app,date_from,date_to,tables):
    """
    gcps_path_in: full Google Storage path (gs://meli-marketing/ML/LTV_3M/.../)}
    bq_app: BigQuery object invocated through BigQuery local Library (./google_cloud.py)
    date_from: from what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    date_to: to what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    """
    
    def query(from_, to_, table_path):
        return f"""
        CREATE TABLE {temp45_base_path + table_path} as (
        WITH INTERVALS as (
        select long.cust as CUS_CUST_ID,
        DATE_DIFF(long.datee, lag(long.datee,1) over (partition by long.cust order by long.datee), DAY) as IPT_LONG,
        DATE_DIFF(short.datee, lag(short.datee,1) over (partition by short.cust order by short.datee), DAY) as IPT_SHORT

        FROM (SELECT DISTINCT a11.ORD_BUYER.ID cust, a11.ORD_CLOSED_DT datee
        FROM `meli-bi-data.WHOWNER.BT_ORD_ORDERS` a11
            LEFT JOIN `meli-bi-data.WHOWNER.BT_ORD_ORDER_BONIF` bonif
            ON a11.ORD_ORDER_ID = bonif.ORD_ORDER_ID and a11.SIT_SITE_ID = bonif.SIT_SITE_ID
        WHERE a11.ord_status = 'paid' AND a11.SIT_SITE_ID = '{PAIS}'

        AND a11.ORD_CLOSED_DT between date'{from_}' and date '{to_}'
        AND a11.ORD_CREATED_DT between DATE_SUB(ORD_CLOSED_DT, INTERVAL 4 DAY) AND ORD_CREATED_DT
        AND ord_status = 'paid' and COALESCE(a11.ORD_AUTO_OFFER_FLG,False) = false and COALESCE(bonif.ORD_FVF_BONIF_FLG,True) = False
        AND EXISTS(SELECT 1 FROM {tabla_pivot2bq} a WHERE a.CUS_CUST_ID = a11.ORD_BUYER.ID)) long

        LEFT JOIN
        (SELECT DISTINCT a11.ORD_BUYER.ID cust, a11.ORD_CLOSED_DT datee
        FROM `meli-bi-data.WHOWNER.BT_ORD_ORDERS` a11
            LEFT JOIN `meli-bi-data.WHOWNER.BT_ORD_ORDER_BONIF` bonif
            ON a11.ORD_ORDER_ID = bonif.ORD_ORDER_ID and a11.SIT_SITE_ID = bonif.SIT_SITE_ID
        WHERE a11.ord_status = 'paid' AND a11.SIT_SITE_ID = '{PAIS}'

        AND a11.ORD_CLOSED_DT between DATE_SUB(date'{to_}', INTERVAL 60 DAY) and date '{to_}'
        AND a11.ORD_CREATED_DT between DATE_SUB(ORD_CLOSED_DT, INTERVAL 4 DAY) AND ORD_CREATED_DT
        AND ord_status = 'paid' and COALESCE(a11.ORD_AUTO_OFFER_FLG,False) = false and COALESCE(bonif.ORD_FVF_BONIF_FLG,True) = False
        AND EXISTS(SELECT 1 FROM {tabla_pivot2bq} a WHERE a.CUS_CUST_ID = a11.ORD_BUYER.ID)) short
        on long.cust = short.cust and long.datee = short.datee
        ), ipt_long as (
        SELECT CUS_CUST_ID,
        SUM(LOG10(IPT_LONG)) as IPT_sum_long,
        STDDEV(LOG10(IPT_LONG)) as IPT_std_long,
        AVG(LOG10(IPT_LONG)) as IPT_mean_long
        FROM INTERVALS 
        GROUP BY 1
        HAVING COUNT(IPT_LONG) > 1)
        , ipt_short as(
        SELECT CUS_CUST_ID,
        SUM(LOG10(IPT_SHORT)) as IPT_sum_short,
        AVG(LOG10(IPT_SHORT)) as IPT_mean_short, 
        
        STDDEV(LOG10(IPT_SHORT)) as IPT_std_short
        FROM INTERVALS 
        GROUP BY 1
        HAVING COUNT(IPT_SHORT) > 1)
        
        SELECT a.CUS_CUST_ID,
        s.* EXCEPT(CUS_CUST_ID),
        l.* EXCEPT(CUS_CUST_ID)
        
        FROM {tabla_pivot2bq} a 
        LEFT JOIN ipt_short s on s.CUS_CUST_ID = a.CUS_CUST_ID
        LEFT JOIN ipt_long l on l.CUS_CUST_ID = a.CUS_CUST_ID
        )"""
    table_path = 'IPT'
    bq_app.execute(f"DROP TABLE IF EXISTS {temp45_base_path + table_path}")
    bq_app.execute(query(date_from.strftime("%Y-%m-%d"),date_to.strftime("%Y-%m-%d"),table_path))
    tables.append(table_path)
    
    if ETL_DEBUG:
        print(f"{table_path}: READY")

def etl_locations(gcps_path_in,bq_app,date_from,date_to,tables):
    """
    gcps_path_in: full Google Storage path (gs://meli-marketing/ML/LTV_3M/.../)}
    bq_app: BigQuery object invocated through BigQuery local Library (./google_cloud.py)
    date_from: from what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    date_to: to what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    """
    
    def query(from_, to_, table_path):
        
        countries = {'MLA':'AR',
                    'MLB':'BR',
                    'MLC':'CL',
                    'MCO':'CO',
                    'MLM':'MX'}
         
        
        return f"""
            CREATE TABLE {temp45_base_path + table_path} as (
            SELECT CUS_CUST_ID_BUY as CUS_CUST_ID, NORMALIZE_AND_CASEFOLD(SHP_ADD_STATE_NAME, NFD) as location, SAFE_CAST(SHP_ADD_ZIP_CODE AS int64) as SHP_ADD_ZIP_CODE,
            COUNT(*) as MOST_SEEN

            FROM `meli-marketing.MODELLING.V_BT_ORD_ORDER` ord
            INNER JOIN `meli-bi-data.WHOWNER.BT_SHP_SHIPMENTS` shp
                ON ord.ORDER_SHIPPING_ID = shp.SHP_SHIPMENT_ID
                AND shp.SIT_SITE_ID = ord.SIT_SITE_ID
                AND shp.SHP_Type = 'forward'
                AND shp.SHP_DATE_CREATED_ID BETWEEN DATE'{from_}' AND DATE'{to_}'
            LEFT JOIN `meli-bi-data.WHOWNER.LK_SHP_ADDRESS` shp_add
                on shp_add.SHP_ADD_ID = shp.SHP_RECEIVER_ADDRESS
                and SHP_ADD_COUNTRY_ID = '{countries[PAIS]}'

            WHERE ord.order_status = 'paid' AND ord.SIT_SITE_ID = '{PAIS}'
                
                AND ord.GMV_USD	< 10000
                AND ord.ORDER_CLOSED_DT between DATE_SUB(date'{from_}', INTERVAL 3 DAY) and date '{to_}'
                AND ORDER_CREATED_DT between DATE_SUB(ORDER_CLOSED_DT, INTERVAL 4 DAY) AND ORDER_CREATED_DT
                AND order_status = 'paid' and FLAG_AUTO_OFFER = false and FLAG_BONIF=False
                AND EXISTS(SELECT 1 FROM {tabla_pivot2bq} a WHERE a.CUS_CUST_ID = ord.CUS_CUST_ID_BUY) 
                GROUP BY 1,2,3
            qualify row_number() over (partition by ord.CUS_CUST_ID_BUY order by MOST_SEEN desc) = 1
            )"""
    table_path = 'LOCATIONS'
    bq_app.execute(f"DROP TABLE IF EXISTS {temp45_base_path + table_path}")
    bq_app.execute(query(date_from.strftime("%Y-%m-%d"),date_to.strftime("%Y-%m-%d"),table_path))
    tables.append(table_path)
    
    if ETL_DEBUG:
        print(f"{table_path}: READY")

def etl_mp_prepaid(gcps_path_in,bq_app,date_from,date_to,tables):
    """
    gcps_path_in: full Google Storage path (gs://meli-marketing/ML/LTV_3M/.../)}
    bq_app: BigQuery object invocated through BigQuery local Library (./google_cloud.py)
    date_from: from what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    date_to: to what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    """
    
    def query(from_, to_, table_path):
        return f"""
                CREATE TABLE {temp45_base_path + table_path} as (
                SELECT a.cus_cust_id as CUS_CUST_ID,
                    '1' as HAS_PREPAID,
                    DATE_DIFF(DATE'{to_}',MAX(CRD.PPD_CARD_CREATION_DT),DAY) as ppd_recency,
                    SUM(PPD_WIT_DOL_AMOUNT) as ppd_sum_money,
                    AVG(PPD_WIT_DOL_AMOUNT) as ppd_mean_money, 
                    MIN(PPD_WIT_DOL_AMOUNT) as ppd_min_money,
                    MAX(PPD_WIT_DOL_AMOUNT) as ppd_max_money,
                    DATE_DIFF(DATE '{to_}', MIN(CRD.PPD_CARD_ACTIVATION_DT), DAY) as ppd_first_activation

                FROM meli-bi-data.WHOWNER.LK_PPD_ACCOUNT a
                
                LEFT JOIN `meli-bi-data.WHOWNER.BT_PPD_WITHDRAW` w
                  ON a.ppd_account_id = w.ppd_account_id
                  AND w.ppd_wit_status = 'approved'
                  AND w.sit_site_id = 'MLA'
                  AND PPD_WIT_CREATION_DATE between DATE_SUB(DATE'{from_}', INTERVAL 180 DAY) AND DATE'{from_}'
                
                INNER JOIN `meli-bi-data.WHOWNER.LK_PPD_CARD` CRD
                    ON a.PPD_ACCOUNT_ID = CRD.PPD_ACCOUNT_ID
                    AND PPD_CARD_CREATION_DT <= date '{to_}'

                WHERE a.PPD_ACCOUNT_STATUS = 'active'
                  AND a.PPD_ACCOUNT_TAGS LIKE '%prod%' 
                  AND a.SIT_SITE_ID = '{PAIS}'
                  AND EXISTS(select 1 from {tabla_pivot2bq} p where a.cus_cust_id = p.cus_cust_id)

                GROUP BY 1
                )
                """
    table_path = 'MP_PREPAID'
    bq_app.execute(f"DROP TABLE IF EXISTS {temp45_base_path + table_path}")
    bq_app.execute(query(date_from.strftime("%Y-%m-%d"),date_to.strftime("%Y-%m-%d"),table_path))
    tables.append(table_path)
    
    if ETL_DEBUG:
        print(f"{table_path}: READY")

def etl_mp_mgm(gcps_path_in,bq_app,date_from,date_to,tables):
    """
    gcps_path_in: full Google Storage path (gs://meli-marketing/ML/LTV_3M/.../)}
    bq_app: BigQuery object invocated through BigQuery local Library (./google_cloud.py)
    date_from: from what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    date_to: to what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    """
    
    def query(to_, table_path):
        return f"""
                CREATE TABLE {temp45_base_path + table_path} as (
                WITH INVESTING as (
                    SELECT nb.CUS_CUST_ID,
                    (CASE ASSET_MGMT_STAT_ID WHEN 'investing' then 'investing' else 'not_investing' END) as asset_mgmt_stat_id
                    FROM {tabla_pivot2bq} nb

                    LEFT JOIN `meli-bi-data.WHOWNER.LK_MP_ASSET_MGMT_USERS` a ON nb.CUS_CUST_ID = a.CUS_CUST_ID

                    WHERE SIT_SITE_ID = '{PAIS}' 
                    AND ASSET_MGMT_STAT_INVESTING_DT <= '{to_}')
                    SELECT nb.CUS_CUST_ID,
                    COALESCE(INVESTING.asset_mgmt_stat_id, 'not_investing') as asset_mgmt_stat_id
                    FROM {tabla_pivot2bq} nb
                    LEFT JOIN INVESTING on INVESTING.CUS_CUST_ID = nb.CUS_CUST_ID
                    )"""
    table_path = 'MP_MGM'
    bq_app.execute(f"DROP TABLE IF EXISTS {temp45_base_path + table_path}")
    bq_app.execute(query(date_to.strftime("%Y-%m-%d"),table_path))
    tables.append(table_path)
    
    if ETL_DEBUG:
        print(f"{table_path}: READY")

def etl_mp_app(gcps_path_in,bq_app,date_from,date_to,tables):
    """
    gcps_path_in: full Google Storage path (gs://meli-marketing/ML/LTV_3M/.../)}
    bq_app: BigQuery object invocated through BigQuery local Library (./google_cloud.py)
    date_from: from what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    date_to: to what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    """
    
    def query(to_, table_path):
        return f"""
            CREATE TABLE {temp45_base_path + table_path} as (
            WITH ADJUST AS (SELECT CUS_CUST_ID,
                DATE_DIFF(DATE'{to_}',MAX(INSTALL_DT), DAY) as days_last_install_mp,
                DATE_DIFF(DATE'{to_}',MIN(INSTALL_DT), DAY) as days_first_install_mp,
                MAX(OS_NAME) as tipo_device
            FROM `meli-marketing.APPINSTALL.BT_INSTALL_EVENT_CUST` ad
            WHERE INSTALL_DT <= DATE'{to_}'
                AND BU = 'MP'
                AND TRIM(SIT_SITE_ID) = '{PAIS}'
                AND EXISTS(SELECT 1 FROM {tabla_pivot2bq} u WHERE ad.CUS_CUST_ID = u.CUS_CUST_ID)
            group by 1)
            , APP_DEVICES as (
                SELECT a.CUS_CUST_ID,
                    DATE_DIFF(DATE'{to_}',MAX(DATE_CREATED), DAY) as days_last_install_mp,
                    DATE_DIFF(DATE'{to_}',MIN(DATE_CREATED), DAY) as days_first_install_mp,
                    MIN(PLATFORM) as tipo_device
                FROM meli-bi-data.WHOWNER.APP_DEVICES a

                WHERE SIT_SITE_ID = '{PAIS}'
                    AND ACTIVE = 'YES'
                    AND MARKETPLACE IN ('MERCADOPAGO') AND PLATFORM IN ('ios','android')
                    AND TOKEN IS NOT NULL
                    AND status IN ('ACTIVE','NOT_ENGAGED')
                    AND date_created <= DATE '2019-11-01'
                    AND EXISTS(SELECT 1 FROM {tabla_pivot2bq} u WHERE a.CUS_CUST_ID = u.CUS_CUST_ID) 
                GROUP BY 1),
            TOTAL AS (
                SELECT 
                ADJUST.CUS_CUST_ID,
                ADJUST.days_last_install_mp,
                ADJUST.days_first_install_mp,
                ADJUST.tipo_device

                FROM ADJUST 
                UNION ALL 
                SELECT 
                APP_DEVICES.CUS_CUST_ID,
                APP_DEVICES.days_last_install_mp,
                APP_DEVICES.days_first_install_mp,
                APP_DEVICES.tipo_device
                FROM APP_DEVICES
            )
            SELECT 
            TOTAL.CUS_CUST_ID,
            MAX(TOTAL.days_last_install_mp) as days_last_install_mp,
            MIN(TOTAL.days_first_install_mp) as days_first_install_mp

            FROM TOTAL
            GROUP BY TOTAL.CUS_CUST_ID
            )"""
    table_path = 'MP_APP'
    bq_app.execute(f"DROP TABLE IF EXISTS {temp45_base_path + table_path}")
    bq_app.execute(query(date_to.strftime("%Y-%m-%d"), table_path))
    tables.append(table_path)
    
    if ETL_DEBUG:
        print(f"{table_path}: READY")

def etl_reg_queries(gcps_path_in,bq_app,date_from,date_to,tables):
    """
    gcps_path_in: full Google Storage path (gs://meli-marketing/ML/LTV_3M/.../)}
    bq_app: BigQuery object invocated through BigQuery local Library (./google_cloud.py)
    date_from: from what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    date_to: to what date you want to run the query (meli-marketing.WHOWNER.BT_ORD_ORDERS->ORDER_CLOSED_DT)
    """
    
    def query(table_path):
        return f"""
        CREATE TABLE {temp45_base_path + table_path} as (
        SELECT a.CUS_CUST_ID,
        REG_DATA_TYPE,
        (CASE 
            WHEN REG_CUST_SCHOOL in ('POS-GRADUACAO COMPLETA','DOUTORADO COMPLETO','MESTRADO COMPLETO') THEN 4
            WHEN REG_CUST_SCHOOL in ('SUPERIOR COMPLETO','SUPERIOR INCOMPLETO') THEN 3
            WHEN REG_CUST_SCHOOL in ('ENSINO MÉDIO INCOMPLETO','ENSINO MÉDIO COMPLETO','MEDIO COMPLETO','MEDIO INCOMPLETO') THEN 2
            WHEN REG_CUST_SCHOOL in ('ENSINO FUNDAMENTAL INCOMPLETO','ENSINO FUNDAMENTAL COMPLETO','FUNDAMENTAL COMPLETO','FUNDAMENTAL INCOMPLETO') THEN 1
            WHEN REG_CUST_SCHOOL in ('PRIMARIO INCOMPLETO','PRIMARIO COMPLETO') THEN 0
            ELSE NULL END) AS REG_CUST_SCHOOL,
        REG_CUST_GENDER,
        (select max(x) from unnest(REGEXP_EXTRACT_ALL(REPLACE(REPLACE(b.REG_CUST_INCOMES,'.',''),',','.'),r'[\d.]+')) x) as REG_CUST_INCOMES,
        DATE_DIFF(CURRENT_DATE(), CAST(REG_CUST_BIRTHDATE as DATE), YEAR) AS REG_CUST_BIRTHDATE,
        REG_CUST_PROFESSION,
        REG_CUST_MATERIAL_STATUS

        FROM `meli-bi-data.WHOWNER.LK_REG_CUSTOMERS` a
        INNER JOIN `meli-bi-data.WHOWNER.LK_REG_PERSON` b on a.REG_CUST_DOC_TYPE = b.REG_CUST_DOC_TYPE
            AND A.REG_CUST_DOC_NUMBER = B.REG_CUST_DOC_NUMBER
        WHERE a.SIT_SITE_ID = '{PAIS}'
        AND EXISTS(SELECT 1 FROM {tabla_pivot2bq} nb WHERE nb.CUS_CUST_ID = a.CUS_CUST_ID )
        )
        """
    table_path = 'REG_DATA'
    bq_app.execute(f"DROP TABLE IF EXISTS {temp45_base_path + table_path}")
    bq_app.execute(query(table_path))
    tables.append(table_path)
    
    if ETL_DEBUG:
        print(f"{table_path}: READY")
