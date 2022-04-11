import os
import pandas as pd
import s3fs

from melitk.analytics.connectors.core.authentication import Authentication
from melitk.analytics.connectors.teradata import ConnTeradata
from melitk.analytics.connectors.presto import ConnPresto
from melitk.analytics.connectors.hive import ConnHive
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

sys.path.append(os.path.dirname(os.path.expanduser(".")))
# sys.path.append(os.path.dirname(os.path.expanduser("./defines.py")))
sys.path.append(os.path.dirname(os.path.expanduser("../defines.py")))
from defines import *

import dask.dataframe as dd

def full_table(s3_path_in,teradata_app,hive,end_cal_train,start_cal_train,start_cal_test,end_cal_test,sample = False):
    # ME quedo con todas las transacciones de interes
    #s3_path_in -> gcp_path_in
    #teradata_app -> bq_app
    
    teradata_app.drop_table(tabla_pivot2)
    teradata_app.drop_table(tabla_pivot)

    if(sample == True):
        sample_str = "SAMPLE 300000"
    else:
        sample_str = ""
        
    to_ = end_cal_train.strftime("%Y-%m-%d")
    teradata_app.execute("""
                 CREATE MULTISET TABLE """+tabla_pivot2+""",
                      NO BEFORE JOURNAL,
                      NO AFTER JOURNAL,
                      CHECKSUM = DEFAULT,
                      DEFAULT MERGEBLOCKRATIO
                      AS
                      (
                         SELECT a11.CUS_CUST_ID_BUY as CUS_CUST_ID
                         FROM WHOWNER.BT_BIDS a11  
                         WHERE a11.BID_BID_STATUS = 'W' AND a11.PHOTO_ID = 'TODATE' AND a11.SIT_SITE_ID = '"""+PAIS+"""'
                             AND a11.BID_BASE_CURRENT_PRICE < 10000 
                             AND a11.TIM_DAY_WINNING_DATE BETWEEN DATE '"""+start_cal_train.strftime("%Y-%m-%d")+"""' AND DATE '"""+end_cal_train.strftime("%Y-%m-%d")+"""'
                             AND ITE_GMV_FLAG = 1 AND MKT_MARKETPLACE_ID = 'TM' AND COALESCE(a11.BID_FVF_BONIF ,'Y')='N' 
                             AND COALESCE(a11.AUTO_OFFER_FLAG, 0) <> 1 --Mandatory Filter   
                         GROUP BY 1
                         
                         """+sample_str+"""
                       )
                       WITH DATA
                       PRIMARY INDEX ( CUS_CUST_ID );    

                     """)
    
def day_table(s3_path_in,teradata_app,hive,end_cal_train,start_cal_train,start_cal_test,end_cal_test,sample = False):
    # ME quedo con todas las transacciones de interes
    
    teradata_app.drop_table(tabla_pivot2)
    teradata_app.drop_table(tabla_pivot)
        
    teradata_app.execute("""
                 CREATE MULTISET TABLE """+tabla_pivot2+""",
                      NO BEFORE JOURNAL,
                      NO AFTER JOURNAL,
                      CHECKSUM = DEFAULT,
                      DEFAULT MERGEBLOCKRATIO
                      AS
                      (
                         SELECT a11.CUS_CUST_ID_BUY as CUS_CUST_ID
                         FROM WHOWNER.BT_BIDS a11  
                         WHERE a11.BID_BID_STATUS = 'W' AND a11.PHOTO_ID = 'TODATE' AND a11.SIT_SITE_ID = '"""+PAIS+"""'
                             AND a11.BID_BASE_CURRENT_PRICE < 10000 
                             AND a11.TIM_DAY_WINNING_DATE BETWEEN DATE '"""+end_cal_train.strftime("%Y-%m-%d")+"""'-3 AND DATE '"""+end_cal_train.strftime("%Y-%m-%d")+"""' --por si hay algun problema con teradata ese dia
                             AND ITE_GMV_FLAG = 1 AND MKT_MARKETPLACE_ID = 'TM' AND COALESCE(a11.BID_FVF_BONIF ,'Y')='N' 
                             AND COALESCE(a11.AUTO_OFFER_FLAG, 0) <> 1 --Mandatory Filter   
                         GROUP BY 1
                       )
                       WITH DATA
                       PRIMARY INDEX ( CUS_CUST_ID );    

                     """) 
    
def country_raw(s3_path_in,teradata_app,hive,end_cal_train,start_cal_train,start_cal_test,end_cal_test):
    # ME bajo la data cruda
    def query(pais,from_,to_):
        return """SELECT a11.TIM_DAY_WINNING_DATE as datee, a11.CUS_CUST_ID_BUY as cust, SUM(a11.BID_BASE_CURRENT_PRICE * a11.BID_QUANTITY_OK) AS sales,
                    SUM(a11.BID_QUANTITY_OK) AS SI,
                    COUNT(DISTINCT CASE WHEN crt_purchase_id IS NOT NULL THEN crt_purchase_id ELSE ord_order_id END) as ordenes
                    FROM WHOWNER.BT_BIDS a11  
                    WHERE a11.BID_BID_STATUS = 'W' AND a11.PHOTO_ID = 'TODATE' AND a11.SIT_SITE_ID = '""" +  PAIS + """' 
                        AND a11.BID_BASE_CURRENT_PRICE < 10000 AND a11.TIM_DAY_WINNING_DATE BETWEEN DATE '""" + from_ + """' AND DATE '""" + to_ + """' 
                        AND ITE_GMV_FLAG = 1 AND MKT_MARKETPLACE_ID = 'TM' AND COALESCE(a11.BID_FVF_BONIF ,'Y')='N' 
                        AND COALESCE(a11.AUTO_OFFER_FLAG, 0) <> 1 --Mandatory Filter 
                        AND EXISTS(SELECT 1 FROM """+tabla_pivot2+""" a WHERE a.CUS_CUST_ID = a11.CUS_CUST_ID_BUY)
                    GROUP BY 1,2
                    ORDER BY 2,1 ASC"""

    teradata_app.fast_export(query(PAIS,start_cal_train.strftime("%Y-%m-%d"),end_cal_train.strftime("%Y-%m-%d")),"s3://" + fda_path + s3_path_in + "Raw_Extend_"+PAIS+"_1.csv")
                        
def shipping(s3_path_in,teradata_app,hive,end_cal_train,start_cal_train,start_cal_test,end_cal_test):
    # Me traigo calificaciones, tipo de device y CPG
    def query(from_,to_):
        return """
            SELECT  a11.CUS_CUST_ID_BUY as CUS_CUST_ID,
                    --COUNT(distinct a11.crt_purchase_id) CANT_COMPRAS_CARRITO,
                    --COUNT(ITE_SUPERMARKET_FLAG) as CPG,
                    --COUNT(distinct CASE WHEN a11.TIM_DAY_WINNING_DATE >= DATE '""" + to_ + """' - 30 THEN cus_cust_id_sel ELSE NULL END) vendedores_distintos_30,
                    --COUNT(distinct CASE WHEN (a11.TIM_DAY_WINNING_DATE >= DATE '""" + to_ + """' - 60) AND (a11.TIM_DAY_WINNING_DATE <= DATE '""" + to_ + """' - 30) THEN cus_cust_id_sel ELSE NULL END) vendedores_distintos_60_30,
                    --COUNT(distinct CASE WHEN (a11.TIM_DAY_WINNING_DATE >= DATE '""" + to_ + """' - 90) AND (a11.TIM_DAY_WINNING_DATE <= DATE '""" + to_ + """' - 60) THEN cus_cust_id_sel ELSE NULL END) vendedores_distintos_90_60,
                    --COUNT(distinct cus_cust_id_sel) vendedores_distintos_long,
                    --SUM (CASE WHEN mob.mapp_mobile_flag = 'Desktop' THEN 1 ELSE 0 END) DESKTOP_PURCHASE,
                    --SUM(CASE WHEN mob.mapp_mobile_flag = 'Mobile-WEB' THEN 1 else 0 END) MOBILE_WEB_PURCHASE,
                    --SUM(CASE WHEN mob.mapp_mobile_flag= 'Mobile-APP' AND MAPP_APP_desc = 'android' THEN 1 else 0 END) ANDROID_PURCHASE,
                    --SUM(CASE WHEN mob.mapp_mobile_flag = 'Mobile-APP' AND MAPP_APP_desc = 'iphone' THEN 1 else 0 END) IOS_PURCHASE,
                    --COUNT(DISTINCT CASE WHEN ITE_TIPO_PROD = 'N' THEN ORD_ORDER_ID ELSE NULL END) AS CANTIDAD_COMPRAS_ITEMS_NUEVOS,
                    --COUNT(DISTINCT CASE WHEN ITE_TIPO_PROD = 'U' THEN ORD_ORDER_ID ELSE NULL END) AS CANTIDAD_COMPRAS_ITEMS_USADOS

                    COUNT(DISTINCT CASE WHEN COALESCE(SHP_FREE_FLAG_ID,0) >= 1 THEN ORD_ORDER_ID ELSE NULL END) AS CANTIDAD_COMPRAS_FREE_SHIPPING,
                    COUNT(DISTINCT CASE WHEN ship.SHP_SHIPMENT_ID IS NULL THEN ORD_ORDER_ID ELSE NULL END) AS CANTIDAD_COMPRAS_NO_ENVIOS,
                    COUNT(DISTINCT CASE WHEN COALESCE(ship.SHP_CART_TYPE,0) = 0 THEN ORD_ORDER_ID ELSE NULL END) AS CANTIDAD_COMPRAS_NO_CARRITO,
                    COUNT(DISTINCT CASE WHEN ship.SHP_PICKING_TYPE_ID = 'drop_off' THEN ORD_ORDER_ID ELSE NULL END) AS CANTIDAD_COMPRAS_ENVIO_DROP_OFF
                    
            FROM BT_BIDS a11

            INNER JOIN WHOWNER.BT_SHP_SHIPMENTS ship
                      ON a11.SHP_SHIPMENT_ID = ship.SHP_SHIPMENT_ID
                      AND a11.SIT_SITE_ID = ship.SIT_SITE_ID
                      AND ship.SHP_Type = 'forward'
                      AND ship.SHP_DATE_CREATED_ID BETWEEN DATE'""" + from_ + """' AND DATE'""" + to_ + """'
                      
            --LEFT JOIN WHOWNER.LK_MAPP_MOBILE mob
            --    ON (COALESCE(a11.MAPP_APP_ID, '-1') = mob.MAPP_APP_ID)

            WHERE a11.BID_BID_STATUS = 'W'
                    AND a11.PHOTO_ID = 'TODATE'
                    AND a11.SIT_SITE_ID = '""" +  PAIS + """'
                    AND a11.ITE_GMV_FLAG = 1
                    AND a11.MKT_MARKETPLACE_ID = 'TM'
                    AND COALESCE(a11.BID_FVF_BONIF ,'Y')='N'
                    AND COALESCE(a11.AUTO_OFFER_FLAG, 0) <> 1 --Mandatory Filter
                    --AND NOT EXISTS (SELECT 1 FROM WHOWNER.LK_ORD_ORDER_TAG OT WHERE a11.ORD_ORDER_ID = OT.ORD_ORDER_ID AND FLAG_AUTO = 'YES')
                    AND a11.BID_BASE_CURRENT_PRICE < 10000
                    AND a11.TIM_DAY_WINNING_DATE BETWEEN DATE '""" + from_ + """' AND DATE '""" + to_ + """'
                    AND EXISTS(SELECT 1 FROM """+tabla_pivot2+""" u WHERE a11.CUS_CUST_ID_BUY = u.CUS_CUST_ID)
            GROUP BY 1
    """   
    teradata_app.fast_export(query(start_cal_train.strftime("%Y-%m-%d"),end_cal_train.strftime("%Y-%m-%d")) ,
                     "s3://"+fda_path+s3_path_in + "shipping.csv") 
    
def compras_compras(s3_path_in,teradata_app,hive,end_cal_train,start_cal_train,start_cal_test,end_cal_test):
    # Me traigo calificaciones, tipo de device y CPG
    def query(from_,to_):
        return """
            SELECT  a11.CUS_CUST_ID_BUY as CUS_CUST_ID,
                    COUNT(distinct a11.crt_purchase_id) CANT_COMPRAS_CARRITO,
                    COUNT(ITE_SUPERMARKET_FLAG) as CPG,
                    COUNT(distinct CASE WHEN a11.TIM_DAY_WINNING_DATE >= DATE '""" + to_ + """' - 30 THEN cus_cust_id_sel ELSE NULL END) vendedores_distintos_30,
                    COUNT(distinct CASE WHEN (a11.TIM_DAY_WINNING_DATE >= DATE '""" + to_ + """' - 60) AND (a11.TIM_DAY_WINNING_DATE <= DATE '""" + to_ + """' - 30) THEN cus_cust_id_sel ELSE NULL END) vendedores_distintos_60_30,
                    COUNT(distinct CASE WHEN (a11.TIM_DAY_WINNING_DATE >= DATE '""" + to_ + """' - 90) AND (a11.TIM_DAY_WINNING_DATE <= DATE '""" + to_ + """' - 60) THEN cus_cust_id_sel ELSE NULL END) vendedores_distintos_90_60,
                    COUNT(distinct cus_cust_id_sel) vendedores_distintos_long,
                    SUM (CASE WHEN mob.mapp_mobile_flag = 'Desktop' THEN 1 ELSE 0 END) DESKTOP_PURCHASE,
                    SUM(CASE WHEN mob.mapp_mobile_flag = 'Mobile-WEB' THEN 1 else 0 END) MOBILE_WEB_PURCHASE,
                    SUM(CASE WHEN mob.mapp_mobile_flag= 'Mobile-APP' AND MAPP_APP_desc = 'android' THEN 1 else 0 END) ANDROID_PURCHASE,
                    SUM(CASE WHEN mob.mapp_mobile_flag = 'Mobile-APP' AND MAPP_APP_desc = 'iphone' THEN 1 else 0 END) IOS_PURCHASE,
                    COUNT(DISTINCT CASE WHEN ITE_TIPO_PROD = 'N' THEN ORD_ORDER_ID ELSE NULL END) AS CANTIDAD_COMPRAS_ITEMS_NUEVOS,
                    COUNT(DISTINCT CASE WHEN ITE_TIPO_PROD = 'U' THEN ORD_ORDER_ID ELSE NULL END) AS CANTIDAD_COMPRAS_ITEMS_USADOS
                    
            FROM BT_BIDS a11
                      
            LEFT JOIN WHOWNER.LK_MAPP_MOBILE mob
                ON (COALESCE(a11.MAPP_APP_ID, '-1') = mob.MAPP_APP_ID)

            WHERE a11.BID_BID_STATUS = 'W'
                    AND a11.PHOTO_ID = 'TODATE'
                    AND a11.SIT_SITE_ID = '""" +  PAIS + """'
                    AND a11.ITE_GMV_FLAG = 1
                    AND a11.MKT_MARKETPLACE_ID = 'TM'
                    AND COALESCE(a11.BID_FVF_BONIF ,'Y')='N'
                    AND COALESCE(a11.AUTO_OFFER_FLAG, 0) <> 1 --Mandatory Filter
                    --AND NOT EXISTS (SELECT 1 FROM WHOWNER.LK_ORD_ORDER_TAG OT WHERE a11.ORD_ORDER_ID = OT.ORD_ORDER_ID AND FLAG_AUTO = 'YES')
                    AND a11.BID_BASE_CURRENT_PRICE < 10000
                    AND a11.TIM_DAY_WINNING_DATE BETWEEN DATE '""" + from_ + """' AND DATE '""" + to_ + """'
                    AND EXISTS(SELECT 1 FROM """+tabla_pivot2+""" u WHERE a11.CUS_CUST_ID_BUY = u.CUS_CUST_ID)
            GROUP BY 1
    """   
    teradata_app.fast_export(query(start_cal_train.strftime("%Y-%m-%d"),end_cal_train.strftime("%Y-%m-%d")) ,
                     "s3://"+fda_path+s3_path_in + "compras_compras.csv") 
    
def compras(s3_path_in,teradata_app,hive,end_cal_train,start_cal_train,start_cal_test,end_cal_test):
    # Me traigo calificaciones, tipo de device y CPG
    def query(from_,to_):
        return """
            SELECT  a11.CUS_CUST_ID_BUY as CUS_CUST_ID,
                    COUNT(distinct a11.crt_purchase_id) CANT_COMPRAS_CARRITO,
                    COUNT(ITE_SUPERMARKET_FLAG) as CPG,
                    COUNT(distinct CASE WHEN a11.TIM_DAY_WINNING_DATE >= DATE '""" + to_ + """' - 30 THEN cus_cust_id_sel ELSE NULL END) vendedores_distintos_30,
                    COUNT(distinct CASE WHEN (a11.TIM_DAY_WINNING_DATE >= DATE '""" + to_ + """' - 60) AND (a11.TIM_DAY_WINNING_DATE <= DATE '""" + to_ + """' - 30) THEN cus_cust_id_sel ELSE NULL END) vendedores_distintos_60_30,
                    COUNT(distinct CASE WHEN (a11.TIM_DAY_WINNING_DATE >= DATE '""" + to_ + """' - 90) AND (a11.TIM_DAY_WINNING_DATE <= DATE '""" + to_ + """' - 60) THEN cus_cust_id_sel ELSE NULL END) vendedores_distintos_90_60,
                    COUNT(distinct cus_cust_id_sel) vendedores_distintos_long,
                    SUM (CASE WHEN mob.mapp_mobile_flag = 'Desktop' THEN 1 ELSE 0 END) DESKTOP_PURCHASE,
                    SUM(CASE WHEN mob.mapp_mobile_flag = 'Mobile-WEB' THEN 1 else 0 END) MOBILE_WEB_PURCHASE,
                    SUM(CASE WHEN mob.mapp_mobile_flag= 'Mobile-APP' AND MAPP_APP_desc = 'android' THEN 1 else 0 END) ANDROID_PURCHASE,
                    SUM(CASE WHEN mob.mapp_mobile_flag = 'Mobile-APP' AND MAPP_APP_desc = 'iphone' THEN 1 else 0 END) IOS_PURCHASE,
                    COUNT(DISTINCT CASE WHEN COALESCE(SHP_FREE_FLAG_ID,0) >= 1 THEN ORD_ORDER_ID ELSE NULL END) AS CANTIDAD_COMPRAS_FREE_SHIPPING,
                    COUNT(DISTINCT CASE WHEN ship.SHP_SHIPMENT_ID IS NULL THEN ORD_ORDER_ID ELSE NULL END) AS CANTIDAD_COMPRAS_NO_ENVIOS,
                    COUNT(DISTINCT CASE WHEN COALESCE(ship.SHP_CART_TYPE,0) = 0 THEN ORD_ORDER_ID ELSE NULL END) AS CANTIDAD_COMPRAS_NO_CARRITO,
                    COUNT(DISTINCT CASE WHEN ship.SHP_PICKING_TYPE_ID = 'drop_off' THEN ORD_ORDER_ID ELSE NULL END) AS CANTIDAD_COMPRAS_ENVIO_DROP_OFF,
                    COUNT(DISTINCT CASE WHEN ITE_TIPO_PROD = 'N' THEN ORD_ORDER_ID ELSE NULL END) AS CANTIDAD_COMPRAS_ITEMS_NUEVOS,
                    COUNT(DISTINCT CASE WHEN ITE_TIPO_PROD = 'U' THEN ORD_ORDER_ID ELSE NULL END) AS CANTIDAD_COMPRAS_ITEMS_USADOS

            FROM BT_BIDS a11

            LEFT JOIN WHOWNER.BT_SHP_SHIPMENTS ship
                      ON a11.SHP_SHIPMENT_ID = ship.SHP_SHIPMENT_ID
                      AND a11.SIT_SITE_ID = ship.SIT_SITE_ID
                      AND ship.SHP_Type = 'forward'
                      AND ship.SHP_DATE_CREATED_ID BETWEEN DATE'""" + from_ + """' AND DATE'""" + to_ + """'
                      
            LEFT JOIN WHOWNER.LK_MAPP_MOBILE mob
                ON (COALESCE(a11.MAPP_APP_ID, '-1') = mob.MAPP_APP_ID)

            WHERE a11.BID_BID_STATUS = 'W'
                    AND a11.PHOTO_ID = 'TODATE'
                    AND a11.SIT_SITE_ID = '""" +  PAIS + """'
                    AND a11.ITE_GMV_FLAG = 1
                    AND a11.MKT_MARKETPLACE_ID = 'TM'
                    AND COALESCE(a11.BID_FVF_BONIF ,'Y')='N'
                    AND COALESCE(a11.AUTO_OFFER_FLAG, 0) <> 1 --Mandatory Filter
                    --AND NOT EXISTS (SELECT 1 FROM WHOWNER.LK_ORD_ORDER_TAG OT WHERE a11.ORD_ORDER_ID = OT.ORD_ORDER_ID AND FLAG_AUTO = 'YES')
                    AND a11.BID_BASE_CURRENT_PRICE < 10000
                    AND a11.TIM_DAY_WINNING_DATE BETWEEN DATE '""" + from_ + """' AND DATE '""" + to_ + """'
                    AND EXISTS(SELECT 1 FROM """+tabla_pivot2+""" u WHERE a11.CUS_CUST_ID_BUY = u.CUS_CUST_ID)
            GROUP BY 1
    """   
    teradata_app.fast_export(query(start_cal_train.strftime("%Y-%m-%d"),end_cal_train.strftime("%Y-%m-%d")) ,
                     "s3://"+fda_path+s3_path_in + "ComprasYshipping.csv")          

    
def demo(s3_path_in,teradata_app,hive,end_cal_train,start_cal_train,start_cal_test,end_cal_test,file_name = "Demograficos.csv"):
    # First compra
    def query():
        q = """
                SELECT  c.cus_cust_id as cust, STRTOK(CUS_STATE,'|',1) as CUS_STATE,
                     CUS_RU_SINCE_DT,
                         CUS_FIRST_BUY, CUS_FIRST_PUBLICATION

                FROM WHOWNER.LK_CUS_CUSTOMERS_DATA c
                WHERE c.sit_site_id_cus = '""" +  PAIS + """'
                      AND EXISTS (SELECT 1 FROM """+tabla_pivot2+""" u
                                      WHERE c.CUS_CUST_ID = u.CUS_CUST_ID)
    """
        return q

    teradata_app.fast_export(query(),"s3://"+fda_path+s3_path_in + file_name)

# Datos como seller
def sellers(s3_path_in,teradata_app,hive,end_cal_train,start_cal_train,start_cal_test,end_cal_test):
    def query(start,end):
        q = """
                    SELECT a11.CUS_CUST_ID_SEL as cust, SUM(a11.BID_BASE_CURRENT_PRICE * a11.BID_QUANTITY_OK) AS SALES_SELLER,
                            COUNT(1) as VENTAS_SELLER, MAX(a11.TIM_DAY_WINNING_DATE) as RECENCY_SELLER,
                            MIN(a11.TIM_DAY_WINNING_DATE) as FIRST_SELLER
                    FROM WHOWNER.BT_BIDS a11  
                    WHERE a11.BID_BID_STATUS = 'W' AND a11.PHOTO_ID = 'TODATE' AND a11.SIT_SITE_ID = '"""+PAIS+"""'
                        AND a11.BID_BASE_CURRENT_PRICE < 10000 
                        AND a11.TIM_DAY_WINNING_DATE BETWEEN DATE '"""+start+"""' AND DATE '"""+end+"""'
                        AND ITE_GMV_FLAG = 1 AND MKT_MARKETPLACE_ID = 'TM' AND COALESCE(a11.BID_FVF_BONIF ,'Y')='N' 
                        AND COALESCE(a11.AUTO_OFFER_FLAG, 0) <> 1 --Mandatory Filter  
                        AND EXISTS(SELECT 1 FROM  """+tabla_pivot2+""" u
                                          WHERE a11.CUS_CUST_ID_SEL = u.CUS_CUST_ID)
                    GROUP BY 1
        """
        return q

    teradata_app.fast_export(query(start_cal_train.strftime("%Y-%m-%d"),
                       end_cal_train.strftime("%Y-%m-%d")),"s3://"+fda_path+s3_path_in + "sellers.csv")
    
    
def payments(s3_path_in,teradata_app,hive,end_cal_train,start_cal_train,start_cal_test,end_cal_test):
    # Formas de pago
    def query(start,end):
        return """
                SELECT
                    ENG_BIDS.CUS_CUST_ID_BUY as CUS_CUST_ID,
                    AVG(CASE WHEN PAY_CCD_INSTALLMENTS_QTY > 1 THEN PAY_CCD_INSTALLMENTS_QTY ELSE NULL END) AVG_CUOTAS,
                    MAX(PAY_CCD_INSTALLMENTS_QTY) MAX_CUOTAS,
                    --SUM(CASE WHEN pay.PAY_COMBO_ID = 'J' AND PAY_CCD_INSTALLMENTS_QTY > 1 THEN 1 ELSE 0 END) AS PSJ,  -- cuotas sin interes
                    SUM(CASE WHEN pay.PAY_CCD_FINANCE_CHARGE_AMT = 0 AND PAY_CCD_INSTALLMENTS_QTY > 1 THEN 1 ELSE 0 END) AS CSI,
                    SUM(CASE WHEN pay.PAY_CCD_FINANCE_CHARGE_AMT > 0 AND PAY_CCD_INSTALLMENTS_QTY > 1 THEN 1 ELSE 0 END) AS CCI,-- SI FUE CON INTERES O NO

                    SUM(CASE WHEN LOWER(pay_pm_type_id)= 'ticket' THEN 1 ELSE 0 END) AS SUM_TICKET,       
                    SUM(CASE WHEN pay_pm_type_id= 'account_money' THEN 1 ELSE 0 END) AS SUM_DINERO_CUENTA, --DINERO EN CUENTA            
          
                    SUM(CASE WHEN pay_pm_type_id IN('debit_card') THEN 1 ELSE 0 END) AS SUM_TD,-- TD            
                    SUM(CASE WHEN pay_pm_type_id IN('credit_card') THEN 1 ELSE 0 END) AS SUM_TC -- TC           

                FROM BT_BIDS ENG_BIDS
                INNER JOIN WHOWNER.BT_MP_PAY_PAYMENTS pay
                    ON ENG_BIDS.CUS_CUST_ID_BUY = pay.CUS_CUST_ID_BUY
                        AND ENG_BIDS.OPE_OPER_ID = pay.PAY_PAYMENT_ID
                        AND pay.SIT_SITE_ID = '""" +  PAIS + """' 
                        AND pay.PAY_MOVE_DATE BETWEEN DATE '""" + start + """' AND DATE '""" + end + """'
                        AND pay.PAY_OPERATION_TYPE_ID = 'regular_payment'
                        AND pay.PAY_OPE_MLIBRE_FLAG = 'Y'
                LEFT JOIN WHOWNER.LK_MP_PAY_PAYMENT_METHODS paymeth
                    ON pay.PAY_PM_ID = paymeth.PAY_PM_ID
                        AND pay.SIT_SITE_ID = paymeth.SIT_SITE_ID
                WHERE ENG_BIDS.BID_BID_STATUS = 'W' AND ENG_BIDS.PHOTO_ID = 'TODATE' AND ENG_BIDS.SIT_SITE_ID = '""" +  PAIS + """' 
                        AND ENG_BIDS.BID_BASE_CURRENT_PRICE < 10000 
                        AND ENG_BIDS.TIM_DAY_WINNING_DATE BETWEEN DATE '""" + start + """' AND DATE '""" + end + """' 
                        AND ENG_BIDS.ITE_GMV_FLAG = 1 AND ENG_BIDS.MKT_MARKETPLACE_ID = 'TM' AND COALESCE(ENG_BIDS.BID_FVF_BONIF ,'Y')='N' 
                        AND COALESCE(ENG_BIDS.AUTO_OFFER_FLAG, 0) <> 1 --Mandatory Filter 
                        AND EXISTS(SELECT 1 FROM """+tabla_pivot2+""" a WHERE a.CUS_CUST_ID = ENG_BIDS.CUS_CUST_ID_BUY)

                GROUP BY 1"""

    teradata_app.fast_export(query((start_cal_train).strftime("%Y-%m-%d"),
                       end_cal_train.strftime("%Y-%m-%d")),"s3://"+fda_path+s3_path_in + "payments.csv")

def tarjetas(s3_path_in,teradata_app,hive,end_cal_train,start_cal_train,start_cal_test,end_cal_test):
    # Tarjetas:
    def query(start,end):
                             
        if(PAIS == "MLB"):
            p = "BRAZIL"
        if(PAIS == "MLA"):
            p = "ARGENTINA"
        if(PAIS == "MLM"):
            p = "MEXICO"
        if(PAIS == "MLC"):
            p = "CHILE"
        if(PAIS == "MLU"):
            p = "URUGUAY"
        if(PAIS == "MCO"):
            p = "COLOMBIA"
                             
        return """
        SELECT
        pay.CUS_CUST_ID_BUY as CUS_CUST_ID,
        SUM(CASE WHEN PAY_CREATED_FROM IN ('1311377052931992','1945000207238192','2034912493936010') THEN 1 ELSE 0 END) AS PAY_APP_MP,
        SUM(CASE WHEN PAY_CREATED_FROM IN ('1505','7092') THEN 1 ELSE 0 END) AS PAY_APP_ML,
        SUM(CASE WHEN pay_pm_type_id = 'debit_card' THEN 1 ELSE 0 END) as DEBIT_PAY,
        SUM(CASE WHEN pay_pm_type_id = 'credit_card' THEN 1 ELSE 0 END) as CREDIT_PAY,
        SUM(CASE WHEN pay_pm_type_id = 'account_money' THEN 1 ELSE 0 END) as ACCOUNT_PAY

        FROM WHOWNER.BT_MP_PAY_PAYMENTS pay
        LEFT JOIN lk_mp_pay_payment_methods payments 
            ON pay.PAY_PM_ID = payments.PAY_PM_ID
            AND payments.SIT_SITE_ID = '"""+ PAIS + """'

        WHERE pay.sit_site_id = '"""+ PAIS + """'
            AND pay.PAY_STATUS_ID IN ('approved', 'refunded') AND pay.TPV_FLAG = 1 
            AND pay.pay_move_date BETWEEN DATE '"""+start+"""' AND DATE '"""+end+"""'
            AND EXISTS (SELECT 1 FROM """+tabla_pivot2+""" u WHERE u.CUS_CUST_ID = pay.CUS_CUST_ID_BUY)
        GROUP BY 1
        """

    teradata_app.fast_export(query((start_cal_train).strftime("%Y-%m-%d"),
                       end_cal_train.strftime("%Y-%m-%d")),"s3://"+fda_path+s3_path_in + "payments_all.csv")
    
def tarjetas_bin(s3_path_in,teradata_app,hive,end_cal_train,start_cal_train,start_cal_test,end_cal_test,file_name = ""):
    # Tarjetas:
    def query(start,end):
                             
        if(PAIS == "MLB"):
            p = "BRAZIL"
        if(PAIS == "MLA"):
            p = "ARGENTINA"
        if(PAIS == "MLM"):
            p = "MEXICO"
        if(PAIS == "MLC"):
            p = "CHILE"
        if(PAIS == "MLU"):
            p = "URUGUAY"
        if(PAIS == "MCO"):
            p = "COLOMBIA"
                             
        return """
            SELECT
            pay.CUS_CUST_ID_BUY as CUS_CUST_ID,
            MAX(CASE WHEN bin.COUNTRY <> '"""+p+"""' THEN 1 ELSE 0 END) as TURISTA,
            MAX(CASE WHEN (bin.CATEGORY = 'CLASSIC') THEN 1
                     WHEN (bin.CATEGORY = 'GOLD') THEN 2 
                     WHEN (bin.CATEGORY = 'BUSINESS') THEN 2
                     WHEN (bin.CATEGORY = 'PLATINUM') THEN 3
                     WHEN (bin.CATEGORY = 'SIGNATURE') THEN 4       
                     WHEN (bin.CATEGORY = 'INFINITE') THEN 4
                     ELSE 0 END) as TIPO_TARJETA,
            MAX(CASE WHEN bin.CARD_TYPE = 'PREPAID' THEN 1 ELSE 0 END) as PREPAID

            FROM WHOWNER.BT_MP_PAY_PAYMENTS pay

            INNER JOIN mkt_corp.BINES_TARJETA bin
                ON bin.BIN = pay.PAY_CCD_FIRST_SIX_DIGITS
                AND bin.CARD_TYPE = 'CREDIT'

            WHERE pay.sit_site_id = '"""+ PAIS + """'
                AND pay.PAY_STATUS_ID = 'approved' AND pay.TPV_FLAG = 1 
                AND pay.pay_move_date BETWEEN DATE '"""+start+"""' AND DATE '"""+end+"""'
                AND EXISTS (SELECT 1 FROM """+tabla_pivot2+""" u WHERE u.CUS_CUST_ID = pay.CUS_CUST_ID_BUY)
            GROUP BY 1
        """

    teradata_app.fast_export(query((start_cal_train).strftime("%Y-%m-%d"),
                       end_cal_train.strftime("%Y-%m-%d")),"s3://"+fda_path+s3_path_in + "tarjetas_bin"+file_name+".csv")

def installs(s3_path_in,teradata_app,hive,end_cal_train,start_cal_train,start_cal_test,end_cal_test,file_name = ""):
    def q_install(end):
        return """
              SELECT CUS_CUST_ID_ADJ as CUS_CUST_ID,
                      MAX(INSTALL_DT) AS last_install, MIN(INSTALL_DT) AS first_install, 
                      MAX(INSTALL_OS_NAME) AS tipo_device
              FROM MKT_CORP.v_LK_CUST_DATA_ADJUST_ALL ad
              WHERE   INSTALL_DT <= DATE'"""+end+"""'
                 AND APP_WALLET_TYPE = 'ML'
                 AND INSTALL_COUNTRY = '"""+country[PAIS]+"""'
                 AND EXISTS(SELECT 1 FROM """+tabla_pivot2+""" u WHERE ad.CUS_CUST_ID_ADJ = u.CUS_CUST_ID)
              group by 1
        """
    teradata_app.fast_export(q_install( end_cal_train.strftime("%Y-%m-%d")),"s3://"+fda_path+s3_path_in + "install_ml"+file_name+".csv")
    
    teradata_app.fast_export("""
           SELECT t.CUS_CUST_ID,MIN(date_created) as install_dt, MIN(A.PLATFORM) as device
           FROM """+tabla_pivot2+""" t
           INNER JOIN APP_DEVICES A 
                 ON A.CUS_CUST_ID = t.CUS_CUST_ID
                 AND A.SIT_SITE_ID = '"""+PAIS+"""'
                 AND A.ACTIVE = 'YES' AND A.STATUS = 'ACTIVE'
                 AND A.MARKETPLACE IN ('MERCADOLIBRE') AND A.PLATFORM IN ('ios','android')
                 AND A.TOKEN IS NOT NULL
                 AND date_created <= DATE'"""+ end_cal_train.strftime("%Y-%m-%d")+"""'
           GROUP BY 1
        ""","s3://"+fda_path+s3_path_in + "install_ml_rezagados"+file_name+".csv")

def visits(s3_path_in,teradata_app,hive,presto,end_cal_train,start_cal_train,start_cal_test,end_cal_test):
    teradata_app.fast_export("""
            SELECT CAST(CUS_CUST_ID AS BIGINT) 
            FROM """+tabla_pivot2,
                         "s3://"+fda_path+s3_path_in + "to_hive.csv",sep = "|")

    # utils.my_download_file("s3://"+fda_path+s3_path_in + "to_hive.csv","to_hive.csv")

    hive.execute("DROP TABLE IF EXISTS "+hive_tbl)
    df = utils.read_csv_from_s3("s3://"+fda_path+s3_path_in + "to_hive.csv")
    utils.subir_pandas_dataframe_a_hive(df,hive_tbl,path_hive_table,{"CUS_CUST_ID": "BIGINT"},hive)

    hive.execute("DROP TABLE IF EXISTS "+tmp_visits_tbl)
    
    def visits_(t,t_30,t_90):
        hive.execute("""
            CREATE TABLE """+tmp_visits_tbl+"""
                        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                        LINES TERMINATED BY '\n'
                        STORED AS PARQUET
                        LOCATION 's3://melidata-results-batch/"""+path_tmp_visits_tbl+"""/'
            AS(
                    SELECT  c.cus_cust_id as CUS_CUST_ID,
                        MAX(log_date) as recency_date_90d,
                        MAX(CASE WHEN log_date >= DATE'"""+t_30+"""' THEN log_date ELSE NULL END) as recency_date,
                        MIN(log_date) as first_date_90d,
                        MIN(CASE WHEN log_date >= DATE'"""+t_30+"""' THEN log_date ELSE NULL END) as first_date,
                        SUM(CASE WHEN log_date >= DATE'"""+t_30+"""' THEN 1 ELSE 0 END) as cant_dias_active,
                        COUNT(1) as cant_dias_active_90d,
                        SUM(CASE WHEN log_date >= DATE'"""+t_30+"""' THEN bookmarks_count ELSE 0 END) as bookmarks
                FROM  MKT_INSIGHTS.VISITAS_POR_USUARIO_DIA c
                WHERE SIT_SITE_ID = '"""+PAIS+"""'
                    AND log_date >= DATE'"""+t_90+"""' 
                    AND log_date <= DATE'"""+t+"""'
                    AND EXISTS(SELECT 1 FROM  """+hive_tbl+""" b WHERE b.CUS_CUST_ID = c.cus_cust_id)
                GROUP BY c.cus_cust_id
                )
        """)
    
    visits_(end_cal_train.strftime("%Y-%m-%d"),(end_cal_train - relativedelta(days = 30)).strftime("%Y-%m-%d"),
            (end_cal_train - relativedelta(days = 90)).strftime("%Y-%m-%d"))

    print("Ready hive")`
    try:
        hive.execute("GRANT SELECT ON TABLE "+tmp_visits_tbl+" TO USER app_mkt")
    except:
        print("same user")
    
    for i in range(4):
        try:
            presto.execute_response("""
                    SELECT CUS_CUST_ID,recency_date_90d,recency_date,first_date_90d,
                            first_date,cant_dias_active,cant_dias_active_90d,bookmarks 
                    FROM """+tmp_visits_tbl).to_csv("upload_me.csv",sep = "|",index=False)
            break
        except:
            if i == 3:
                print("Error en presto. No hay datos de visitas")
            pass

    utils.my_upload_file("upload_me.csv","s3://"+fda_path+s3_path_in + "Visitas_short.csv")
    print("Uploaded: "+"s3://"+fda_path+s3_path_in + "Visitas_short.csv")

    hive.execute("DROP TABLE IF EXISTS "+tmp_visits_tbl)
    hive.execute("DROP TABLE IF EXISTS "+hive_tbl)
    
def target(s3_path_in,teradata_app,hive,end_cal_train,start_cal_train,start_cal_test,end_cal_test):
    teradata_app.fast_export("""
                SELECT a11.CUS_CUST_ID_BUY as CUS_CUST_ID, COUNT(distinct TIM_DAY_WINNING_DATE) as FREQ_TARGET,
                       SUM(a11.BID_BASE_CURRENT_PRICE * a11.BID_QUANTITY_OK) AS GMV_TARGET
                FROM WHOWNER.BT_BIDS a11  
                WHERE a11.BID_BID_STATUS = 'W' AND a11.PHOTO_ID = 'TODATE' AND a11.SIT_SITE_ID = '"""+PAIS+"""'
                      AND a11.BID_BASE_CURRENT_PRICE < 10000 
                      AND a11.TIM_DAY_WINNING_DATE BETWEEN DATE '"""+start_cal_test.strftime("%Y-%m-%d")+"""' AND DATE '"""+end_cal_test.strftime("%Y-%m-%d")+"""'
                      AND ITE_GMV_FLAG = 1 AND MKT_MARKETPLACE_ID = 'TM' AND COALESCE(a11.BID_FVF_BONIF ,'Y')='N' 
                      AND COALESCE(a11.AUTO_OFFER_FLAG, 0) <> 1 --Mandatory Filter   
                GROUP BY 1
    ""","s3://"+fda_path+s3_path_in + "target.csv")
