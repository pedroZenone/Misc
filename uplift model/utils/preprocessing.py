import pandas as pd
import os
import numpy as np
import datetime
import seaborn as sns
import matplotlib.pyplot as plt
import pickle
import json
from google_cloud import BigQuery, Storage
#from mktutils.google_cloud import BigQuery
from dateutil.relativedelta import relativedelta

def days_calc_preprocessing(df):
    
    df['PROCESS_DATE']=pd.to_datetime(df.PROCESS_DATE)
    
    if 'FIRST_ORDER' in df.columns:
        df['FIRST_ORDER']=pd.to_datetime(df.FIRST_ORDER)
        df['TODAY']=df['FIRST_ORDER']
        df.TODAY.fillna(df.PROCESS_DATE,inplace=True)
    else:
        df['TODAY']=pd.to_datetime(df.PROCESS_DATE)
        
    df['Dias_install_ml']=(df.MIN_INSTALL_ML-df.CUS_RU_SINCE).dt.days
    df['Dias_install_mp']=(df.MIN_INSTALL_MP-df.CUS_RU_SINCE).dt.days
    #df['Dias_log']=(df.TODAY-df.CUS_RU_SINCE).dt.days
    df['Dias_last_view']=(df.TODAY-df.LAST_VIEW).dt.days
    df['Dias_first_view']=(df.TODAY-df.FIRST_VIEW).dt.days
    df['Dias_last_view_log']=(df.LAST_VIEW-df.CUS_RU_SINCE).dt.days
    df['Dias_first_view_log']=(df.FIRST_VIEW-df.CUS_RU_SINCE).dt.days
    
    df['Dias_install_ml'].fillna(-10,inplace=True)
    df['Dias_install_mp'].fillna(-10,inplace=True)
    #df['Dias_log'].fillna(-10,inplace=True)
    df['Dias_last_view'].fillna(-10,inplace=True)
    df['Dias_first_view'].fillna(-10,inplace=True)
    df['Dias_last_view_log'].fillna(-10,inplace=True)
    df['Dias_first_view_log'].fillna(-10,inplace=True)


def transform_date_preprocessing(df):
    date_col=['CUS_RU_SINCE','MAX_INSTALL_ML','MIN_INSTALL_ML','MAX_INSTALL_MP','MIN_INSTALL_MP','LAST_VIEW','FIRST_VIEW','PROCESS_DATE']
    for col in date_col:
        df[col]=pd.to_datetime(df[col])
        
def transform_numeric_preprocessing(df):
    numeric_col=df.columns[df.columns.str.contains("_SUM|_COUNT|MIN_PRICE|MAX_PRICE|AVG_PRICE")].to_list()
    for col in numeric_col:
        df[col].fillna(0,inplace=True)
        df[col]=df[col].astype(float)
        
def preparo_datos(un_df,train_cols):
    
    cates = ['OS_NAME','countrysubdivision','APP_ML','APP_MP']
    
    train_cols_nocat = [x for x in train_cols if(x not in cates)]
    cates = [x for x in train_cols if(x in cates)]

    for x in cates:
        
        if(un_df[x].dtype == "float64"):
            un_df[x] = un_df[x].astype(int)

        un_df[x] = un_df[x].astype(str)

    return un_df,cates,train_cols_nocat

def fun_query_apply(fecha_hasta,PAIS):
    
    dict_pais=dict({'MCO':'co','MLM':'mx','MLB':'br'})
    
    return """
        WITH customer as (
            SELECT 
            
            c.CUS_CUST_ID,
            c.SIT_SITE_ID_CUS SIT_SITE_ID,
            DATE(CUS_RU_SINCE_DT) CUS_RU_SINCE,
            MAX(CASE WHEN b.cus_cust_id IS NULL THEN 'Sin App' ELSE 'App' END) TYPE_APP,
            MAX(DATE '"""+fecha_hasta+"""') as PROCESS_DATE
            FROM meli-bi-data.WHOWNER.LK_CUS_CUSTOMERS_DATA c
                
                LEFT JOIN meli-bi-data.WHOWNER.APP_DEVICES b
                ON b.CUS_CUST_ID=c.CUS_CUST_ID
                AND b.SIT_SITE_ID IN ('"""+PAIS+"""')
                AND (UPPER(PLATFORM)) IN ('ANDROID', 'IOS')
                AND STATUS = 'ACTIVE'
                AND c.SIT_SITE_ID_CUS=b.SIT_SITE_ID
                
            WHERE c.cus_cust_status in ('active')
            AND DATE(CUS_RU_SINCE_DT) BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -60 DAY) AND DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -1 DAY)
            AND c.CUS_NICKNAME not like '%TEST%'
            AND c.SIT_SITE_ID_CUS IN ('"""+PAIS+"""')
            AND NOT EXISTS (SELECT 1 FROM meli-bi-data.WHOWNER.BT_ORD_ORDERS 
                            WHERE ord_status = 'paid'
                                 AND ORD_BUYER.ID=c.CUS_CUST_ID 
                                 AND ORD_CREATED_DT BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -60 DAY) AND DATE '"""+fecha_hasta+"""'
                           )
            GROUP BY 1,2,3
        ),
        
        payments as (
            SELECT 
            p.cus_cust_id_buy,
            p.sit_site_id,
            SUM(SAFE_CAST(pay_transaction_dol_amt as NUMERIC)) TOTAL_PAYMENTS_AMT_SUM,
            SUM(CASE WHEN tpv_segment_id IN ('Wallet') THEN CAST(pay_transaction_dol_amt AS NUMERIC) END) WALLET_PAYMENTS_AMT_SUM,
            SUM(CASE WHEN tpv_segment_id IN ('Aggregator') THEN CAST(pay_transaction_dol_amt AS NUMERIC) END) AGGREGATOR_PAYMENTS_AMT_SUM,
            COUNT(distinct pay_payment_id) TOTAL_PAYMENTS_COUNT

            FROM meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS p
            
            JOIN customer c
                ON c.SIT_SITE_ID=TRIM(p.sit_site_id)
                AND c.cus_cust_id=p.cus_cust_id_buy
            
            WHERE  pay_move_date BETWEEN CUS_RU_SINCE AND DATE_ADD(CUS_RU_SINCE, INTERVAL 60 DAY)
                AND PAY_APPROVED_DT is not null
                AND tpv_segment_id IN ('Account Fund','Wallet','Point','Aggregator','Point Device Sale')

            GROUP BY 1,2
        ),

        event as (
            SELECT 
                e.CUS_CUST_ID,
                e.sit_site_id,
                SUM(CAT_CATEG_ID_COUNT) CAT_CATEG_ID_COUNT,
                SUM(VIP_VIEW_APP_ANDROID_ITEM_COUNT) AS VIP_VIEW_APP_ANDROID_ITEM_COUNT,
                SUM(VIP_VIEW_APP_IOS_ITEM_COUNT) AS VIP_VIEW_APP_IOS_ITEM_COUNT,
                SUM(VIP_VIEW_DESKTOP_ITEM_COUNT) AS VIP_VIEW_DESKTOP_ITEM_COUNT,
                SUM(VIP_VIEW_WEB_MOBILE_ITEM_COUNT) AS VIP_VIEW_WEB_MOBILE_ITEM_COUNT,
                SUM(BUY_INTENTION_ITEM_COUNT) AS BUY_INTENTION_ITEM_COUNT,
                SUM(QUESTION_ITEM_COUNT) AS QUESTION_ITEM_COUNT,
                SUM(CHECKOUT_CONGRATS_ITEM_COUNT) AS CHECKOUT_CONGRATS_ITEM_COUNT,
                SUM(ADD_TO_CART_ITEM_COUNT) AS ADD_TO_CART_ITEM_COUNT,
                SUM(BOOKMARK_ITEM_COUNT) AS BOOKMARK_ITEM_COUNT,
                MIN(MIN_PRICE_ITEM) AS MIN_PRICE_ITEM ,
                MAX(MAX_PRICE_ITEM) AS MAX_PRICE_ITEM,
                AVG(AVG_PRICE_ITEM) AS AVG_PRICE_ITEM,
                MAX(CASE WHEN MAX_PRICE_ITEM<=10000 THEN MAX_PRICE_ITEM ELSE 0 END) AS MAX_PRICE_ITEM_PODADO,
                AVG(CASE WHEN AVG_PRICE_ITEM<=10000 THEN AVG_PRICE_ITEM ELSE 0 END) AS AVG_PRICE_ITEM_PODADO,
                COUNT(DISTINCT LOG_DATE) AS LOG_DATE_COUNT,
                MAX(LOG_DATE) LAST_VIEW,
                MIN(LOG_DATE) FIRST_VIEW,
                SUM(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN CAT_CATEG_ID_COUNT END) CAT_CATEG_ID_COUNT_7D,
                SUM(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN VIP_VIEW_APP_ANDROID_ITEM_COUNT END) AS VIP_VIEW_APP_ANDROID_ITEM_COUNT_7D,
                SUM(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN VIP_VIEW_APP_IOS_ITEM_COUNT END) AS VIP_VIEW_APP_IOS_ITEM_COUNT_7D,
                SUM(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN VIP_VIEW_DESKTOP_ITEM_COUNT END) AS VIP_VIEW_DESKTOP_ITEM_COUNT_7D,
                SUM(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN VIP_VIEW_WEB_MOBILE_ITEM_COUNT END) AS VIP_VIEW_WEB_MOBILE_ITEM_COUNT_7D,
                SUM(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN BUY_INTENTION_ITEM_COUNT END) AS BUY_INTENTION_ITEM_COUNT_7D,
                SUM(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN QUESTION_ITEM_COUNT END) AS QUESTION_ITEM_COUNT_7D,
                SUM(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN CHECKOUT_CONGRATS_ITEM_COUNT END) AS CHECKOUT_CONGRATS_ITEM_COUNT_7D,
                SUM(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN ADD_TO_CART_ITEM_COUNT END) AS ADD_TO_CART_ITEM_COUNT_7D,
                SUM(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN BOOKMARK_ITEM_COUNT END) AS BOOKMARK_ITEM_COUNT_7D,
                MIN(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN MIN_PRICE_ITEM END) AS MIN_PRICE_ITEM_7D ,
                MAX(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN MAX_PRICE_ITEM END) AS MAX_PRICE_ITEM_7D,
                AVG(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN AVG_PRICE_ITEM END) AS AVG_PRICE_ITEM_7D,
                COUNT(DISTINCT CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN LOG_DATE END) AS LOG_DATE_COUNT_7D,
                
                SUM(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN CAT_CATEG_ID_COUNT END) CAT_CATEG_ID_COUNT_F7D,
                SUM(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN VIP_VIEW_APP_ANDROID_ITEM_COUNT END) AS VIP_VIEW_APP_ANDROID_ITEM_COUNT_F7D,
                SUM(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN VIP_VIEW_APP_IOS_ITEM_COUNT END) AS VIP_VIEW_APP_IOS_ITEM_COUNT_F7D,
                SUM(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN VIP_VIEW_DESKTOP_ITEM_COUNT END) AS VIP_VIEW_DESKTOP_ITEM_COUNT_F7D,
                SUM(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN VIP_VIEW_WEB_MOBILE_ITEM_COUNT END) AS VIP_VIEW_WEB_MOBILE_ITEM_COUNT_F7D,
                SUM(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN BUY_INTENTION_ITEM_COUNT END) AS BUY_INTENTION_ITEM_COUNT_F7D,
                SUM(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN QUESTION_ITEM_COUNT END) AS QUESTION_ITEM_COUNT_F7D,
                SUM(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN CHECKOUT_CONGRATS_ITEM_COUNT END) AS CHECKOUT_CONGRATS_ITEM_COUNT_F7D,
                SUM(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN ADD_TO_CART_ITEM_COUNT END) AS ADD_TO_CART_ITEM_COUNT_F7D,
                SUM(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN BOOKMARK_ITEM_COUNT END) AS BOOKMARK_ITEM_COUNT_F7D,
                MIN(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN MIN_PRICE_ITEM END) AS MIN_PRICE_ITEM_F7D ,
                MAX(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN MAX_PRICE_ITEM END) AS MAX_PRICE_ITEM_F7D,
                AVG(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN AVG_PRICE_ITEM END) AS AVG_PRICE_ITEM_F7D,
                COUNT(DISTINCT CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN LOG_DATE END) AS LOG_DATE_COUNT_F7D
                                
            FROM meli-marketing.MODELLING.LK_EVENT_CUSTOMER_ITEMS_AGG e
              
              JOIN customer c
                ON c.CUS_CUST_ID = e.cus_cust_id
                AND c.SIT_SITE_ID = e.sit_site_id  
              

            WHERE log_date BETWEEN CUS_RU_SINCE AND DATE_ADD(CUS_RU_SINCE, INTERVAL 60 DAY)
            AND e.sit_site_id in ('"""+PAIS+"""')
            AND e.log_date<DATE '"""+fecha_hasta+"""'
            GROUP BY 1,2
        ),

        install as (
            select 
                i.CUS_CUST_ID,
                MIN(i.OS_NAME) OS_NAME,
                MAX(CASE WHEN BU = 'ML' THEN 1 ELSE 0 END) APP_ML,
                MAX(CASE WHEN BU = 'MP' THEN 1 ELSE 0 END) APP_MP,
                MAX(CASE WHEN BU = 'ML' THEN INSTALL_DT END) MAX_INSTALL_ML,
                MIN(CASE WHEN BU = 'ML' THEN INSTALL_DT END) MIN_INSTALL_ML,
                MAX(CASE WHEN BU = 'MP' THEN INSTALL_DT END) MAX_INSTALL_MP,
                MIN(CASE WHEN BU = 'MP' THEN INSTALL_DT END) MIN_INSTALL_MP

            from `meli-marketing.APPINSTALL.BT_INSTALL_EVENT_CUST` i

            JOIN customer c
                ON c.CUS_CUST_ID = i.cus_cust_id

            where TRIM(c.SIT_SITE_ID) = '"""+PAIS+"""'
                and i.INSTALL_DT  BETWEEN CUS_RU_SINCE AND DATE_ADD(CUS_RU_SINCE, INTERVAL 60 DAY)
                and i.BU in ('MP','ML')
                and i.CUS_CUST_ID is not null

            GROUP BY 1
        )

        SELECT 

            c.*,
            p.* except (cus_cust_id_buy,sit_site_id),
            ec.* except (cus_cust_id,sit_site_id),
            i.* except (cus_cust_id)
        FROM customer c

        LEFT JOIN payments p
            on p.cus_cust_id_buy=c.cus_cust_id
            and p.sit_site_id=c.SIT_SITE_ID

        LEFT JOIN event ec
            ON c.CUS_CUST_ID=ec.CUS_CUST_ID
            AND ec.sit_site_id=c.SIT_SITE_ID

        LEFT JOIN install i 
            on i.CUS_CUST_ID = c.CUS_CUST_ID
            
"""

def fun_query_train(fecha_hasta,PAIS):
    
    dict_pais=dict({'MCO':'co','MLM':'mx','MLB':'br'})
    
    return """WITH customer as (
            SELECT 
            
            c.CUS_CUST_ID,
            c.SIT_SITE_ID_CUS SIT_SITE_ID,
            DATE(CUS_RU_SINCE_DT) CUS_RU_SINCE,
            MIN(CASE WHEN b.cus_cust_id IS NULL THEN 'Sin App' ELSE 'App' END) TYPE_APP,
            MAX(DATE '"""+fecha_hasta+"""') as PROCESS_DATE
            FROM meli-bi-data.WHOWNER.LK_CUS_CUSTOMERS_DATA c
                
            LEFT JOIN meli-bi-data.WHOWNER.APP_DEVICES b
                ON b.CUS_CUST_ID=c.CUS_CUST_ID
                AND b.SIT_SITE_ID IN ('"""+PAIS+"""')
                AND (UPPER(PLATFORM)) IN ('ANDROID', 'IOS')
                AND STATUS = 'ACTIVE'
                
            WHERE c.cus_cust_status in ('active')
            AND DATE(CUS_RU_SINCE_DT) BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -60 DAY) AND DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -1 DAY)
            AND c.CUS_NICKNAME not like '%TEST%'
            AND c.SIT_SITE_ID_CUS IN ('"""+PAIS+"""')
            --AND NOT EXISTS (SELECT 1 FROM meli-bi-data.WHOWNER.BT_ORD_ORDERS WHERE ord_status = 'paid'
            --AND ORD_BUYER.ID=c.CUS_CUST_ID AND ORD_CREATED_DT BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -60 DAY) AND DATE '"""+fecha_hasta+"""')
            GROUP BY 1,2,3
            
        ),
        orders as (
            Select ORD_BUYER.ID as CUS_CUST_ID,
            o.sit_site_id as SIT_SITE_ID, 
            MIN(ORD_CREATED_DT) FIRST_ORDER 
            from meli-bi-data.WHOWNER.BT_ORD_ORDERS o

                JOIN customer c
                on c.SIT_SITE_ID =o.sit_site_id
                AND c.cus_cust_id=o.ORD_BUYER.ID
             
            where ord_status = 'paid'
            and ORD_CREATED_DT BETWEEN c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 60 DAY)
            AND ORD_CLOSED_DT BETWEEN o.ORD_CREATED_DT AND DATE_ADD(o.ORD_CREATED_DT, INTERVAL 2 DAY)
            group by 1,2
            )
        ,
        payments as (
            SELECT 
            p.cus_cust_id_buy,
            p.sit_site_id,
            SUM(pay_transaction_dol_amt) TOTAL_PAYMENTS_AMT_SUM,
            SUM(CASE WHEN tpv_segment_id IN ('Account Fund') THEN pay_transaction_dol_amt END) ACCOUNT_FUND_PAYMENTS_AMT_SUM,
            SUM(CASE WHEN tpv_segment_id IN ('Wallet') THEN pay_transaction_dol_amt END) WALLET_PAYMENTS_AMT_SUM,
            SUM(CASE WHEN tpv_segment_id IN ('Aggregator') THEN pay_transaction_dol_amt END) AGGREGATOR_PAYMENTS_AMT_SUM,
    
            SUM(CASE WHEN pay_move_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN pay_transaction_dol_amt END) TOTAL_PAYMENTS_AMT_SUM_7D,
            SUM(CASE WHEN tpv_segment_id IN ('Account Fund') AND pay_move_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN pay_transaction_dol_amt END) ACCOUNT_FUND_PAYMENTS_AMT_SUM_7D,
            SUM(CASE WHEN tpv_segment_id IN ('Wallet') AND pay_move_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN pay_transaction_dol_amt END) WALLET_PAYMENTS_AMT_SUM_7D,
            SUM(CASE WHEN tpv_segment_id IN ('Point') AND pay_move_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN pay_transaction_dol_amt END) POINT_PAYMENTS_AMT_SUM_7D,
            SUM(CASE WHEN tpv_segment_id IN ('Aggregator') AND pay_move_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN pay_transaction_dol_amt END) AGGREGATOR_PAYMENTS_AMT_SUM_7D,
            
            SUM(CASE WHEN pay_move_date BETWEEN c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN pay_transaction_dol_amt END) TOTAL_PAYMENTS_AMT_SUM_F7D,
            SUM(CASE WHEN tpv_segment_id IN ('Account Fund') AND pay_move_date BETWEEN c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN pay_transaction_dol_amt END) ACCOUNT_FUND_PAYMENTS_AMT_SUM_F7D,
            SUM(CASE WHEN tpv_segment_id IN ('Wallet') AND pay_move_date BETWEEN c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN pay_transaction_dol_amt END) WALLET_PAYMENTS_AMT_SUM_F7D,
            SUM(CASE WHEN tpv_segment_id IN ('Point') AND pay_move_date BETWEEN c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN pay_transaction_dol_amt END) POINT_PAYMENTS_AMT_SUM_F7D,
            SUM(CASE WHEN tpv_segment_id IN ('Aggregator') AND pay_move_date BETWEEN c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN pay_transaction_dol_amt END) AGGREGATOR_PAYMENTS_AMT_SUM_F7D,

            
            COUNT(distinct pay_payment_id) TOTAL_PAYMENTS_COUNT,
            COUNT(DISTINCT CASE WHEN tpv_segment_id IN ('Account Fund') THEN pay_payment_id END) ACCOUNT_FUND_PAYMENTS_COUNT,
            COUNT(DISTINCT CASE WHEN tpv_segment_id IN ('Wallet') THEN pay_payment_id END) WALLET_PAYMENTS_COUNT,
            COUNT(DISTINCT CASE WHEN tpv_segment_id IN ('Aggregator') THEN pay_payment_id END) AGGREGATOR_PAYMENTS_COUNT,
            
            
            COUNT(distinct CASE WHEN pay_move_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN pay_payment_id END) TOTAL_PAYMENTS_COUNT_7D,
            COUNT(DISTINCT CASE WHEN tpv_segment_id IN ('Point Device Sale') AND pay_move_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN pay_payment_id END) POINT_DEVICE_COUNT_7D,
            COUNT(DISTINCT CASE WHEN tpv_segment_id IN ('Account Fund') AND pay_move_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN pay_payment_id END) ACCOUNT_FUND_PAYMENTS_COUNT_7D,
            COUNT(DISTINCT CASE WHEN tpv_segment_id IN ('Wallet') AND pay_move_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN pay_payment_id END) WALLET_PAYMENTS_COUNT_7D,
            COUNT(DISTINCT CASE WHEN tpv_segment_id IN ('Point') AND pay_move_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN pay_payment_id END) POINT_PAYMENTS_COUNT_7D,
            COUNT(DISTINCT CASE WHEN tpv_segment_id IN ('Aggregator') AND pay_move_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN pay_payment_id END) AGGREGATOR_PAYMENTS_COUNT_7D,

            COUNT(distinct CASE WHEN pay_move_date BETWEEN c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN pay_payment_id END) TOTAL_PAYMENTS_COUNT_F7D,
            COUNT(DISTINCT CASE WHEN tpv_segment_id IN ('Point Device Sale') AND pay_move_date BETWEEN c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN pay_payment_id END) POINT_DEVICE_COUNT_F7D,
            COUNT(DISTINCT CASE WHEN tpv_segment_id IN ('Account Fund') AND pay_move_date BETWEEN c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN pay_payment_id END) ACCOUNT_FUND_PAYMENTS_COUNT_F7D,
            COUNT(DISTINCT CASE WHEN tpv_segment_id IN ('Wallet') AND pay_move_date BETWEEN c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN pay_payment_id END) WALLET_PAYMENTS_COUNT_F7D,
            COUNT(DISTINCT CASE WHEN tpv_segment_id IN ('Point') AND pay_move_date BETWEEN c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN pay_payment_id END) POINT_PAYMENTS_COUNT_F7D,
            COUNT(DISTINCT CASE WHEN tpv_segment_id IN ('Aggregator') AND pay_move_date BETWEEN c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN pay_payment_id END) AGGREGATOR_PAYMENTS_COUNT_F7D
            

            FROM meli-bi-data.WHOWNER.BT_MP_PAY_PAYMENTS p
            
            JOIN customer c
                ON c.SIT_SITE_ID=TRIM(p.sit_site_id)
                AND c.cus_cust_id=p.cus_cust_id_buy
            
            LEFT JOIN orders o
                ON o.CUS_CUST_ID = p.cus_cust_id_buy
                AND o.SIT_SITE_ID = p.sit_site_id
            
            WHERE  pay_move_date BETWEEN CUS_RU_SINCE AND DATE_ADD(CUS_RU_SINCE, INTERVAL 60 DAY)
            AND PAY_APPROVED_DT is not null
            AND tpv_segment_id IN ('Account Fund','Wallet','Point','Aggregator','Point Device Sale')
            AND pay_move_date<COALESCE(FIRST_ORDER,DATE '"""+fecha_hasta+"""')

            GROUP BY 1,2
        ),

        event as (
            SELECT 
                e.CUS_CUST_ID,
                e.sit_site_id,
                SUM(CAT_CATEG_ID_COUNT) CAT_CATEG_ID_COUNT,
                SUM(VIP_VIEW_APP_ANDROID_ITEM_COUNT) AS VIP_VIEW_APP_ANDROID_ITEM_COUNT,
                SUM(VIP_VIEW_APP_IOS_ITEM_COUNT) AS VIP_VIEW_APP_IOS_ITEM_COUNT,
                SUM(VIP_VIEW_DESKTOP_ITEM_COUNT) AS VIP_VIEW_DESKTOP_ITEM_COUNT,
                SUM(VIP_VIEW_WEB_MOBILE_ITEM_COUNT) AS VIP_VIEW_WEB_MOBILE_ITEM_COUNT,
                SUM(BUY_INTENTION_ITEM_COUNT) AS BUY_INTENTION_ITEM_COUNT,
                SUM(QUESTION_ITEM_COUNT) AS QUESTION_ITEM_COUNT,
                SUM(CHECKOUT_CONGRATS_ITEM_COUNT) AS CHECKOUT_CONGRATS_ITEM_COUNT,
                SUM(ADD_TO_CART_ITEM_COUNT) AS ADD_TO_CART_ITEM_COUNT,
                SUM(BOOKMARK_ITEM_COUNT) AS BOOKMARK_ITEM_COUNT,
                MIN(MIN_PRICE_ITEM) AS MIN_PRICE_ITEM ,
                MAX(MAX_PRICE_ITEM) AS MAX_PRICE_ITEM,
                AVG(AVG_PRICE_ITEM) AS AVG_PRICE_ITEM,
                MAX(CASE WHEN MAX_PRICE_ITEM<=10000 THEN MAX_PRICE_ITEM ELSE 0 END) AS MAX_PRICE_ITEM_PODADO,
                AVG(CASE WHEN AVG_PRICE_ITEM<=10000 THEN AVG_PRICE_ITEM ELSE 0 END) AS AVG_PRICE_ITEM_PODADO,
                COUNT(DISTINCT LOG_DATE) AS LOG_DATE_COUNT,
                MAX(LOG_DATE) LAST_VIEW,
                MIN(LOG_DATE) FIRST_VIEW,
                SUM(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN CAT_CATEG_ID_COUNT END) CAT_CATEG_ID_COUNT_7D,
                SUM(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN VIP_VIEW_APP_ANDROID_ITEM_COUNT END) AS VIP_VIEW_APP_ANDROID_ITEM_COUNT_7D,
                SUM(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN VIP_VIEW_APP_IOS_ITEM_COUNT END) AS VIP_VIEW_APP_IOS_ITEM_COUNT_7D,
                SUM(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN VIP_VIEW_DESKTOP_ITEM_COUNT END) AS VIP_VIEW_DESKTOP_ITEM_COUNT_7D,
                SUM(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN VIP_VIEW_WEB_MOBILE_ITEM_COUNT END) AS VIP_VIEW_WEB_MOBILE_ITEM_COUNT_7D,
                SUM(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN BUY_INTENTION_ITEM_COUNT END) AS BUY_INTENTION_ITEM_COUNT_7D,
                SUM(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN QUESTION_ITEM_COUNT END) AS QUESTION_ITEM_COUNT_7D,
                SUM(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN CHECKOUT_CONGRATS_ITEM_COUNT END) AS CHECKOUT_CONGRATS_ITEM_COUNT_7D,
                SUM(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN ADD_TO_CART_ITEM_COUNT END) AS ADD_TO_CART_ITEM_COUNT_7D,
                SUM(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN BOOKMARK_ITEM_COUNT END) AS BOOKMARK_ITEM_COUNT_7D,
                MIN(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN MIN_PRICE_ITEM END) AS MIN_PRICE_ITEM_7D ,
                MAX(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN MAX_PRICE_ITEM END) AS MAX_PRICE_ITEM_7D,
                AVG(CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN AVG_PRICE_ITEM END) AS AVG_PRICE_ITEM_7D,
                COUNT(DISTINCT CASE WHEN log_date BETWEEN DATE_ADD(DATE '"""+fecha_hasta+"""', INTERVAL -7 DAY) AND DATE '"""+fecha_hasta+"""' THEN LOG_DATE END) AS LOG_DATE_COUNT_7D,
                
                SUM(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN CAT_CATEG_ID_COUNT END) CAT_CATEG_ID_COUNT_F7D,
                SUM(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN VIP_VIEW_APP_ANDROID_ITEM_COUNT END) AS VIP_VIEW_APP_ANDROID_ITEM_COUNT_F7D,
                SUM(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN VIP_VIEW_APP_IOS_ITEM_COUNT END) AS VIP_VIEW_APP_IOS_ITEM_COUNT_F7D,
                SUM(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN VIP_VIEW_DESKTOP_ITEM_COUNT END) AS VIP_VIEW_DESKTOP_ITEM_COUNT_F7D,
                SUM(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN VIP_VIEW_WEB_MOBILE_ITEM_COUNT END) AS VIP_VIEW_WEB_MOBILE_ITEM_COUNT_F7D,
                SUM(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN BUY_INTENTION_ITEM_COUNT END) AS BUY_INTENTION_ITEM_COUNT_F7D,
                SUM(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN QUESTION_ITEM_COUNT END) AS QUESTION_ITEM_COUNT_F7D,
                SUM(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN CHECKOUT_CONGRATS_ITEM_COUNT END) AS CHECKOUT_CONGRATS_ITEM_COUNT_F7D,
                SUM(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN ADD_TO_CART_ITEM_COUNT END) AS ADD_TO_CART_ITEM_COUNT_F7D,
                SUM(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN BOOKMARK_ITEM_COUNT END) AS BOOKMARK_ITEM_COUNT_F7D,
                MIN(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN MIN_PRICE_ITEM END) AS MIN_PRICE_ITEM_F7D ,
                MAX(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN MAX_PRICE_ITEM END) AS MAX_PRICE_ITEM_F7D,
                AVG(CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN AVG_PRICE_ITEM END) AS AVG_PRICE_ITEM_F7D,
                COUNT(DISTINCT CASE WHEN log_date BETWEEN  c.CUS_RU_SINCE AND DATE_ADD(c.CUS_RU_SINCE, INTERVAL 7 DAY) THEN LOG_DATE END) AS LOG_DATE_COUNT_F7D
                
                
                
            FROM meli-marketing.MODELLING.LK_EVENT_CUSTOMER_ITEMS_AGG e
              
              JOIN customer c
                ON c.CUS_CUST_ID = e.cus_cust_id
                AND c.SIT_SITE_ID = e.sit_site_id  
              
              LEFT JOIN orders o
                ON o.CUS_CUST_ID = e.cus_cust_id
                AND o.SIT_SITE_ID = e.sit_site_id

            WHERE log_date BETWEEN CUS_RU_SINCE AND DATE_ADD(CUS_RU_SINCE, INTERVAL 60 DAY)
            AND e.sit_site_id in ('"""+PAIS+"""')
            AND e.log_date<COALESCE(o.FIRST_ORDER,DATE '"""+fecha_hasta+"""')
            GROUP BY 1,2
        ),

        install as (

            select 
                i.CUS_CUST_ID,
                MIN(i.OS_NAME) OS_NAME,
                MAX(CASE WHEN BU = 'ML' THEN 1 ELSE 0 END) APP_ML,
                MAX(CASE WHEN BU = 'MP' THEN 1 ELSE 0 END) APP_MP,
                MAX(CASE WHEN BU = 'ML' THEN INSTALL_DT END) MAX_INSTALL_ML,
                MIN(CASE WHEN BU = 'ML' THEN INSTALL_DT END) MIN_INSTALL_ML,
                MAX(CASE WHEN BU = 'MP' THEN INSTALL_DT END) MAX_INSTALL_MP,
                MIN(CASE WHEN BU = 'MP' THEN INSTALL_DT END) MIN_INSTALL_MP

            from `meli-marketing.APPINSTALL.BT_INSTALL_EVENT_CUST` i

            JOIN customer c
                ON c.CUS_CUST_ID = i.cus_cust_id

            LEFT JOIN orders o
                ON o.CUS_CUST_ID = i.cus_cust_id

            where TRIM(c.SIT_SITE_ID) = '"""+PAIS+"""'
                and i.INSTALL_DT  BETWEEN CUS_RU_SINCE AND DATE_ADD(CUS_RU_SINCE, INTERVAL 60 DAY)
                AND i.INSTALL_DT < COALESCE(o.FIRST_ORDER,DATE '"""+fecha_hasta+"""')
                and i.BU in ('MP','ML')
                and i.CUS_CUST_ID is not null

            GROUP BY 1
        )

        SELECT 

            c.*,
            p.* except (cus_cust_id_buy,sit_site_id),
            ec.* except (cus_cust_id,sit_site_id),
            i.* except (cus_cust_id),
            o.* except (cus_cust_id,sit_site_id)
        FROM customer c

        LEFT JOIN payments p
            on p.cus_cust_id_buy=c.cus_cust_id
            and p.sit_site_id=c.SIT_SITE_ID

        LEFT JOIN event ec
            ON c.CUS_CUST_ID=ec.CUS_CUST_ID
            AND ec.sit_site_id=c.SIT_SITE_ID

        LEFT JOIN install i 
            on i.CUS_CUST_ID = c.CUS_CUST_ID
            
        left join orders o
            on o.cus_cust_id=c.cus_cust_id
            AND o.sit_site_id=c.SIT_SITE_ID
"""


def model_drift(site,bu,fecha,p_60,total):
    
    from mktutils.google_cloud import BigQuery
    import base64
    
    AUTH_BIGQUERY = base64.b64decode(os.environ['SECRET_AUTH_BIGQUERY'])
    bq = BigQuery(AUTH_BIGQUERY)
    
    Q = """
    delete from meli-marketing.MODELLING.NB_DRIFT
    where fecha = date'"""+fecha+"""' and site = '"""+site+"""'
    and bu = '"""+bu+"""'
    """
    bqQuery = bq.execute_response(Q)
    try:
        while(not bqQuery.done()): time.sleep(1)
    except:
        pass
    
    Q = """
        INSERT INTO meli-marketing.MODELLING.NB_DRIFT (SITE, BU, FECHA, p_60,total)
        VALUES('"""+site+"""', '"""+bu+"""', DATE'"""+fecha+"""', """+str(p_60)+""", """+str(total)+""")
        """
    bqQuery = bq.execute_response(Q) 