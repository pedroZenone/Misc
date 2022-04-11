def temp_table_q_tera(lu_day,site,batch_id,bq,tera):   ### OJO!!! cambiar el batch_id!!
    users = tera.execute_response("""
        SELECT
            case when notification_batch_id like '%/_CG%' ESCAPE '/' THEN 'control' 
                 when notification_event_type_id = 'shown' then 'shown'
                 when notification_event_type_id = 'open' then 'open'
                 ELSE 'sent' end as EVENT_TYPE,
            cus_cust_id
        from WHOWNER.BT_NOTIFICATION_CAMPAIGN a
        where a.notification_event_type_id in ('sent','open','shown')
            and a.notification_path = 'NOTIFICATION_CAMPAIGN'
            and a.notificaton_business = 'mercadolibre'
            and a.notification_batch_id LIKE '"""+batch_id+"""%'
            and a.notification_batch_id not like all ('%/_BLACKL%','%/_HOLDRE%','%/_JNYBLACKLIST%') ESCAPE '/'
            and a.notification_date = DATE'"""+lu_day+"""'
            and a.sit_site_id = '"""+site+"""'
        group by 1,2
    """
    )
    users.columns = ["EVENT_TYPE","cus_cust_id"]
    users["sit_site_id"] = site
    users["notification_date"] = lu_day
    users = users[["cus_cust_id","sit_site_id","notification_date","EVENT_TYPE"]]

    users.cus_cust_id = users.cus_cust_id.astype(str)
    users.notification_date = users.notification_date.astype(str)

    bq.execute("DROP TABLE meli-marketing.TEMP45.PZ_ADD_SENTS_CHURN")
    bq.subir_df(users, "meli-marketing.TEMP45.PZ_ADD_SENTS_CHURN", {"cus_cust_id":"STRING","sit_site_id":"STRING","notification_date":"STRING","EVENT_TYPE":"STRING"})
    bq.execute("""
        DELETE FROM  meli-marketing.TEMP45.PZ_SENTS_CHURN WHERE sit_site_id = '"""+site+"""';

        INSERT INTO meli-marketing.TEMP45.PZ_SENTS_CHURN ( cus_cust_id,sit_site_id,notification_date,EVENT_TYPE)
        (
            select CAST(cus_cust_id as INT64) as cus_cust_id,sit_site_id,CAST(notification_date as DATE) as notification_date,EVENT_TYPE
            from meli-marketing.TEMP45.PZ_ADD_SENTS_CHURN b
        );
    """)
    
    return users
        
def tmp_table_q(lu_day,site):
    return """
    CREATE TABLE IF NOT EXISTS meli-marketing.TEMP45.PZ_SENTS_CHURN
    (
        cus_cust_id INT64,
        sit_site_id STRING,
        notification_date DATE,
        EVENT_TYPE STRING
    );

    DELETE FROM  meli-marketing.TEMP45.PZ_SENTS_CHURN WHERE 1=1;

    INSERT INTO meli-marketing.TEMP45.PZ_SENTS_CHURN ( cus_cust_id,sit_site_id,notification_date,EVENT_TYPE)
    (
        select CAST(usr.user_id as INT64) as cus_cust_id, site as sit_site_id, DATE'"""+lu_day+"""' as notification_date,
        CASE WHEN json_extract_scalar(event_data,'$.batch_id') like '%_CG' then 'control'
             WHEN json_extract_scalar(event_data,'$.event_type') = 'sent' then 'sent'
             WHEN json_extract_scalar(event_data,'$.event_type') = 'shown' then 'shown'
             WHEN json_extract_scalar(event_data,'$.event_type') = 'open' then 'open'
             ELSE 'NA' END
        from `meli-bi-data.MELIDATA.TRACKS`
        where ds = DATE'"""+lu_day+"""'
            and bu = 'mercadolibre'
            and site  in ("""+site+""")
            and REGEXP_CONTAINS(json_extract_scalar(event_data,'$.campaign_id'), '(?i)churncupon')
            and json_extract_scalar(event_data,'$.batch_id') not like '%_BLACKL%'
            and json_extract_scalar(event_data,'$.batch_id') not like '%_HOLDRE%'
            and json_extract_scalar(event_data,'$.batch_id') not like '%_JNYBLACKLIST%'
            and json_extract_scalar(event_data,'$.event_type') in ('sent','shown','open')
            and path like '/notification/campaigns_%'
        group by 1,2,3,4
    );
"""

def tools_q(sites_str):
    return """ 
             SELECT CAST(CAMPAIGN_ID as INT64) as CAMPAIGN_ID
             FROM `meli-marketing.MODELLING.V_BT_MKT_TOOLS_CAMPAIGN` cp
             WHERE cp.mtc_site_id in ("""+sites_str+""")
                 AND lower(cp.mtc_name) LIKE '%churncupon%'
                 AND cp.mtc_start_date BETWEEN DATE_SUB(CURRENT_DATE() ,INTERVAL 120 DAY) and  CURRENT_DATE()
                 AND CAST(CAMPAIGN_ID as INT64) is not null
           """

def table_persuadidos(lu_day,sites_str,tools):
    return """
    CREATE TABLE IF NOT EXISTS meli-marketing.TEMP45.PZ_TMP_PERSUADIDOS
    (
        CUS_CUST_ID INT64,
        SIT_SITE_ID STRING,
        NOTIFICATION_DATE DATE,
        GRUPO STRING
    );

    DELETE FROM  meli-marketing.TEMP45.PZ_TMP_PERSUADIDOS WHERE 1=1;

    INSERT INTO meli-marketing.TEMP45.PZ_TMP_PERSUADIDOS ( CUS_CUST_ID,SIT_SITE_ID,NOTIFICATION_DATE,GRUPO)
    (
        WITH OPENS as(
            select b.cus_cust_id,b.sit_site_id,c.notification_date
            from meli-marketing.MKTPUBLIC.V_BT_PUSH_NOTIFICATION_EVENT b
            INNER JOIN meli-marketing.TEMP45.PZ_SENTS_CHURN c
                ON c.cus_cust_id = b.cus_cust_id
                AND c.sit_site_id = b.sit_site_id
            where lower(BATCH_ID) like '%churncupon%'
                and b.EVENT_TYPE in ('open')
                and APP = 'mercadolibre'
                AND b.SENT_DATE between notification_date AND DATE_ADD(notification_date,INTERVAL 1 DAY)
            group by 1,2,3
        ),
        CONVERSION_OPEN AS(   
            SELECT c.cus_cust_id,c.notification_date,c.sit_site_id
            from  meli-bi-data.WHOWNER.BT_ORD_ORDERS o
            INNER JOIN OPENS c
                ON c.cus_cust_id = o.ORD_BUYER.ID
                AND c.sit_site_id = o.sit_site_id
            WHERE ord_status = 'paid' and ORD_CLOSED_DT is not null
                  AND ORD_CLOSED_DT between ORD_CREATED_DT AND DATE_ADD(ORD_CREATED_DT,INTERVAL 4 DAY)
                  AND ORD_CREATED_DT between notification_date AND DATE_ADD(notification_date,INTERVAL 1 DAY)
            GROUP BY 1,2,3
        ),
         CUPONERO_ AS(
              SELECT   pay.cus_cust_id_buy as cus_cust_id,DATE'"""+lu_day+"""' as notification_date,pay.SIT_SITE_ID
              FROM  `meli-bi-data.WHOWNER.BT_PAY_PAYMENTS` as pay
              INNER JOIN `meli-bi-data.WHOWNER.BT_MKT_COUPON_V2` as cpn
                  on pay.pay_payment_id = cpn.pay_payment_id
                  and pay.cus_cust_id_buy = cpn.cus_cust_id_buy
                  AND mkt_cpn_campaign_id in ("""+tools+""")
              WHERE
                  pay.pay_status_code = 'approved'
                  AND pay.sit_site_id in ("""+sites_str+""")
                  AND pay.pay_created_dt BETWEEN DATE'"""+lu_day+"""' and DATE_ADD(DATE'"""+lu_day+"""',interval 1 day)
              GROUP BY 1,2,3
         ),
        CUPONERO AS (
              SELECT pay.cus_cust_id,s.notification_date,s.SIT_SITE_ID
              FROM  CUPONERO_ as pay
              INNER JOIN meli-marketing.TEMP45.PZ_SENTS_CHURN s
                  ON pay.cus_cust_id = s.cus_cust_id
                  AND pay.SIT_SITE_ID  = s.SIT_SITE_ID 
                  AND s.notification_date = pay.notification_date
                  AND EVENT_TYPE = 'sent'
              GROUP BY 1,2,3
        ),
        PERSUADIDOS_ AS(
            SELECT CAST(o.CUS_CUST_ID as INT64) as CUS_CUST_ID,o.SIT_SITE_ID,o.NOTIFICATION_DATE,'TG' as GRUPO
                FROM CONVERSION_OPEN o

                UNION ALL

                SELECT CAST(c.CUS_CUST_ID as INT64) as CUS_CUST_ID,c.SIT_SITE_ID,c.NOTIFICATION_DATE,'TG' as GRUPO
                FROM CUPONERO c

                UNION ALL

                SELECT CAST(d.CUS_CUST_ID as INT64) as CUS_CUST_ID,d.SIT_SITE_ID,d.NOTIFICATION_DATE,'CG' as GRUPO
                FROM meli-marketing.TEMP45.PZ_SENTS_CHURN d
                WHERE EVENT_TYPE = 'control'
        )
        SELECT CUS_CUST_ID,SIT_SITE_ID,NOTIFICATION_DATE, max(GRUPO) as GRUPO
        FROM PERSUADIDOS_
        GROUP BY 1,2,3        
        );
    """


def covariates_q():
    return """
    WITH RFM AS(
        SELECT o.ORD_BUYER.ID as CUS_CUST_ID, p.GRUPO,p.sit_site_id,
                COUNT(distinct ORD_CREATED_DT) as freq_rfm,
                MIN(ORD_CREATED_DT) as first_purchase_window,
                MAX(ORD_CREATED_DT) as recency,
                AVG(CASE WHEN ORD_ITEM.QTY*ORD_ITEM.BASE_CURRENT_PRICE > 400 THEN 400 ELSE ORD_ITEM.QTY*ORD_ITEM.BASE_CURRENT_PRICE END) AS GMV_MEAN
        FROM meli-bi-data.WHOWNER.BT_ORD_ORDERS o
        INNER JOIN meli-marketing.TEMP45.PZ_TMP_PERSUADIDOS p
            ON p.CUS_CUST_ID = o.ORD_BUYER.ID
            AND p.SIT_SITE_ID = o.SIT_SITE_ID

        WHERE ord_status = 'paid' and ORD_CLOSED_DT is not null
              AND ORD_CLOSED_DT between ORD_CREATED_DT AND DATE_ADD(ORD_CREATED_DT,INTERVAL 4 DAY)
              AND ORD_CREATED_DT between DATE_SUB(notification_date,INTERVAL 365 DAY) AND  DATE_SUB(notification_date,INTERVAL 1 DAY)
        GROUP BY 1,2,3
    ),
    INTERIN AS(
        SELECT o.ORD_BUYER.ID as CUS_CUST_ID, p.GRUPO,p.sit_site_id,
                COUNT(distinct ORD_CREATED_DT) as freq_7d,
                SUM(CASE WHEN ORD_ITEM.QTY*ORD_ITEM.BASE_CURRENT_PRICE > 400 THEN 400 ELSE ORD_ITEM.QTY*ORD_ITEM.BASE_CURRENT_PRICE END) AS GMV_7d
        FROM meli-bi-data.WHOWNER.BT_ORD_ORDERS o
        INNER JOIN meli-marketing.TEMP45.PZ_TMP_PERSUADIDOS p
            ON p.CUS_CUST_ID = o.ORD_BUYER.ID
            AND p.SIT_SITE_ID = o.SIT_SITE_ID

        WHERE ord_status = 'paid' and ORD_CLOSED_DT is not null
              AND ORD_CLOSED_DT between ORD_CREATED_DT AND DATE_ADD(ORD_CREATED_DT,INTERVAL 4 DAY)
              AND ORD_CREATED_DT between notification_date AND  DATE_ADD(notification_date,INTERVAL 6 DAY)
        GROUP BY 1,2,3
    ),
    VISITAS AS(
        SELECT  c.cus_cust_id,p.SIT_SITE_ID,p.GRUPO,
                MAX(c.log_date) as recency_date,
                COUNT(1) as cant_dias_active
        FROM  meli-marketing.MODELLING.LK_EVENT_CUSTOMER_ITEMS_AGG c
        INNER JOIN meli-marketing.TEMP45.PZ_TMP_PERSUADIDOS p
            ON c.cus_cust_id = p.cus_cust_id
            AND p.SIT_SITE_ID = c.SIT_SITE_ID
        WHERE c.log_date between DATE_SUB(notification_date,INTERVAL 90 DAY) AND  DATE_SUB(notification_date,INTERVAL 1 DAY)
        GROUP BY 1,2,3
    )
        SELECT p.cus_cust_id as CUS_CUST_ID ,p.SIT_SITE_ID,p.GRUPO,p.NOTIFICATION_DATE,
               recency_date,cant_dias_active,
               freq_7d,GMV_7d,
               freq_rfm,first_purchase_window,recency,GMV_MEAN
        FROM meli-marketing.TEMP45.PZ_TMP_PERSUADIDOS p
        LEFT JOIN VISITAS v
            ON p.cus_cust_id = v.cus_cust_id
            AND p.SIT_SITE_ID = v.SIT_SITE_ID
            AND p.GRUPO = v.GRUPO
        LEFT JOIN INTERIN i
            ON p.cus_cust_id = i.cus_cust_id
            AND p.SIT_SITE_ID = i.SIT_SITE_ID
            AND p.GRUPO = i.GRUPO
        LEFT JOIN RFM r
            ON p.cus_cust_id = r.cus_cust_id
            AND p.SIT_SITE_ID = r.SIT_SITE_ID
            AND p.GRUPO = r.GRUPO
"""

def preds_q(sites,days):
    return """
        SELECT p.cus_cust_id as user_id,f.sit_site_id,f.gmv_pred,f.freq_pred
        FROM meli-marketing.TEMP45.PZ_SENTS_CHURN p
        INNER JOIN  meli-marketing.MODELLING.PZ_ML_FORECAST3M f
            ON f.cus_cust_id = p.cus_cust_id
            AND f.sit_site_id = p.SIT_SITE_ID
            AND f.log_date =  DATE_ADD(p.NOTIFICATION_DATE,  INTERVAL """+days+""" day)
        WHERE p.SIT_SITE_ID in ("""+sites+""")
             and EVENT_TYPE in ('sent','control')
    """

def envios_q():
    return """

        SELECT  p.EVENT_TYPE AS GRUPO,p.SIT_SITE_ID,
                COUNT(distinct  CUS_CUST_ID) as buyers
        FROM meli-bi-data.WHOWNER.BT_ORD_ORDERS o
        INNER JOIN meli-marketing.TEMP45.PZ_SENTS_CHURN p
            ON p.CUS_CUST_ID = o.ORD_BUYER.ID
            AND p.SIT_SITE_ID = o.SIT_SITE_ID
            and EVENT_TYPE in ('sent','control')
        WHERE ord_status = 'paid' and ORD_CLOSED_DT is not null
              AND ORD_CLOSED_DT between ORD_CREATED_DT AND DATE_ADD(ORD_CREATED_DT,INTERVAL 4 DAY)
              AND ORD_CREATED_DT between notification_date AND DATE_ADD(notification_date,INTERVAL 1 DAY)
        GROUP BY 1,2
"""

def rates_q(lu_day,sites_str):
    return """
        select SIT_SITE_ID,EVENT_TYPE,count(distinct cus_cust_id) as cant
            from meli-marketing.TEMP45.PZ_SENTS_CHURN 
        GROUP BY 1,2
"""

def presupuesto_q(lu_day,sites_str):
    return """
    WITH TOOLS as(
            SELECT CAST(CAMPAIGN_ID as INT64) as CAMPAIGN_ID,cp.mtc_site_id as sit_site_id
             FROM `meli-marketing.MODELLING.V_BT_MKT_TOOLS_CAMPAIGN` cp
             WHERE cp.mtc_site_id in  ("""+sites_str+""")
             AND replace(lower(cp.MTC_NAME),' ','') LIKE '%churncupon%'
             AND cp.mtc_start_date BETWEEN DATE_SUB(CURRENT_DATE() ,INTERVAL 120 DAY) and  CURRENT_DATE()
         )
          SELECT pay.SIT_SITE_ID, SUM(mkt_cpn_amount) as AMOUNT
          FROM  `meli-bi-data.WHOWNER.BT_PAY_PAYMENTS` as pay
          INNER JOIN `meli-bi-data.WHOWNER.BT_MKT_COUPON_V2` as cpn
              on pay.pay_payment_id = cpn.pay_payment_id
              and pay.cus_cust_id_buy = cpn.cus_cust_id_buy
          INNER JOIN tools t
            on t.CAMPAIGN_ID = cpn.mkt_cpn_campaign_id
            AND t.SIT_SITE_ID = pay.SIT_SITE_ID
          WHERE
              pay.pay_status_code = 'approved'
              AND pay.pay_created_dt BETWEEN DATE'"""+lu_day+"""' and DATE_ADD(DATE'"""+lu_day+"""',interval 1 day)
          GROUP BY 1
"""

def cambio_q():
    return """
        SELECT c.SIT_SITE_ID,CCO_TC_VALUE 
        FROM `meli-bi-data.WHOWNER.LK_CURRENCY_CONVERTION` c
         INNER JOIN (select sit_site_id,max(notification_date) as FECHA from meli-marketing.TEMP45.PZ_SENTS_CHURN group by 1) a
            ON tim_day = FECHA
            AND c.SIT_SITE_ID = a.SIT_SITE_ID
        """

def resu_short_q(lu_day,sites_str,tools):
    return """
    WITH CONVERSION AS(   
        SELECT EVENT_TYPE,c.sit_site_id,count(distinct c.cus_cust_id) as buyers
        from meli-bi-data.WHOWNER.BT_ORD_ORDERS o
        INNER JOIN meli-marketing.TEMP45.PZ_SENTS_CHURN c
            ON c.cus_cust_id = o.ORD_BUYER.ID
            AND c.sit_site_id = o.sit_site_id
            AND EVENT_TYPE in ('sent','control')
        WHERE ord_status = 'paid' and ORD_CLOSED_DT is not null
              AND ORD_CLOSED_DT between ORD_CREATED_DT AND DATE_ADD(ORD_CREATED_DT,INTERVAL 4 DAY)
              AND ORD_CREATED_DT BETWEEN DATE'"""+lu_day+"""' and DATE_ADD(DATE'"""+lu_day+"""',interval 1 day)
        GROUP BY 1,2
    ),
     PRESUPUESTO AS(
          SELECT   pay.SIT_SITE_ID,'sent' as EVENT_TYPE,SUM(mkt_cpn_amount) as AMOUNT
          FROM  `meli-bi-data.WHOWNER.BT_PAY_PAYMENTS` as pay
          INNER JOIN `meli-bi-data.WHOWNER.BT_MKT_COUPON_V2` as cpn
              on pay.pay_payment_id = cpn.pay_payment_id
              and pay.cus_cust_id_buy = cpn.cus_cust_id_buy
              AND mkt_cpn_campaign_id in ("""+tools+""")
          WHERE
              pay.pay_status_code = 'approved'
              AND pay.sit_site_id in ("""+sites_str+""")
              AND pay.pay_created_dt BETWEEN DATE'"""+lu_day+"""' and DATE_ADD(DATE'"""+lu_day+"""',interval 1 day)
          GROUP BY 1,2
     ),
      ENVIO as(
        select b.sit_site_id,EVENT_TYPE,count(distinct cus_cust_id) as AMT
        from meli-marketing.TEMP45.PZ_SENTS_CHURN b
        group by 1,2
    )
    SELECT a.sit_site_id,a.EVENT_TYPE,AMT,AMOUNT,BUYERS
    FROM ENVIO a
    LEFT JOIN PRESUPUESTO b
        ON a.SIT_SITE_ID = b.SIT_SITE_ID
        AND a.EVENT_TYPE = b.EVENT_TYPE
    LEFT JOIN CONVERSION c
        ON a.SIT_SITE_ID = c.SIT_SITE_ID
        AND a.EVENT_TYPE = c.EVENT_TYPE
"""

def cambio2_q(lu_day,sites_str):
    return """
        SELECT c.SIT_SITE_ID,CCO_TC_VALUE 
        FROM `meli-bi-data.WHOWNER.LK_CURRENCY_CONVERTION` c
        WHERE tim_day = DATE'"""+lu_day+"""'
            AND c.SIT_SITE_ID in ("""+sites_str+""")
        """


def inc_metrics(x,cambio,presupuesto,TAKE_RATE):
    from causalml.inference.meta import LRSRegressor
    
    test = x.loc[x.GRUPO == "TG"]
    control = x.loc[x.GRUPO == "CG"]

#     users_ctr = pd.merge(users,users_all.loc[users_all.GRUPO == "CG"].drop(["GRUPO"],axis = 1),on="CUS_CUST_ID")
#     sizes = users_all.loc[users_all.LTV == G].groupby("GRUPO").size()
#     inc = test.shape[0] - (users_ctr.loc[users_ctr.LTV == G].shape[0]*sizes["TG"]/sizes["CG"]) 

    site_in = x.SIT_SITE_ID.unique()[0] # saco el site de aca
    u5 = test.assign(TREAT = True)
    u5 = u5.append(control.assign(TREAT = False))

    xg = LRSRegressor()
    te, lb, ub = xg.estimate_ate(u5[['recency', 'freq_rfm', 'first_purchase_window', 'recency_date', 'cant_dias_active']].values, u5.TREAT.values, u5.freq_pred.values)
    print(lb[0],te[0],ub[0])
    inc_freq = te[0]*test.shape[0]

    xg = LRSRegressor()
    te, lb, ub = xg.estimate_ate(u5[['recency', 'freq_rfm', 'first_purchase_window', 'recency_date', 'cant_dias_active', 'GMV_MEAN']].values, u5.TREAT.values, u5.gmv_pred.values)
    inc_gmv = te[0]*test.shape[0]
    inc_revenue = inc_gmv*TAKE_RATE[site_in] - (presupuesto/cambio)
    print(lb[0],te[0],ub[0])
    
    return inc_gmv,inc_revenue,inc_freq