import os
import pandas as pd
import s3fs

from melitk.analytics.connectors.core.authentication import Authentication
from melitk.analytics.connectors.teradata import ConnTeradata
from melitk.analytics.connectors.presto import ConnPresto
from melitk.analytics.connectors.hive import ConnHive
import datetime
from dateutil.relativedelta import relativedelta

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

def locations_queries(s3_path_in,teradata_app,start_cal_train,end_cal_test,file_name = "Location.csv"):
    teradata_app.drop_table(volatile)
    
    if(PAIS == 'MLB'):
        teradata_app.execute( 
            """
                CREATE MULTISET VOLATILE TABLE """+volatile+"""  AS
                (
                    SELECT a.CUS_CUST_ID, SHP_ADD_STATE_NAME_R, SHP_ADD_ZIP_CODE
                    FROM(
                         SELECT
                             B.CUS_CUST_ID_BUY AS CUS_CUST_ID,
                             RECEPTOR.SHP_ADD_STATE_NAME AS SHP_ADD_STATE_NAME_R,
                             SHP_ADD_ZIP_CODE,
                             COUNT(1) as MOST_SEEN
                         FROM whowner.bt_bids  b
                         INNER JOIN      WHOWNER. BT_SHP_SHIPPING_ITEMS AS SI
                            ON      b.sit_site_id=SI.sit_site_id 
                            AND b.BID_BID_STATUS = 'W'
                            AND b.SIT_SITE_ID = '"""+PAIS+"""'
                            AND b.BID_BASE_CURRENT_PRICE < 10000
                            AND b.TIM_DAY_WINNING_DATE BETWEEN DATE '"""+start_cal_train.strftime("%Y-%m-%d")+"""' AND DATE '"""+end_cal_test.strftime("%Y-%m-%d")+"""'
                            AND b.PHOTO_ID='TODATE'
                            AND B.ITE_GMV_FLAG = 1
                            AND  b.ord_order_id=CAST(SI.shp_order_id AS BIGINT)
                            AND b.ITE_ITEM_ID = SI.ITE_ITEM_ID
                         INNER JOIN BT_SHP_SHIPMENTS C
                             ON  C.SHP_SHIPMENT_ID = SI.SHP_SHIPMENT_ID
                             AND c.SHP_Type = 'forward'
                         LEFT JOIN  LK_SHP_ADDRESS RECEPTOR
                         --ON  RECEPTOR.SHP_ADD_COUNTRY_ID='BR'
                             ON  RECEPTOR.SHP_ADD_COUNTRY_ID='BR'
                             AND  C.SHP_RECEIVER_ADDRESS=RECEPTOR.SHP_ADD_ID
                         WHERE c.SHP_DATE_CANCELLED_ID IS NULL
                               AND EXISTS (SELECT 1 FROM  """+tabla_pivot2+""" u
                                             WHERE b.CUS_CUST_ID_BUY = u.CUS_CUST_ID)
                         GROUP BY 1,2,3
                    ) a
                    qualify row_number() over (partition by a.CUS_CUST_ID order by a.MOST_SEEN) = 1
                ) WITH DATA PRIMARY INDEX ( CUS_CUST_ID )
                  ON COMMIT PRESERVE ROWS;
            """
        )

        teradata_app.drop_table(volatile2)

        teradata_app.execute( 
            """
                CREATE MULTISET VOLATILE TABLE """+volatile2+"""  AS
                (
                    SELECT A.cus_cust_id,REG_ADDR_STATE_NAME
                    from """+tabla_pivot2+""" A
                    INNER JOIN WHOWNER.LK_REG_CUSTOMERS R ON A.CUS_CUST_ID=R.CUS_CUST_ID 
                    INNER JOIN (
                                    select * 
                                    from WHOWNER.LK_REG_PERSON_ADDRESS
                                    QUALIFY RANK() OVER (PARTITION BY REG_CUST_DOC_NUMBER, REG_CUST_DOC_TYPE ORDER BY REG_ADDR_ADDRESS_ID DESC) = 1
                                ) R2 
                        ON R.REG_CUST_DOC_TYPE = R2.REG_CUST_DOC_TYPE and R.REG_CUST_DOC_NUMBER = R2.REG_CUST_DOC_NUMBER 
                ) WITH DATA PRIMARY INDEX ( cus_cust_id )
                  ON COMMIT PRESERVE ROWS;
            """
        )

        teradata_app.drop_table(tabla_destroy)

        teradata_app.execute( 
            """
                CREATE MULTISET TABLE """+tabla_destroy+""",
                 NO BEFORE JOURNAL,
                 NO AFTER JOURNAL,
                 CHECKSUM = DEFAULT,
                 DEFAULT MERGEBLOCKRATIO
                 as
                 (  
                     select A.CUS_CUST_ID, COALESCE(b.SHP_ADD_STATE_NAME_R,c.REG_ADDR_STATE_NAME) as SHP_ADD_STATE_NAME_R
                     FROM """+tabla_pivot2+""" A
                     LEFT JOIN """+volatile+""" b ON A.CUS_CUST_ID = b.CUS_CUST_ID
                     LEFT JOIN """+volatile2+""" c ON A.CUS_CUST_ID = c.CUS_CUST_ID
                ) WITH DATA PRIMARY INDEX ( cus_cust_id );
            """
        )

        teradata_app.fast_export("select * from "+tabla_destroy,"s3://"+fda_path+s3_path_in+file_name)

        teradata_app.drop_table(tabla_destroy)
        
    if(PAIS == 'MLM'):

        query2 = """
                    SELECT CUS_CUST_ID, SHP_ADD_STATE_NAME_R, SHP_ADD_ZIP_CODE
                    FROM(
                          SELECT
                              B.CUS_CUST_ID_BUY AS CUS_CUST_ID,
                              RECEPTOR.SHP_ADD_STATE_NAME AS SHP_ADD_STATE_NAME_R,
                              SHP_ADD_ZIP_CODE,
                              COUNT(*) as MOST_SEEN
                          FROM whowner.bt_bids  b
                          INNER JOIN      WHOWNER. BT_SHP_SHIPPING_ITEMS AS SI
                                 ON      b.sit_site_id=SI.sit_site_id 
                                 AND b.BID_BID_STATUS = 'W'
                                 AND b.SIT_SITE_ID = '"""+PAIS+"""'
                                 AND b.BID_BASE_CURRENT_PRICE < 10000
                                 AND b.TIM_DAY_WINNING_DATE BETWEEN DATE '"""+start_cal_train.strftime("%Y-%m-%d")+"""' AND DATE '"""+end_cal_test.strftime("%Y-%m-%d")+"""'
                                 AND b.PHOTO_ID='TODATE'
                                 AND B.ITE_GMV_FLAG = 1
                                  AND  b.ord_order_id=CAST(SI.shp_order_id AS BIGINT)
                                  AND b.ITE_ITEM_ID = SI.ITE_ITEM_ID
                          INNER JOIN BT_SHP_SHIPMENTS C
                              ON  C.SHP_SHIPMENT_ID = SI.SHP_SHIPMENT_ID
                              AND c.SHP_Type = 'forward'
                          LEFT JOIN  LK_SHP_ADDRESS RECEPTOR
                              ON  RECEPTOR.SHP_ADD_COUNTRY_ID='MX'
                              AND  C.SHP_RECEIVER_ADDRESS=RECEPTOR.SHP_ADD_ID
                          WHERE c.SHP_DATE_CANCELLED_ID IS NULL
                              AND EXISTS(SELECT 1 FROM """+tabla_pivot2+""" a WHERE a.CUS_CUST_ID = b.CUS_CUST_ID_BUY) 
                          GROUP BY 1,2,3
                      ) a
                    qualify
                       row_number() over (partition by a.CUS_CUST_ID order by a.MOST_SEEN) = 1
        """

        teradata_app.fast_export(query2,"s3://"+fda_path+s3_path_in+file_name,sep ="|")
        
    if(PAIS == 'MLA'):
        query2 = """
                    SELECT CUS_CUST_ID, SHP_ADD_STATE_NAME_R, SHP_ADD_ZIP_CODE
                    FROM(
                          SELECT
                              B.CUS_CUST_ID_BUY AS CUS_CUST_ID,
                              RECEPTOR.SHP_ADD_STATE_NAME AS SHP_ADD_STATE_NAME_R,
                              SHP_ADD_ZIP_CODE,
                              COUNT(*) as MOST_SEEN
                          FROM whowner.bt_bids  b
                          INNER JOIN      WHOWNER. BT_SHP_SHIPPING_ITEMS AS SI
                                 ON      b.sit_site_id=SI.sit_site_id 
                                 AND b.BID_BID_STATUS = 'W'
                                 AND b.SIT_SITE_ID = '"""+PAIS+"""'
                                 AND b.BID_BASE_CURRENT_PRICE < 10000
                                 AND b.TIM_DAY_WINNING_DATE BETWEEN DATE '"""+start_cal_train.strftime("%Y-%m-%d")+"""' AND DATE '"""+end_cal_test.strftime("%Y-%m-%d")+"""'
                                 AND b.PHOTO_ID='TODATE'
                                 AND B.ITE_GMV_FLAG = 1
                                  AND  b.ord_order_id=CAST(SI.shp_order_id AS BIGINT)
                                  AND b.ITE_ITEM_ID = SI.ITE_ITEM_ID
                          INNER JOIN BT_SHP_SHIPMENTS C
                              ON  C.SHP_SHIPMENT_ID = SI.SHP_SHIPMENT_ID
                              AND c.SHP_Type = 'forward'
                          LEFT JOIN  LK_SHP_ADDRESS RECEPTOR
                              ON  RECEPTOR.SHP_ADD_COUNTRY_ID='AR'
                              AND  C.SHP_RECEIVER_ADDRESS=RECEPTOR.SHP_ADD_ID
                          WHERE c.SHP_DATE_CANCELLED_ID IS NULL
                              AND EXISTS(SELECT 1 FROM """+tabla_pivot2+""" a WHERE a.CUS_CUST_ID = b.CUS_CUST_ID_BUY) 
                          GROUP BY 1,2,3
                      ) a
                    qualify
                       row_number() over (partition by a.CUS_CUST_ID order by a.MOST_SEEN) = 1
        """

        teradata_app.fast_export(query2,"s3://"+fda_path+s3_path_in+file_name,sep ="|")
        
    if(PAIS == 'MLC'):
        query2 = """
                    SELECT CUS_CUST_ID, SHP_ADD_STATE_NAME_R, SHP_ADD_ZIP_CODE
                    FROM(
                          SELECT
                              B.CUS_CUST_ID_BUY AS CUS_CUST_ID,
                              RECEPTOR.SHP_ADD_STATE_NAME AS SHP_ADD_STATE_NAME_R,
                              SHP_ADD_ZIP_CODE,
                              COUNT(*) as MOST_SEEN
                          FROM whowner.bt_bids  b
                          INNER JOIN      WHOWNER. BT_SHP_SHIPPING_ITEMS AS SI
                                 ON      b.sit_site_id=SI.sit_site_id 
                                 AND b.BID_BID_STATUS = 'W'
                                 AND b.SIT_SITE_ID = '"""+PAIS+"""'
                                 AND b.BID_BASE_CURRENT_PRICE < 10000
                                 AND b.TIM_DAY_WINNING_DATE BETWEEN DATE '"""+start_cal_train.strftime("%Y-%m-%d")+"""' AND DATE '"""+end_cal_test.strftime("%Y-%m-%d")+"""'
                                 AND b.PHOTO_ID='TODATE'
                                 AND B.ITE_GMV_FLAG = 1
                                  AND  b.ord_order_id=CAST(SI.shp_order_id AS BIGINT)
                                  AND b.ITE_ITEM_ID = SI.ITE_ITEM_ID
                          INNER JOIN BT_SHP_SHIPMENTS C
                              ON  C.SHP_SHIPMENT_ID = SI.SHP_SHIPMENT_ID
                              AND c.SHP_Type = 'forward'
                          LEFT JOIN  LK_SHP_ADDRESS RECEPTOR
                              ON  RECEPTOR.SHP_ADD_COUNTRY_ID='CL'
                              AND  C.SHP_RECEIVER_ADDRESS=RECEPTOR.SHP_ADD_ID
                          WHERE c.SHP_DATE_CANCELLED_ID IS NULL
                              AND EXISTS(SELECT 1 FROM """+tabla_pivot2+""" a WHERE a.CUS_CUST_ID = b.CUS_CUST_ID_BUY) 
                          GROUP BY 1,2,3
                      ) a
                    qualify
                       row_number() over (partition by a.CUS_CUST_ID order by a.MOST_SEEN) = 1
        """

        teradata_app.fast_export(query2,"s3://"+fda_path+s3_path_in+file_name,sep ="|")
        
    if(PAIS == 'MLU'):
        query2 = """
                    SELECT CUS_CUST_ID, SHP_ADD_STATE_NAME_R, SHP_ADD_ZIP_CODE
                    FROM(
                          SELECT
                              B.CUS_CUST_ID_BUY AS CUS_CUST_ID,
                              RECEPTOR.SHP_ADD_STATE_NAME AS SHP_ADD_STATE_NAME_R,
                              SHP_ADD_ZIP_CODE,
                              COUNT(*) as MOST_SEEN
                          FROM whowner.bt_bids  b
                          INNER JOIN      WHOWNER. BT_SHP_SHIPPING_ITEMS AS SI
                                 ON      b.sit_site_id=SI.sit_site_id 
                                 AND b.BID_BID_STATUS = 'W'
                                 AND b.SIT_SITE_ID = '"""+PAIS+"""'
                                 AND b.BID_BASE_CURRENT_PRICE < 10000
                                 AND b.TIM_DAY_WINNING_DATE BETWEEN DATE '"""+start_cal_train.strftime("%Y-%m-%d")+"""' AND DATE '"""+end_cal_test.strftime("%Y-%m-%d")+"""'
                                 AND b.PHOTO_ID='TODATE'
                                 AND B.ITE_GMV_FLAG = 1
                                  AND  b.ord_order_id=CAST(SI.shp_order_id AS BIGINT)
                                  AND b.ITE_ITEM_ID = SI.ITE_ITEM_ID
                          INNER JOIN BT_SHP_SHIPMENTS C
                              ON  C.SHP_SHIPMENT_ID = SI.SHP_SHIPMENT_ID
                              AND c.SHP_Type = 'forward'
                          LEFT JOIN  LK_SHP_ADDRESS RECEPTOR
                              ON  RECEPTOR.SHP_ADD_COUNTRY_ID='UY'
                              AND  C.SHP_RECEIVER_ADDRESS=RECEPTOR.SHP_ADD_ID
                          WHERE c.SHP_DATE_CANCELLED_ID IS NULL
                              AND EXISTS(SELECT 1 FROM """+tabla_pivot2+""" a WHERE a.CUS_CUST_ID = b.CUS_CUST_ID_BUY) 
                          GROUP BY 1,2,3
                      ) a
                    qualify
                       row_number() over (partition by a.CUS_CUST_ID order by a.MOST_SEEN) = 1
        """

        teradata_app.fast_export(query2,"s3://"+fda_path+s3_path_in+file_name,sep ="|")
        
    
    if(PAIS=='MCO'):
        query2 = """
                    SELECT CUS_CUST_ID, SHP_ADD_STATE_NAME_R, SHP_ADD_ZIP_CODE
                    FROM(
                          SELECT
                              B.CUS_CUST_ID_BUY AS CUS_CUST_ID,
                              RECEPTOR.SHP_ADD_STATE_NAME AS SHP_ADD_STATE_NAME_R,
                              SHP_ADD_ZIP_CODE,
                              COUNT(*) as MOST_SEEN
                          FROM whowner.bt_bids  b
                          INNER JOIN      WHOWNER. BT_SHP_SHIPPING_ITEMS AS SI
                                 ON      b.sit_site_id=SI.sit_site_id 
                                 AND b.BID_BID_STATUS = 'W'
                                 AND b.SIT_SITE_ID = '"""+PAIS+"""'
                                 AND b.BID_BASE_CURRENT_PRICE < 10000
                                 AND b.TIM_DAY_WINNING_DATE BETWEEN DATE '"""+start_cal_train.strftime("%Y-%m-%d")+"""' AND DATE '"""+end_cal_test.strftime("%Y-%m-%d")+"""'
                                 AND b.PHOTO_ID='TODATE'
                                 AND B.ITE_GMV_FLAG = 1
                                  AND  b.ord_order_id=CAST(SI.shp_order_id AS BIGINT)
                                  AND b.ITE_ITEM_ID = SI.ITE_ITEM_ID
                          INNER JOIN BT_SHP_SHIPMENTS C
                              ON  C.SHP_SHIPMENT_ID = SI.SHP_SHIPMENT_ID
                              AND c.SHP_Type = 'forward'
                          LEFT JOIN  LK_SHP_ADDRESS RECEPTOR
                              ON  RECEPTOR.SHP_ADD_COUNTRY_ID='CO'
                              AND  C.SHP_RECEIVER_ADDRESS=RECEPTOR.SHP_ADD_ID
                          WHERE c.SHP_DATE_CANCELLED_ID IS NULL
                              AND EXISTS(SELECT 1 FROM """+tabla_pivot2+""" a WHERE a.CUS_CUST_ID = b.CUS_CUST_ID_BUY) 
                          GROUP BY 1,2,3
                      ) a
                    qualify
                       row_number() over (partition by a.CUS_CUST_ID order by a.MOST_SEEN) = 1
        """
        teradata_app.fast_export(query2,"s3://"+fda_path+s3_path_in+file_name,sep ="|")