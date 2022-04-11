import sys, os 
prj_path = os.path.dirname(os.getcwd())
utils_path = prj_path + '/utils'
if  not utils_path in sys.path:
    print('adding utils to path ')
    sys.path.insert(1, utils_path)
    
from utilities import *

def proc_pushvars(df:pd.DataFrame)->pd.DataFrame:
    """Pre-processing procedure for push notification variables.
    df: DataFrame with the variables.
    """
    logger.info(f"--------- START PROCESSING PROCEDURE ON PUSHVAR ---------")
    logger.info(f"INPUT DATAFRAME {df.shape} unique users {df.CUS_CUST_ID.nunique()}")
    
    df['SENT_DATE'] = pd.to_datetime(df.SENT_DATE)
    
    recency_na = np.round(df['RECENCY'].isna().mean(),3)*100
    logger.info(f"FILL RECENCY NA VALUES ({recency_na} %) WITH MAX(RECENCY)+1")
    df['RECENCY'] = df['RECENCY'].fillna(np.max(df.RECENCY)+1).astype(np.int64)
    
    r_push_na = np.round(df['R_PUSH_LAST_15D_VS_REST_OF_MONTH'].isna().mean(),3)*100
    logger.info(f"FILL R_PUSH_LAST_15D_VS_REST_OF_MONTH NA ({r_push_na}) VALUES WITH -1")
    df['R_PUSH_LAST_15D_VS_REST_OF_MONTH'] = df['R_PUSH_LAST_15D_VS_REST_OF_MONTH']\
    .fillna(-1).astype(np.int64)
    
    logger.info(f"ADDING VAR R_PUSH_RANK")
    df['R_PUSH_RANK'] = df['R_PUSH_LAST_15D_VS_REST_OF_MONTH'].rank(na_option='bottom',pct=True)
    
    df = prefix_source_info(df = df,
                            prefix = conf['PREFIX_MASTERS']['PUSH_VARS'],
                            exceptions= ['Y','W','SENT_DATE','CUS_CUST_ID'])
    
    logger.info(f"OUTPUT DATAFRAME {df.shape} unique users {df.CUS_CUST_ID.nunique()}")
    logger.info(f"--------- END PROCESSING PROCEDURE ON PUSHVAR ---------")
    return df

def proc_behaviour(df: pd.DataFrame)->pd.DataFrame:
    """Pre-processing procedure for events on :
    marketplace and MP app.
    df: DataFrame with the variables.
    """
    logger.info(f"--------- START PROCESSING PROCEDURE ON BEHAVIOUR ---------")
    logger.info(f"INPUT DATAFRAME {df.shape} unique users {df.CUS_CUST_ID.nunique()}")
    
    logger.info(f"CAST VARIABLES TYPE")
    df['SENT_DATE'] = pd.to_datetime(df.SENT_DATE)
    df['MIN_PRICE_ITEM'] = df['MIN_PRICE_ITEM'].astype(np.float64)
    df['MAX_PRICE_ITEM'] = df['MAX_PRICE_ITEM'].astype(np.float64)
    df['TOTAL_PAYMENTS_AMT_SUM'] = df['TOTAL_PAYMENTS_AMT_SUM'].astype(np.float64)
    df['WALLET_PAYMENTS_AMT_SUM'] = df['WALLET_PAYMENTS_AMT_SUM'].astype(np.float64)
    df['AGGREGATOR_PAYMENTS_AMT_SUM'] = df['AGGREGATOR_PAYMENTS_AMT_SUM'].astype(np.float64)
    df['AVG_PRICE_ITEM_7D'] = df['AVG_PRICE_ITEM_7D'].astype(np.float64)
    df['AVG_PRICE_ITEM_F7D'] = df['AVG_PRICE_ITEM_F7D'].astype(np.float64)
    
    logger.info(f"Clipping prices and money by percentil 95")
    cols = ['AVG_PRICE_ITEM_7D','CAT_CATEG_ID_COUNT_7D', 
            'AVG_PRICE_ITEM_F7D','MIN_PRICE_ITEM', 'MAX_PRICE_ITEM', 
            'BUY_INTENTION_ITEM_COUNT','CAT_CATEG_ID_COUNT_F7D', 
            'VIP_VIEW_APP_ANDROID_ITEM_COUNT','VIP_VIEW_APP_IOS_ITEM_COUNT',
            'VIP_VIEW_DESKTOP_ITEM_COUNT','VIP_VIEW_WEB_MOBILE_ITEM_COUNT', 
            'VIP_VIEW', 'QUESTION_ITEM_COUNT','CHECKOUT_CONGRATS_ITEM_COUNT',
            'ADD_TO_CART_ITEM_COUNT','BOOKMARK_ITEM_COUNT', 
            'TOTAL_PAYMENTS_AMT_SUM','WALLET_PAYMENTS_AMT_SUM', 
            'AGGREGATOR_PAYMENTS_AMT_SUM','TOTAL_PAYMENTS_COUNT']
    for col in cols:
        df[col] =  outlier_clipping(df[col].copy(),q = .95)
    
    recency_na = np.round(df['RECENCY'].isna().mean(),3)*100
    logger.info(f"FILL RECENCY NA VALUES ({recency_na} %) WITH MAX(RECENCY)+1")
    m = np.max(df.RECENCY) + 1
    df['RECENCY'].fillna(m,inplace = True)
    df['RECENCY'] = df['RECENCY'].astype(np.int64)

    cols = ['MIN_PRICE_ITEM','MAX_PRICE_ITEM',
            'MAX_FROM_INSTALL_ML','MIN_FROM_INSTALL_ML',
            'MAX_FROM_INSTALL_MP','MIN_FROM_INSTALL_MP']
    for col in cols:
        logger.info(f"FILL NA {col} ({np.round(df[col].isna().mean(),2)*100}%) VALUES WITH -1")
        df[col].fillna(-1,inplace=True)
    
    os_na = np.round(df['OS_NAME'].isna().mean(),3)*100
    logger.info(f"FILL NA VALUES IN OS_NAME ({os_na} %) BY ORIGINAL DISTIRBUTION")
    class_p = df['OS_NAME'].value_counts(normalize=True).to_dict()
    categorical_imputer(series = df['OS_NAME'],proportions = class_p)
    df['OS_IS_ANDROID'] = df['OS_NAME'].map({'android':1,'ios':0})
    df.drop(columns='OS_NAME', inplace=True)
    
    df = prefix_source_info(df = df,
                            prefix = conf['PREFIX_MASTERS']['BEHAVIOUR_VARS'],
                            exceptions= ['Y','W','SENT_DATE','CUS_CUST_ID'])
    
    logger.info(f"OUTPUT DATAFRAME {df.shape} unique users {df.CUS_CUST_ID.nunique()}")
    logger.info(f"--------- END PROCESSING PROCEDURE ON BEHAVIOUR ---------")
    return df