# from bayes_opt import BayesianOptimization
import warnings
from sklearn.metrics import average_precision_score,precision_recall_curve,mean_squared_error, mean_absolute_error
from catboost import CatBoostClassifier,CatBoostRegressor,Pool
from sklearn.model_selection import StratifiedKFold
from sklearn.model_selection import train_test_split,KFold
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import ParameterSampler
from sklearn.model_selection import RandomizedSearchCV

import sys
sys.path.append(os.path.dirname(os.path.expanduser("../defines.py")))
from defines import *

def simple_fit(dinamic_params,X_train,y_train,X_valid,y_valid,foo_model_type,basic_params,d_types,cates):
    params = dict(basic_params,**dinamic_params)
    params = caster(d_types,params)
    gbm = foo_model_type(params)
    gbm.fit(X=X_train, y=y_train,cat_features = cates, eval_set=(X_valid, y_valid),verbose = False)
    return gbm

def foo_evaluation_classifier(y_test,y_pred):
#     precision, recall, _ = precision_recall_curve(y_test,y_pred)

#     df = pd.DataFrame([])
#     df["pre"] = precision
#     df["rec"]= recall
#     df = df.loc[(df.pre > 0.65) & (df.pre <= 0.7)]
    
#     return df.rec.mean()
    return average_precision_score(y_test,y_pred)

def foo_evaluation_regression(y_test,y_pred):
    return -mean_absolute_error(y_test,y_pred)

def foo_predict_classifier(model,X):
    return model.predict_proba(X)[:,1]

def foo_predict_regression(model,X):
    return model.predict(X)

def foo_model_regression(params):
    return CatBoostRegressor(**params)

def foo_model_classifier(params):
    return CatBoostClassifier(**params)

# Levanto el dataframe largo y entreno devuelta

def train_cv(aux,X,y,X_valid,y_valid,foo_evaluation,foo_predict,foo_model_type,basic_params,d_types,cates):
    l_ = []
    for train_index, test_index in KFold(n_splits=3,shuffle = True).split(X):
        X_train, X_test = X[train_index,:], X[test_index,:]
        y_train, y_test = y[train_index], y[test_index]

        gbm = simple_fit(aux,X_train,y_train,X_valid,y_valid,foo_model_type,basic_params,d_types,cates)
        y_pred = foo_predict(gbm,X_test)
        resu = foo_evaluation(y_test,y_pred)
        
        l_.append(resu)

    return np.mean(l_)

def train_combinations(X_train,X_valid,y_train,y_valid,foo_evaluation,foo_predict,foo_model_type,basic_params,d_types,hyperparams,cates,log="", n_iter = FULL_TRAINING_STEPS):
    gbm= foo_model_type(basic_params)
    rand = RandomizedSearchCV(gbm,hyperparams,scoring = foo_evaluation,n_iter = n_iter,n_jobs = n_jobs_train,verbose = 50,cv=3,random_state = 0)
    catboost_params = {"cat_features":cates,"eval_set":(X_valid,y_valid)}
    
    if(log != ""):
        orig_stdout = sys.stdout
        sys.stdout=open(log,"w")
        
    #rand.fit(X_train,y_train,None,**catboost_params)
    rand.fit(X_train,y_train,**catboost_params)
    
    if(log != ""):
        sys.stdout.close()
        sys.stdout=orig_stdout 
        
    return rand.best_params_

#     def lgb_evaluate(    # Cambiar esto en caso de usar otro modelo!
#                     learning_rate,
#                     depth,      
#                     l2_leaf_reg,
#                     border_count
#                     ):
#         params = {"learning_rate":learning_rate,
#                     "depth":depth,           
#                     "l2_leaf_reg":l2_leaf_reg,
#                     "border_count":border_count}
#         return train_cv(params,X_train,y_train,X_valid,y_valid,foo_evaluation,foo_predict,foo_model_type,basic_params,d_types,cates)

#     def bayesOpt(train_x, train_y):
#         lgbBO = BayesianOptimization(lgb_evaluate, hyperparams)


#         lgbBO.maximize(init_points=init_points, n_iter=n_iter)
#         return lgbBO
#         try:
#             print(lgbBO.res['max'])
#         except:
#             print("Non resu")

#     values = []
#     for i,params in enumerate(list(ParameterSampler(hyperparams, n_iter=n_iter,random_state=0))):
#         metric = train_cv(params,X_train,y_train,X_valid,y_valid,foo_evaluation,foo_predict,foo_model_type,basic_params,d_types,cates)
#         values.append({"target":metric,"params":params})
#         print("Iteration",i,"Target:",metric,"Params:",params)
        
#     resus = pd.DataFrame(values).sort_values(by = "target",ascending = False) # guardo los mejores scores
    
#     return resus

def caster(data_types,params):
    for d in data_types.items():
        params[d[0]] = d[1](params[d[0]])

    return params

def picture(X_train,y_train,X_valid, y_valid,categ_ind,best_model,foo_model_type,train_cols_nocat,cates):
    
    X_train_ = np.c_[X_train,np.random.normal(size=X_train.shape[0])]
    X_valid_ = np.c_[X_valid,np.random.normal(size=X_valid.shape[0])]
    
    params = best_model.get_params()
    gbm = foo_model_type(params)
    gbm.fit(X=X_train_, y=y_train,cat_features = categ_ind, eval_set=(X_valid_, y_valid),verbose = False)

    warnings.simplefilter(action='ignore', category=FutureWarning)

    # print_s3("Modelo con variable random entrenado","s3://fda-labs/ltv-ml/Lightgbm/log.txt")  

    feature_imp = pd.DataFrame(list(zip(train_cols_nocat+cates+["rand"], gbm.get_feature_importance(Pool(X_train_, label=y_train, cat_features=categ_ind)))),
                columns=['Feature','Value'])

    plt.figure(figsize=(50, 50))
    sns.set(font_scale = 5)
    sns.barplot(x="Value", y="Feature", data=feature_imp.sort_values(by="Value", 
                                                        ascending=False)[0:55])
    plt.title('LightGBM Features (avg over folds)')
    plt.tight_layout()
    plt.savefig('lgbmImportance.png')
    
    return feature_imp
    
def anti_dummies(summary_cal,cols,col_name):
    for i,c in enumerate(cols):
        summary_cal[c] = summary_cal[c]*(i+1)

    summary_cal[col_name] = summary_cal[cols].sum(axis = 1)
    return summary_cal.drop(cols,axis = 1)