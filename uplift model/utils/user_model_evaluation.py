import sys, os 
prj_path = os.path.dirname(os.getcwd())
utils_path = prj_path + '/utils'
if  not utils_path in sys.path:
    print('adding utils to path ')
    sys.path.insert(1, utils_path)
    
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import auc

def fancy_bar_values(x=list,
                     y=list,
                     s = list,
                     ax=None,
                     fancy = {'fontweight':'bold',
                              'horizontalalignment':'center',
                              'rotation':45,
                              'fontsize':8}
                    ):
    
    for x_, y_,s_ in zip(x,y,s):
        ax.text(x = x_, 
                y = y_*.45,
                s = str(s_),
                **fancy)
        
def UpliftSummarizeBucket(y_true:list,
                    treatment_vector:list,
                    y_predict:list):
    """DataFrame summarizing by 10% buckets incremental model.
    y_true: actual outcome (for the momentos binary outcome 
    {0 = no event,1 = event})
    treatment_vector: vector indicating treatment group 
    {0 for control, 1 for treatment}
    y_predict: uplift score (.predict output)"""
      
    min_aux = np.min(y_predict)
    max_aux = np.max(y_predict)
    x = [(x - min_aux)/(max_aux - min_aux) for x in y_predict]
    eval_data = pd.DataFrame({'y_true':y_true,
                              'treatment_vector':treatment_vector,
                              'y_predict':y_predict,
                              'rank':x}).sort_values(by = 'rank',ascending=False)
    
    eval_data['bucket']= pd.qcut(-1*eval_data['rank'],q = 10,labels=np.arange(10,110,10))
    
    eval_data_group = eval_data.groupby(['bucket','treatment_vector']).agg(
        conversion = ('y_true',np.mean),
        elements = ('y_true',np.sum),
        n = ('y_true','count'),
        y_predict_avg = ('y_predict',np.mean)).reset_index()
    
    eval_data_group['bucket'] = eval_data_group.bucket.astype(int)
    
    return eval_data_group, eval_data.y_true.mean()

def conversion_incremental_plot(y_true:list,
                                treatment_vector:list,
                                y_predict:list,
                                ax1=None,ax2=None,
                                plot = True):
    """Estimate the rate of events by buket and treatment group 
    and the differences between this rates (uplift).
    
    y_true: actual outcome (for the momentos binary outcome 
    {0 = no event,1 = event})
    treatment_vector: vector indicating treatment group 
    {0 for control, 1 for treatment}
    y_predict: uplift score (.predict output) """
          
    data, conversion  = UpliftSummarizeBucket(y_true = y_true,
                                     treatment_vector = treatment_vector,
                                     y_predict = y_predict)
    
    ##Conversion for treatment and control group.
    # Conversion : Total of units with positive outcome over the total of units per bucket.
    treatment = data.treatment_vector==1
    control = data.treatment_vector==0
    treatment_conversion = data.loc[treatment].conversion.reset_index(drop=True)
    control_conversion = data.loc[control].conversion.reset_index(drop=True)
    
    #Base rate of conversion.
    base_conversion = np.sum(data.elements)/np.sum(data.n)
    #Lift in the best 10%
    lift_conversion_top10 = treatment_conversion[0]/base_conversion
    
    #Uplift.
    conversion_delta = treatment_conversion - control_conversion
    uplift_top30 = np.sum(conversion_delta[:2])  
    
    if (ax1 == None) & (plot):
            fig, (ax1,ax2)= plt.subplots(2, sharex=True ,figsize =(12,6))
    
    
    if plot:
        x = data.bucket.unique()
        width = 2.5
        
        treatment = ax1.bar(x - width/2,
                            treatment_conversion,
                            width,
                            label='treatment')
        control = ax1.bar(x + width/2,
                          control_conversion,
                          width,
                          label='control')

        ax1.set_ylabel('Conversion')
        ax1.hlines(conversion,
                   xmin=0,xmax= np.max(x),
                   linestyles ='dashed',
                   colors ='k',
                   label = f'total conversion: {np.round(conversion,3)}'
                  )
        
        ax1.legend()
        fancy_bar_values(x = x - width/2,
                         y = treatment_conversion,
                         s = np.round(treatment_conversion,3),
                         ax=ax1)
        
        fancy_bar_values(x = x + width/2,
                         y = control_conversion,
                         s = np.round(control_conversion,3),
                         ax = ax1)

        ax1.xaxis.set_ticks(np.arange(10, 110, 10))
        ax1.axes.yaxis.set_ticks([])

        ax2.bar(x = x,
                height = conversion_delta,
                width = 5,
                color = np.where(conversion_delta>=0,'C0','r'))
        fancy_bar_values(x = x,
                         y = conversion_delta,
                         s = np.round(conversion_delta,3),
                         ax = ax2)
        ax2.xaxis.set_ticks(np.arange(10, 110, 10))
        ax2.axes.yaxis.set_ticks([])
        ax2.set_ylabel('Uplift')

    return lift_conversion_top10, uplift_top30

def gain_plot(y_true:list,
              treatment_vector:list,
              y_predict:list,
              ax1 = None,ax2 = None, 
              plot = True):
    
    """Estimate the gini coefficent by cumulative gain chart or uplift curve.
    The distribution of the treatment groups by bukets of 10% and finally the
     
    y_true: actual outcome (for the momentos binary outcome 
    {0 = no event,1 = event})
    treatment_vector: vector indicating treatment group 
    {0 for control, 1 for treatment}
    y_predict: uplift score (.predict output)
    """
    
    data, conversion  = UpliftSummarizeBucket(y_true = y_true,
                                              treatment_vector = treatment_vector,
                                              y_predict = y_predict)
    if (ax1 == None) & (plot):
            fig, (ax1,ax2)= plt.subplots(2, sharex=True ,figsize =(12,6))

    #Summarizing by buckets (elements: Number of units with positive outcome)
    data_ = data.groupby('bucket').agg(elements = ('elements','sum'))
    data_['acc_recall'] = data_.elements.cumsum()
    data_.reset_index(inplace = True)
    
    #List of buckets and cummulative postive outcome
    x = data_.bucket.to_list()
    x.insert(0,0)
    y = data_.acc_recall.to_list()
    y.insert(0,0)
    
    #Areas under the curves.
    auc_actual_model = auc(x,y)
    auc_random_model = max(x)*max(y)*.5
    auc_optimal_model = auc([0,conversion*100,max(x)],[0,max(y),max(y)])
    
    ##Gini coefficent.
    #(Check the definition on:
    #Uplift Modeling: Identifying Optimal Treatment Group
    #Allocation and Whom to Contact to Maximize Return on Investment)
    gini_coef =(auc_actual_model-auc_random_model)/\
    (auc_optimal_model- auc_random_model)
    gini_coef = np.round(gini_coef,3)
    
    n = data_.loc[2].acc_recall*100
    m = max(data_.acc_recall)
    #Gain onthe top 30% of the data (sorted by score)
    gain_top30 = np.round(n/m,2)

    if plot == True:
        ax1.plot(x,y, marker='*',
                 label = f'actual model - Gini coefficient {gini_coef}')
        for x_,y_ in zip(data_.bucket,data_.acc_recall):
                y_2 = np.round(y_*100/max(data_.acc_recall),0)
                w = np.int16(y_2)
                ax1.text(x_,y_,str(w)+' %',
                         horizontalalignment='center',
                         verticalalignment='bottom')
        #Random model
        ax1.plot([0,max(data_.bucket)],[0,np.max(data_.acc_recall)],
                 label = 'random model')
        #Perfect model
        ax1.plot([0,conversion*100,max(x)],[0,max(y),max(y)],
                 label = 'optimal model')  
        ax1.set_ylabel('Gain chart')
        ax1.legend()
        ax1.xaxis.set_ticks(np.arange(10, 110, 10))
        ax1.axes.yaxis.set_ticks([])

        x_control = data.loc[data.treatment_vector==0].bucket
        y_control_ = data.loc[data.treatment_vector ==0].n
        x_variant = data.loc[data.treatment_vector==1].bucket
        y_variant_ = data.loc[data.treatment_vector ==1].n

        y_control = y_control_*100/(y_control_.values+y_variant_.values)
        y_variant = y_variant_*100/(y_control_.values+y_variant_.values)
        labels = [str(np.round(x,1))+' %' for x in y_variant]                   
        ax2.bar(x_variant,y_variant,width = 5, label = 'treatment')
        fancy_bar_values(x = x_variant,
                         y = y_variant,
                         s = labels,
                         ax = ax2)
        ax2.bar(x_control,y_control,bottom = y_variant,
                width = 5, label = 'control')
        ax2.set_ylabel('Proportion \n Treatment vs. control')
        ax2.legend()
        ax2.xaxis.set_ticks(np.arange(10, 110, 10))
        ax2.axes.yaxis.set_ticks([])

    return gini_coef, gain_top30

def qini_plot(y_true:list,
              treatment_vector:list,
              y_predict:list,
              ax1 = None,
              plot = True,
              negative_efect = False):
    """Estimate the Qini coefficent from the qini curve.
    y_true: actual outcome (for the momentos binary outcome 
    {0 = no event,1 = event})
    treatment_vector: vector indicating treatment group 
    {0 for control, 1 for treatment}
    y_predict: uplift score (.predict output)
    """
    
    data, _  = UpliftSummarizeBucket(y_true = y_true,
                                     treatment_vector = treatment_vector,
                                     y_predict = y_predict)
    #Cast treatment vector and some variables.
    data['treatment_vector'] = data.treatment_vector.map({0:'control',
                                                          1:'variant'})
    data['elements'] = data.elements.astype(int)
    data['n'] = data.n.astype(int)
    #pivoting(bucket|control_elements|variant_elements..)
    d = data.pivot_table(index = 'bucket',
                         columns = 'treatment_vector',
                         values = ['elements','n']).cumsum().reset_index()
    d.sort_values(by = 'bucket', ascending = True, inplace = True)
    d.columns = ['_'.join(col).strip() for col in d.columns.values]
    
    #Incremental response (A - B*C).
    #A = positive outcome on variant group 
    #B = conversion rate on control group 
    #C = total elements on control group.
    d['incremental'] = d.elements_variant -(d.elements_control/d.n_control)*d.n_variant
    d['incremental'] = d.incremental.astype(int)
    
    #actual model 
    bukets = np.insert(d.bucket_.values,0,0)
    incremental = np.insert(d.incremental.values,0,0)
    optimal_x = ((
            d.iloc[-1].elements_control+d.iloc[-1].elements_variant)/\
            (d.iloc[-1].n_control+d.iloc[-1].n_variant))*100
    
    if negative_efect == True:
        optimal_y = d.iloc[-1].elements_control+d.iloc[-1].elements_variant
    else:
        optimal_y = incremental[-1]
    
    #Areas under the curves.
    auc_random_model = 100*incremental[-1]*.5
    auc_actual_model = auc(bukets,incremental)
    auc_optimal_model = auc([0,optimal_x,100,100],
                            [0,optimal_y,optimal_y,incremental[-1]])
    
    #Same definition as gini coefficent.
    #(Check the definition on:
    #Uplift Modeling: Identifying Optimal Treatment Group
    #Allocation and Whom to Contact to Maximize Return on Investment)
    qini_coeff = (auc_actual_model - auc_random_model)/(auc_optimal_model - auc_random_model)
    qini_coeff = np.round(qini_coeff,3)
    
    if (ax1 == None ) & (plot):
        fig, (ax1) = plt.subplots(1, sharex=True ,figsize =(12,4))
    if plot:
        ax1.plot(bukets,incremental, marker='*',
                 label = f'actual model - Qini coefficient {qini_coeff}')
        ax1.fill_between(bukets, incremental,[ x*incremental[-1]/100 for x in np.arange(0,110,10)],
                        alpha=0.2)
        ax1.plot([0,100],[0,incremental[-1]], label = 'random model')
        ax1.plot([0,optimal_x,100,100],[0,optimal_y,optimal_y,incremental[-1]],
                 label = f'optimal model - Incrementals: {np.round(optimal_y,2)}')
        ax1.legend()
        ax1.xaxis.set_ticks(np.arange(10, 110, 10))
        plt.ylabel('Incremental events')
        plt.xlabel('Proportion of Treatment Group Targeted (%)')

    
    
    return qini_coeff,d

def EvalIncrementalModels(y_true:list,
                          treatment_vector:list,
                          y_predict:list,
                          plot:bool=True):
    """Wraper function for summarize the performance of a incremental model.
    y_true: actual outcome (for the momentos binary outcome 
    {0 = no event,1 = event})
    treatment_vector: vector indicating treatment group 
    {0 for control, 1 for treatment}
    y_predict: uplift score (.predict output)
    """
    if plot:
        fig, (ax1,ax2,ax3,ax4,ax5) = plt.subplots(5,1, sharex=True ,figsize =(12,12)) 
    else:
        ax1,ax2,ax3,ax4,ax5 = None,None,None,None,None
    
    ##Incremental plot. Lift in the top 10% and Uplift in the top 30%
    lift_conversion_top10, uplift_top30 = conversion_incremental_plot(y_true = y_true,
                                                                      treatment_vector = treatment_vector,
                                                                      y_predict = y_predict,
                                                                      plot = plot,
                                                                      ax1 = ax1,
                                                                      ax2 = ax2)
    ##Gain cumulative. Gini coefficent and % on positive events cumulated at the top 30%
    gini_coef, gain_top30 = gain_plot(y_true = y_true,
                                      treatment_vector = treatment_vector,
                                      y_predict = y_predict,
                                      plot = plot,
                                      ax1 = ax3,
                                      ax2 = ax4)
    ##Qini curve. Qini coefficent.
    qini_coeff,_ = qini_plot(y_true = y_true,
                             treatment_vector = treatment_vector,
                             y_predict = y_predict,
                             plot = plot,
                             ax1 = ax5)
    
    return lift_conversion_top10, uplift_top30, gini_coef, gain_top30, qini_coeff