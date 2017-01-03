from pandas.core.common import array_equivalent
import pandas as pd
import pickle
import os

def duplicate_columns(frame):
    groups = frame.columns.to_series().groupby(frame.dtypes).groups
    dups = []

    for u, v in groups.items():

        cs = frame[v].columns
        vs = frame[v]
        lcs = len(cs)

        for i in range(lcs):
            ia = vs.iloc[:,i].values
            for j in range(i+1, lcs):
                ja = vs.iloc[:,j].values
                if array_equivalent(ia, ja):
                    dups.append(cs[i])
                    break

    return dups

def rename_IS(df,name):
    
    df_not_in=df[~df['title'].isin([name])]
    df_in=df[df['title'].isin([name])]

    
    for k in df_in.index:
        if k==df_in.index[0]:
            df_in.set_value(k,'title',name+'_eps')
        else:
            df_in.set_value(k,'title',name+'_nos')
    
    return df_not_in.append(df_in,ignore_index=False)
    

def restructure_df(df,curris,stock):
    temp=df
    try:
        del temp['parent_index']   
    except:
        pass
    test2=temp.transpose()
    test3=test2.iloc[0].to_frame()
    listtest2=test3['title'].tolist()
    
    test2.columns=listtest2
    test2.reset_index(inplace=True)
    test2.drop(test2.index[[0]],inplace=True)
    test2['Year']=test2['index']
    test2['Month']=12
    test2['Day']=31
    del test2['index']
    test2.reset_index(inplace=True)
    del test2['index']
    test2.columns=test2.columns.str.replace(" ","") 
    test2['Ticker']=stock
    test2['Currency']=curris
    return test2

def saveobject(inputobject,name,folder):
    pickle_out=open(os.path.join(folder,name),'wb')
    pickle.dump(inputobject, pickle_out)
    pickle_out.close()