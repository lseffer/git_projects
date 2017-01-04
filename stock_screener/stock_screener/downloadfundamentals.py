from pandas.core.common import array_equivalent
import pandas as pd
import time as t
import re
import pickle

import stock_screener.good_morning as gm
from stock_screener.helperfunctions import *

def keyratiodata(tickerlist):

    kr = gm.KeyRatiosDownloader()

    df=pd.DataFrame()
    i = 0
    for each_stock in tickerlist:
        t.sleep(0.5)
        try:
            kr_frames = kr.download(each_stock)
        except:
            pass
        for k in range(0,len(kr_frames)):
            if k==0:
                temp_df=kr_frames[k].transpose()
                temp_df.insert(0,'Ticker',each_stock)
                temp_df.insert(1,'Year',temp_df.index.to_timestamp().year)
                temp_df.insert(2,'Month',12)
                temp_df.insert(3,'Day',31)
    #            test=pd.merge(temp_df,temp_df,on=['Year','Month','Day','Ticker'],how='left')
                temp_df.columns=temp_df.columns.str.replace(" ","")            
                result=temp_df
                
            else:
                temp_df=kr_frames[k].transpose()
                temp_df.insert(0,'Ticker',each_stock)
                temp_df.insert(1,'Year',temp_df.index.to_timestamp().year)
                temp_df.insert(2,'Month',12)
                temp_df.insert(3,'Day',31)
                temp_df.columns=temp_df.columns.str.replace(" ","")
                result=pd.merge(result,temp_df,on=['Year','Month','Day','Ticker'],how='left')
                
        result.set_index(['Year','Month','Day','Ticker'],inplace=True)

        try:

            if i!=0:
                
                df.columns=result.columns
                df=df.append(result,ignore_index=False)
            else:
                df=df.append(result,ignore_index=False)
            print(each_stock)
            i+=1
        except Exception as e:
            print(str(e),'<-- problem')
            i+=1
    return df           

def financialdata(tickerlist):

    fd=gm.FinancialsDownloader()
    finData=pd.DataFrame()
    k = 0
    for each_stock in tickerlist:
        t.sleep(0.5)
        
        try:
            fd_dict=fd.download(each_stock)
    #        
    #        test=fd_dict.get('balance_sheet')
    #        test=test.append(fd_dict.get('cash_flow'),ignore_index=False)
    #        test=test.append(fd_dict.get('income_statement'),ignore_index=False)
    #        test.insert(1,'Ticker',each_stock)
    #        test=test.transpose()
    #        test.reset_index(inplace=True)
            StockCurrency=fd_dict.get('currency')
            
            isdf_1=fd_dict.get('income_statement')   
            isdf_2=rename_IS(isdf_1,'Basic')
            isdf_3=rename_IS(isdf_2,'Diluted')
            
            bsdf=restructure_df(fd_dict.get('balance_sheet'),StockCurrency,each_stock)
            cfdf=restructure_df(fd_dict.get('cash_flow'),StockCurrency,each_stock)
            isdf=restructure_df(isdf_3,StockCurrency,each_stock)
            
            res=pd.merge(bsdf,cfdf,how='left',on=['Year','Month','Day','Ticker','Currency'])
            res=pd.merge(res,isdf,how='left',on=['Year','Month','Day','Ticker','Currency'])

    #        res.set_index(['Year','Month','Day','Ticker'],inplace=True)
            
            
        except Exception as e:
            print(str(e),'stock combination failed')        
            pass
            
        try:
            if k!=0:
    #            finData.columns=res.columns
                rmvdups=res.T.drop_duplicates().T
                dupslist=duplicate_columns(rmvdups)
                res_final=rmvdups.drop(dupslist, axis=1)
                
                finData=pd.concat([finData,res_final],axis=0,ignore_index=True)
    #            finData=pd.merge(finData,res.T.drop_duplicates().T,how='outer')
    #            finData=finData.append(res,ignore_index=False)
            else:
                rmvdups=res.T.drop_duplicates().T
                dupslist=duplicate_columns(rmvdups)
                res_final=rmvdups.drop(dupslist, axis=1)
                finData=finData.append(res_final,ignore_index=True)

            print(each_stock)
            k+=1
        except Exception as e:
            print(str(e),'<-- problem')
            k+=1
            
        fd_dict.clear

    return finData      
