from pandas.core.common import array_equivalent
import pandas as pd
import time as t
import re
import pickle
import datetime
import numpy as np
import pandas_datareader.data as web

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

def stockpricedata(tickerlist):
    end_date_list=[]
    start_date_list=[]
    for year in range(0,11):
        end_date_list.append(datetime.date(2015-year,12,31))
        start_date_list.append(datetime.date(2015-year,12,31-6))
    dates_list = [start_date_list,end_date_list]
    #
    #haloj=web.DataReader(['AAK.ST','ABB.ST'],'yahoo','2015-12-25','2015-12-31')
    #print(haloj)
    #
    #adjCloseMeanDF = pd.DataFrame(columns=haloj.columns)
    #adjCloseMeanDF

    #stock_price_tickerlist=['AAK.ST','ABB.ST','^GDAXI']

    for dt in range(0,len(dates_list[0])):

        haloj=web.DataReader(stock_price_tickerlist,'yahoo',dates_list[0][dt],dates_list[1][dt])['Adj Close']        
        if dt==0: 
            adjCloseMeanDF = pd.DataFrame(index=dates_list[1],columns=haloj.columns)    
        
        for k in haloj.columns:
            
            adjCloseMeanDF.set_value(dates_list[1][dt],k,np.mean(haloj[k]))

    adjCloseMeanDF.sort_index(inplace=True)
    adjCloseChange=adjCloseMeanDF.pct_change()
    adjCloseChange2=pd.DataFrame(index=adjCloseChange.index,columns=adjCloseChange.columns)
    for k in adjCloseChange.columns:
                
                adjCloseChange2[k]=np.where(adjCloseChange[k]>adjCloseChange['^GDAXI']+0.05,1,0)



    #testdf['Year']=testdf.index.to_date().year


    testdf=adjCloseChange2.stack().to_frame()
    #testdf.index.get_level_values(0).tolist()
    testdf2=adjCloseMeanDF.stack().to_frame()

    testdf2.rename(columns={0:'eoy_price'},inplace=True)

    testdf.rename(columns={0:'12mWinner'},inplace=True)

    testdf3=testdf.join(testdf2)

    #testdf['datetime']=np.asarray(testdf.index.get_level_values(0).tolist())

    #testdf['year']=testdf[testdf.index.get_level_values(0)].map(lambda x: x.year)
    #testdf['year']=pd.DatetimeIndex(testdf[])
    #pd.DatetimeIndex(testdf)

    testdf3.reset_index(inplace=True)

    testdf3['Year']=testdf3['level_0'].apply(lambda x: x.year)
    testdf3['Month']=testdf3['level_0'].apply(lambda x: x.month)
    testdf3['Day']=testdf3['level_0'].apply(lambda x: x.day)
    testdf3['Ticker']=testdf3['level_1']
    testdf3.set_index(['Year','Month','Day','Ticker'],inplace=True)
    del testdf3['level_1'],testdf3['level_0']
    testdf3.reset_index(inplace=True)


    asddf=pd.DataFrame(textData)


    asddf['Ticker']=stock_price_tickerlist[:len(stock_price_tickerlist)-1]
    asddf['Osloticker']=tickerlist


    testdf4=pd.merge(testdf3,asddf,how='left',on=['Ticker'])
    if oslo:
        testdf4.rename(columns={'Ticker':'yTicker','Osloticker':'Ticker',0 : 'Comp_name'},inplace=True)
    else:
        testdf4.rename(columns={'Ticker':'yTicker',3:'Ticker',0 : 'Comp_name'},inplace=True)



    testdf4=testdf4[['Year','Month','Day','Ticker','12mWinner','eoy_price','Comp_name']]
    testdf4=testdf4[pd.notnull(testdf4['Ticker'])]
    return testdf4