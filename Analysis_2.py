#!/usr/bin/env python
# coding: utf-8

# In[1]:


'''This program aims to establish an information card for the original Transaction Data'''
#importing needed libraries
import pandas as pd
import numpy as np
import dask.dataframe as dd
import matplotlib.pyplot as plt


# In[2]:


#Defining a function to get the total number of unique orders and summation of sales by year
def get_total_ord_sales_by_year(df):
    df_total_ord_sales=df.groupby(['CustomerID','Month'])['Sales'].agg([sum,'count']).reset_index()
    df_total_ord_sales.columns=['CustomerID','Month','Total sales','Total #orders']
    #df13_total_ord_sales['Total #orders']=df13.grouby
    return (df_total_ord_sales)


# In[ ]:


'''Defining a function that fills the dataframe of the information card with data pertaining to 2013
Note that the columns of the information card dataframe ie Sales and orders monthwise starting 2013 begin at column number 3
The pointer to the last updated row is returned for future data loading operations'''

def fill_main_df_13(yrdf,ind):
    #Obtaining list of unique customers
    custid_unq=list(yrdf['CustomerID'].unique())
    #ind=0
    for cu in custid_unq:
        df_cust=yrdf[yrdf['CustomerID']==cu]
        j=0#index for customer df
        main_df.iloc[ind,0]=cu
        #Filling the details based on the month(coded 1-12)
        if df_cust.iloc[j,1]==1:
            main_df.iloc[ind,3]=df_cust.iloc[j,2]
            main_df.iloc[ind,4]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==2:
            main_df.iloc[ind,5]=df_cust.iloc[j,2]
            main_df.iloc[ind,6]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==3:
            main_df.iloc[ind,7]=df_cust.iloc[j,2]
            main_df.iloc[ind,8]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==4:
            main_df.iloc[ind,9]=df_cust.iloc[j,2]
            main_df.iloc[ind,10]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==5:
            main_df.iloc[ind,11]=df_cust.iloc[j,2]
            main_df.iloc[ind,12]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==6:
            main_df.iloc[ind,13]=df_cust.iloc[j,2]
            main_df.iloc[ind,14]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==7:
            main_df.iloc[ind,15]=df_cust.iloc[j,2]
            main_df.iloc[ind,16]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==8:
            main_df.iloc[ind,17]=df_cust.iloc[j,2]
            main_df.iloc[ind,18]=df_cust.iloc[j,3]  
        elif df_cust.iloc[j,1]==9:
            main_df.iloc[ind,19]=df_cust.iloc[j,2]
            main_df.iloc[ind,20]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==10:
            main_df.iloc[ind,21]=df_cust.iloc[j,2]
            main_df.iloc[ind,22]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==11:
            main_df.iloc[ind,23]=df_cust.iloc[j,2]
            main_df.iloc[ind,24]=df_cust.iloc[j,3]
        else:
            main_df.iloc[ind,25]=df_cust.iloc[j,2]
            main_df.iloc[ind,26]=df_cust.iloc[j,3] 
        j+=1
        ind+=1 
    return ind


# In[ ]:


'''Defining a function that fills the dataframe of the information card with data pertaining to 2014
Note that the columns of the information card dataframe ie Sales and orders monthwise starting 2014 begin at column number 27
The pointer to the last updated row is returned for future data loading operations'''

def fill_main_df_14(yrdf,ind):
    custid_unq=list(yrdf['CustomerID'].unique())
    #ind=0
    #save=ind
    for cu in custid_unq:
        mcu_list=list(main_df['CustomerID'].unique())#getting list of existing customers
        save=ind
        #Setting a flag variable to check if a customer already exists in information card
        flag=False
        df_cust=yrdf[yrdf['CustomerID']==cu]
        j=0#index for customer df
        #if the current customer already exists in the dataframe of the information card
        if cu in mcu_list:
            #print(main_df.shape)
            #save=main_df[main_df['CustomerID']==cu].index[0]
            save=mcu_list.index(cu)
            flag=True
            #print(save,flag)
            #main_df.iloc[ind,0]=cu
        else:
            main_df.iloc[save,0]=cu
        if df_cust.iloc[j,1]==1:
            main_df.iloc[save,27]=df_cust.iloc[j,2]
            main_df.iloc[save,28]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==2:
            main_df.iloc[save,29]=df_cust.iloc[j,2]
            main_df.iloc[save,30]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==3:
            main_df.iloc[save,31]=df_cust.iloc[j,2]
            main_df.iloc[save,32]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==4:
            main_df.iloc[save,33]=df_cust.iloc[j,2]
            main_df.iloc[save,34]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==5:
            main_df.iloc[save,35]=df_cust.iloc[j,2]
            main_df.iloc[save,36]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==6:
            main_df.iloc[save,37]=df_cust.iloc[j,2]
            main_df.iloc[save,38]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==7:
            main_df.iloc[save,39]=df_cust.iloc[j,2]
            main_df.iloc[save,40]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==8:
            main_df.iloc[save,41]=df_cust.iloc[j,2]
            main_df.iloc[save,42]=df_cust.iloc[j,3]  
        elif df_cust.iloc[j,1]==9:
            main_df.iloc[save,43]=df_cust.iloc[j,2]
            main_df.iloc[save,44]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==10:
            main_df.iloc[save,45]=df_cust.iloc[j,2]
            main_df.iloc[save,46]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==11:
            main_df.iloc[save,47]=df_cust.iloc[j,2]
            main_df.iloc[save,48]=df_cust.iloc[j,3]
        else:
            main_df.iloc[save,49]=df_cust.iloc[j,2]
            main_df.iloc[save,50]=df_cust.iloc[j,3] 
        j+=1
        #print(ind)
        if (flag!=True):
            ind+=1
    return ind


# In[3]:


'''Defining a function that fills the dataframe of the information card with data pertaining to 2015
Note that the columns of the information card dataframe ie Sales and orders monthwise starting 2015 begin at column number 51
The pointer to the last updated row is returned for future data loading operations'''

def fill_main_df_15(yrdf,ind):
    custid_unq=list(yrdf['CustomerID'].unique())
    #ind=0
    #save=ind
    for cu in custid_unq:
        mcu_list=list(main_df['CustomerID'].unique())#getting list of existing customers
        save=ind
        #Setting a flag variable to check if a customer already exists in information card
        flag=False
        df_cust=yrdf[yrdf['CustomerID']==cu]
        j=0#index for customer df
        #if the current customer already exists in the dataframe of the information card
        if cu in mcu_list:
            #print(main_df.shape)
            #save=main_df[main_df['CustomerID']==cu].index[0]
            save=mcu_list.index(cu)
            flag=True
            #print(save,flag)
            #main_df.iloc[ind,0]=cu
        else:
            main_df.iloc[save,0]=cu
        if df_cust.iloc[j,1]==1:
            main_df.iloc[save,51]=df_cust.iloc[j,2]
            main_df.iloc[save,52]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==2:
            main_df.iloc[save,53]=df_cust.iloc[j,2]
            main_df.iloc[save,54]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==3:
            main_df.iloc[save,55]=df_cust.iloc[j,2]
            main_df.iloc[save,56]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==4:
            main_df.iloc[save,57]=df_cust.iloc[j,2]
            main_df.iloc[save,58]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==5:
            main_df.iloc[save,59]=df_cust.iloc[j,2]
            main_df.iloc[save,60]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==6:
            main_df.iloc[save,61]=df_cust.iloc[j,2]
            main_df.iloc[save,62]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==7:
            main_df.iloc[save,63]=df_cust.iloc[j,2]
            main_df.iloc[save,64]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==8:
            main_df.iloc[save,65]=df_cust.iloc[j,2]
            main_df.iloc[save,66]=df_cust.iloc[j,3]  
        elif df_cust.iloc[j,1]==9:
            main_df.iloc[save,67]=df_cust.iloc[j,2]
            main_df.iloc[save,68]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==10:
            main_df.iloc[save,69]=df_cust.iloc[j,2]
            main_df.iloc[save,70]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==11:
            main_df.iloc[save,71]=df_cust.iloc[j,2]
            main_df.iloc[save,72]=df_cust.iloc[j,3]
        else:
            main_df.iloc[save,73]=df_cust.iloc[j,2]
            main_df.iloc[save,74]=df_cust.iloc[j,3] 
        j+=1
        #print(ind)
        if (flag!=True):
            ind+=1
    return ind


# In[4]:


'''Defining a function that fills the dataframe of the information card with data pertaining to 2016
Note that the columns of the information card dataframe ie Sales and orders monthwise starting 2016 begin at column number 75
The pointer to the last updated row is returned for future data loading operations'''

def fill_main_df_16(yrdf,ind):
    custid_unq=list(yrdf['CustomerID'].unique())
    #ind=0
    #save=ind
    for cu in custid_unq:
        mcu_list=list(main_df['CustomerID'].unique())#getting list of existing customers
        save=ind
        #Setting a flag variable to check if a customer already exists in information card
        flag=False
        df_cust=yrdf[yrdf['CustomerID']==cu]
        j=0#index for customer df
        #if the current customer already exists in the dataframe of the information card
        if cu in mcu_list:
            #print(main_df.shape)
            #save=main_df[main_df['CustomerID']==cu].index[0]
            save=mcu_list.index(cu)
            flag=True
            #print(save,flag)
            #main_df.iloc[ind,0]=cu
        else:
            main_df.iloc[save,0]=cu
        if df_cust.iloc[j,1]==1:
            main_df.iloc[save,75]=df_cust.iloc[j,2]
            main_df.iloc[save,76]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==2:
            main_df.iloc[save,77]=df_cust.iloc[j,2]
            main_df.iloc[save,78]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==3:
            main_df.iloc[save,79]=df_cust.iloc[j,2]
            main_df.iloc[save,80]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==4:
            main_df.iloc[save,81]=df_cust.iloc[j,2]
            main_df.iloc[save,82]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==5:
            main_df.iloc[save,83]=df_cust.iloc[j,2]
            main_df.iloc[save,84]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==6:
            main_df.iloc[save,85]=df_cust.iloc[j,2]
            main_df.iloc[save,86]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==7:
            main_df.iloc[save,87]=df_cust.iloc[j,2]
            main_df.iloc[save,88]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==8:
            main_df.iloc[save,89]=df_cust.iloc[j,2]
            main_df.iloc[save,90]=df_cust.iloc[j,3]  
        elif df_cust.iloc[j,1]==9:
            main_df.iloc[save,91]=df_cust.iloc[j,2]
            main_df.iloc[save,92]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==10:
            main_df.iloc[save,93]=df_cust.iloc[j,2]
            main_df.iloc[save,94]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==11:
            main_df.iloc[save,95]=df_cust.iloc[j,2]
            main_df.iloc[save,96]=df_cust.iloc[j,3]
        else:
            main_df.iloc[save,97]=df_cust.iloc[j,2]
            main_df.iloc[save,98]=df_cust.iloc[j,3] 
        j+=1
        #print(ind)
        if (flag!=True):
            ind+=1
    return ind


# In[5]:


'''Defining a function that fills the dataframe of the information card with data pertaining to 2017
Note that the columns of the information card dataframe ie Sales and orders monthwise starting 2017 begin at column number 99
Also, Only details for the first 5 months(Jan-May) of 2017 are only given in the original Transcation Data
The pointer to the last updated row is returned for future data loading operations'''

def fill_main_df_17(yrdf,ind):
    custid_unq=list(yrdf['CustomerID'].unique())
    #ind=0
    #save=ind
    for cu in custid_unq:
        mcu_list=list(main_df['CustomerID'].unique())#getting list of existing customers
        save=ind
        #Setting a flag variable to check if a customer already exists in information card
        flag=False
        df_cust=yrdf[yrdf['CustomerID']==cu]
        j=0#index for customer df
        # if current customer already exists in dataframe of information card
        if cu in mcu_list:
            #print(main_df.shape)
            #save=main_df[main_df['CustomerID']==cu].index[0]
            save=mcu_list.index(cu)
            flag=True
            #print(save,flag)
            #main_df.iloc[ind,0]=cu
        else:
            main_df.iloc[save,0]=cu
        if df_cust.iloc[j,1]==1:
            main_df.iloc[save,99]=df_cust.iloc[j,2]
            main_df.iloc[save,100]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==2:
            main_df.iloc[save,101]=df_cust.iloc[j,2]
            main_df.iloc[save,102]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==3:
            main_df.iloc[save,103]=df_cust.iloc[j,2]
            main_df.iloc[save,104]=df_cust.iloc[j,3]
        elif df_cust.iloc[j,1]==4:
            main_df.iloc[save,105]=df_cust.iloc[j,2]
            main_df.iloc[save,106]=df_cust.iloc[j,3]
        else:
            main_df.iloc[save,107]=df_cust.iloc[j,2]
            main_df.iloc[save,108]=df_cust.iloc[j,3]
        j+=1
        #print(ind)
        if (flag!=True):
            ind+=1
    return ind


# In[8]:


'''Creating a set of columns in a desired format(sales,orders) of unique customers monthwise for the duration 2013-2017
Then appending the Customer ID, the startdate and most recent date of each customer to create desired attribute set for the
information card'''

yr_list=[13,14,15,16,17]
month_list=['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec']
main_cols=[]
attr_cols=['sales','order']
for yr in yr_list:
    for month in month_list:
        for a in attr_cols:
            name=month+'_'+str(yr)+'_'+a
            main_cols.append(name)
print(len(main_cols))  
#deleting cols from jun 17 since there is only data for first 5 months of 2017
main_cols=main_cols[:106]
print(main_cols)
cols_final=['CustomerID','mindate','maxdate']
cols_final.extend(main_cols)
print(cols_final)


# In[9]:


'''Creating an empty dataframe that represents the information card for future analysis'''
#creating a main dataframe with newly_defined attribute set and 60000 rows
main_df=pd.DataFrame(columns=cols_final, index=list(range(60000)))
main_df=main_df.fillna(0)#filling all entries with 0


# In[ ]:


#opening the sorted Transcation Data file in chunks and concatenating all chunks to get the entire dataframe
df_iterator=pd.read_csv(r'C:\Users\mahat\Documents\rawdata\RawData\TransactionData_sorted.csv',chunksize=10000,encoding="ISO-8859-1")
chunk_list=[]
#years_list=[2013,2014,2015,2016,2017]
for df_chunk in df_iterator:
    chunk_list.append(df_chunk)
new_df=pd.concat(chunk_list)
#ensuring if all columns don't have NaN values
print(new_df.isnull().sum())    


# In[ ]:


#considering data year wise
df13=new_df[new_df['Year']==2013]
df14=new_df[new_df['Year']==2014]
df15=new_df[new_df['Year']==2015]
df16=new_df[new_df['Year']==2016]
df17=new_df[new_df['Year']==2017]


# In[ ]:


'''Invoking get_total_ord_sales_year() for the year-based dataframes to get slaes and orders dataframes year-wise '''
df13_total_ord_sales=get_total_ord_sales_by_year(df13)
#print(df13_total_ord_sales)
df14_total_ord_sales=get_total_ord_sales_by_year(df14)
df15_total_ord_sales=get_total_ord_sales_by_year(df15)
df16_total_ord_sales=get_total_ord_sales_by_year(df16)
df17_total_ord_sales=get_total_ord_sales_by_year(df17)


# In[ ]:


'''Time to fill in the information card ie main_df year-wise'''
indx=fill_main_df_13(df13_total_ord_sales,0)
#print(indx)
indx=fill_main_df_14(df14_total_ord_sales,indx)
#print(indx)
indx=fill_main_df_15(df15_total_ord_sales,indx)
#print(indx)
indx=fill_main_df_16(df16_total_ord_sales,indx)
#print(indx)
indx=fill_main_df_17(df17_total_ord_sales,indx)
#print(indx)


# In[ ]:


'''The information card contains about 51234 unique customers for the duration 2013-17
Removing unnecessary rows after customer #51234'''
#print(main_df.iloc[51233,0:3])
main_df=main_df.iloc[:51234,:]
print(main_df.shape)


# In[ ]:


#to fill in min time(start date of the customer) and max time(most recent date of customer)
cust_id=main_df['CustomerID'].unique()
mintimes=[]
maxtimes=[]
count=0
for cid in cust_id:
    dum_df=new_df[new_df['CustomerID']==cid]
    mintimes.append(dum_df['Order Date'].min())
    maxtimes.append(dum_df['Order Date'].max())
    count+=1
    #print(count)
main_df['mindate']=mintimes
main_df['maxdate']=maxtimes


# In[ ]:


#Saving the updated Information Card
main_df.to_csv(r'C:\Users\mahat\Documents\rawdata\RawData\TransactionData_consolidated_final.csv',index=False)

