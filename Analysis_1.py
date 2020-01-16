#!/usr/bin/env python
# coding: utf-8

# In[1]:


#importing the needed modules
import pandas as pd
import numpy as np


# In[3]:


'''Reading the Transaction csv file in chunks due to th large size of the file
Appending all chunks to get the entire dataframe '''

df_iterator=pd.read_csv(r'C:\Users\mahat\Documents\rawdata\RawData\TransactionData.csv',chunksize=10000,encoding="ISO-8859-1")
chunk_list=[]
for df_chunk in df_iterator:
    df_chunk=df_chunk.drop(df_chunk.columns[0],axis=1)
    #df_chunk=sort_year_month(df_chunk)
    chunk_list.append(df_chunk)
new_df=pd.concat(chunk_list)
#print(new_df)


# In[4]:


'''Obtaining data attributes'unique values such as channel types, customer segment types, #unique customers, months and years '''
print(new_df['Channel'].unique())
print(new_df['Customer Segment'].unique())
print(new_df['CustomerID'].unique())
print(new_df['Month'].unique())
print(new_df['Year'].unique())


# In[ ]:


'''Sorting the dataframe based on the order date and storing the sorted dataframe in a new csv file'''
new_df=new_df.sort_values(by=['Order Date'])
#print(new_df)
new_df.to_csv(r'C:\Users\mahat\Documents\rawdata\RawData\TransactionData_sorted.csv',index=False)

