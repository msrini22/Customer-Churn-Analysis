#!/usr/bin/env python
# coding: utf-8

# In[1]:


'''This program aims to identify the customers that most likely will churn 
by performing a clustering analysis based on the interaction and purchasing characteristics of the customers'''
#importing needed libraries
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from matplotlib.colors import ListedColormap


# In[2]:


#loading the information card(consolidated file)
df=pd.read_csv(r'C:\Users\mahat\Documents\rawdata\RawData\TransactionData_consolidated_final.csv')
print(df.shape)
df.describe()


# In[3]:


'''converting 'maxdate' attribute to proper datetime format'''
df_maxdate=df['maxdate']
yr_list=[]
m_list=[]
d_list=[]
i=0
while(i<df.shape[0]):
    my_date=str(df_maxdate[i])
    yr_list.append(my_date[0:4])#first 4 characters denote the year
    m_list.append(my_date[4:6])#next 2 characters denote the month
    d_list.append(my_date[6:])#rest of the characters denote the date
    i+=1
#print(yr_list)
#print(m_list)
#print(d_list)
df_time=pd.DataFrame(columns=['year','month','day'])
df_time['year']=yr_list
df_time['month']=m_list
df_time['day']=d_list
df_time=pd.to_datetime(df_time)
#print(df_time)
df['maxdate']=pd.Series(df_time)
#print(df['maxdate'])


# In[ ]:


'''This part aims to measure and quantify the purchasing characteristic and the worth of each customer
by calculating each one's recency, frequency and total sales contribution
The 3 attributes:recency, frequency and sales are needed to form RFS clusters'''

#to create dataframe for recency,frequency, and sales
df_rfs=pd.DataFrame(columns=['CustomerID','Recency','Frequency','Sales'])
df_rfs['CustomerID']=df['CustomerID'].unique()
recency_list=[]
freq_list=[]
sales_list=[]
most_recent_date=df['maxdate'].max()#most recent date of the entire data
print(most_recent_date)
i=0
while(i<df.shape[0]):
    recency_list.append(most_recent_date-df.iloc[i,2])
    i+=1
df_rfs['Recency']=recency_list
i=0
#j=3
while(i<df.shape[0]):
    j=3
    f_sum=0
    s_sum=0
    while(j<df.shape[1]):
        s_sum+= df.iloc[i,j]
        f_sum+=df.iloc[i,j+1]
        j+=2
    freq_list.append(f_sum) 
    sales_list.append(s_sum)
    i+=1
df_rfs['Frequency']=freq_list
df_rfs['Sales']=sales_list
#print(df_rfs)


# In[ ]:


#getting the number of days (integer part) in the recency attribute
#creating frequency table
print(df_rfs['Recency'].dtype)
df_rfs['Recency']=df_rfs['Recency'].dt.days
print(df_rfs['Recency'])


# In[ ]:


'''This part marks the beginning of cluster analysis using K-Means. Different values of K are tried to obtain the 
best value of k by using the elbow method. The sse (sum of squared error) is plotted for various values of k.
The k at which elbow point is present is considered as the optimised value'''

#kmeans plot for recency
sse={}
df_recency = df_rfs[['Recency']]
for k in range(1, 10):
    kmeans = KMeans(n_clusters=k, max_iter=1000).fit(df_recency)
    df_recency["clusters"] = kmeans.labels_
    sse[k] = kmeans.inertia_ 
plt.figure()
plt.plot(list(sse.keys()), list(sse.values()))
plt.xlabel("Number of clusters")
plt.show()


# In[ ]:


'''This function is used to obtain the best cluster/group and the rankings of the same based on the recency, frequency or
sales scores. In case of recency, the lower the value, the better. In case of frequency and sales, the higher the value,
the better. The dataframe containing the customer id, recency, frequency, sales, recency cluster, frequency cluster, and 
sales cluster is returned
The i/p target_filed_name can be either of the 3:recency, frequency, and sales'''
def order_cluster(cluster_field_name, target_field_name,df,ascending):
    new_cluster_field_name = 'new_' + cluster_field_name
    df_new = df.groupby(cluster_field_name)[target_field_name].mean().reset_index()
    df_new = df_new.sort_values(by=target_field_name,ascending=ascending).reset_index(drop=True)
    df_new['index'] = df_new.index
    df_final = pd.merge(df,df_new[[cluster_field_name,'index']], on=cluster_field_name)
    df_final = df_final.drop([cluster_field_name],axis=1)
    df_final = df_final.rename(columns={"index":cluster_field_name})
    return df_final


# In[ ]:


#K=2 seems appropriate for this task.
#obtaining clusters based on recency values
kmeans = KMeans(n_clusters=2)
kmeans.fit(df_rfs[['Recency']])
df_rfs['RecencyCluster'] = kmeans.predict(df_rfs[['Recency']])
df_rfs = order_cluster('RecencyCluster', 'Recency',df_rfs,False)
df_rfs.groupby('RecencyCluster')['Recency'].describe()

#obtaining clusters based on frequency values
kmeans.fit(df_rfs[['Frequency']])
df_rfs['FrequencyCluster'] = kmeans.predict(df_rfs[['Frequency']])
df_rfs = order_cluster('FrequencyCluster', 'Frequency',df_rfs,True)
df_rfs.groupby('FrequencyCluster')['Frequency'].describe()

#obtaining clusters based on sales values
kmeans.fit(df_rfs[['Sales']])
df_rfs['SalesCluster'] = kmeans.predict(df_rfs[['Sales']])
df_rfs = order_cluster('SalesCluster', 'Sales',df_rfs,True)
df_rfs.groupby('SalesCluster')['Sales'].describe()


# In[ ]:


'''This part computes the total score of each group based on the summation of the 3 fields:r,f and s. The result is then 
grouped by total score and the mean of the 3 attributes are displayed.
For 2 clusters, there are 4 scores. A total Score of 0 and 1 denotes that the customer might churn. Label is 1
A total score of 2 and 3 indicates that the customer will continue to be a patron of the company. Label is 0'''

#computing total scores
df_rfs['TotalScore'] = df_rfs['RecencyCluster'] + df_rfs['FrequencyCluster'] + df_rfs['SalesCluster']
df_rfs.groupby('TotalScore')['Recency','Frequency','Sales'].mean()
churn_list=[]#needed to store list of churn labels
for row in df_rfs['TotalScore']:
    if row>1:
        churn_list.append(0)
    else:
        churn_list.append(1)
df_rfs['Churn']=churn_list
print(df_rfs)


# In[ ]:


#visualizing the two groups(churn vs not churn by plotting recency against frequency. Red denotes not churn while blue denotes churn
x = df_rfs['Recency']
y = df_rfs['Frequency']
classes = ['Churn','Not Churn']
values = df_rfs['Churn']
colours = ListedColormap(['r','b'])
scatter = plt.scatter(x, y,c=values, cmap=colours)
plt.xlabel('Recency')
plt.ylabel('Frequency')
plt.title('Recency vs Frequency')
plt.legend(labels=classes,loc='upper right')
plt.show()


# In[ ]:


#visualizing the two groups(churn vs not churn by plotting recency against sales.
x = df_rfs['Recency']
y = df_rfs['Sales']
classes = ['Churn','Not Churn']
values = df_rfs['Churn']
colours = ListedColormap(['r','b'])
scatter = plt.scatter(x, y,c=values, cmap=colours)
plt.xlabel('Recency')
plt.ylabel('Sales')
plt.title('Recency vs Sales')
plt.legend(labels=classes,loc='upper right')
plt.show()


# In[ ]:


#visualizing the two groups(churn vs not churn by plotting sales against frequency.
x = df_rfs['Sales']
y = df_rfs['Frequency']
classes = ['Churn','Not Churn']
values = df_rfs['Churn']
colours = ListedColormap(['r','b'])
scatter = plt.scatter(x, y,c=values, cmap=colours)
plt.xlabel('Sales')
plt.ylabel('Frequency')
plt.title('Sales vs Frequency')
plt.legend(labels=classes,loc='upper right')
plt.show()


# In[ ]:


#saving customer ID and predicted churn label results
df_rfs.to_csv(r'C:\Users\mahat\Documents\rawdata\RawData\TransactionData_rfs_churn_results.csv', index=False)

