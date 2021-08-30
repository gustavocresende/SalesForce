# -*- coding: utf-8 -*-
"""
Created on Mon Aug 23 12:11:23 2021

@author: gvegn
"""



from simple_salesforce import Salesforce
import requests
import pandas as pd
import csv
from io import StringIO
from datetime import datetime
import numpy as np


sf = Salesforce(username='gustavo.ciravegna@saltpay.co', 
password='*********',
security_token='fvUFvCb5oPP9oRvX1f7w00Dgy')
sf_org = 'https://saltpay-co.lightning.force.com/'
export_params = '?isdtp=p1&export=1&enc=UTF-8&xf=csv'

#Accounts
report_id = '00O4L0000022lyxUAA'
sf_report_url = sf_org + report_id + export_params
response = requests.get(sf_report_url, headers=sf.headers, cookies={'sid': sf.session_id})
new_report = response.content.decode('utf-8')
df_accounts = pd.read_csv(StringIO(new_report))

report_id = '00O8e000000VIXCEA4'
sf_report_url = sf_org + report_id + export_params
response = requests.get(sf_report_url, headers=sf.headers, cookies={'sid': sf.session_id})
new_report = response.content.decode('utf-8')
df_accounts_outlets = pd.read_csv(StringIO(new_report))


#Add Industries to the Accounts that dont have industries
##Get the Industry that shows the most for each parent account
df_accounts_outlets = df_accounts_outlets[df_accounts_outlets['Parent Account ID'].isnull()==False]
df_accounts_outlets = df_accounts_outlets.groupby(['Parent Account ID','Industry ID']).agg(NI=('Industry ID','count'))
df_accounts_outlets = df_accounts_outlets.reset_index()
uniqueID = np.unique(df_accounts_outlets['Parent Account ID'])
app = pd.DataFrame(columns = df_accounts_outlets.columns )

for idi in uniqueID:
    df = df_accounts_outlets[df_accounts_outlets['Parent Account ID']==idi]
    df = df.loc[[df['NI'].idxmax()]]
    app = app.append(df)
app = app.reset_index(drop=True)


df_accounts_outlets = app
df_accounts_outlets= df_accounts_outlets.drop(['NI'],axis=1)
df_accounts_outlets = df_accounts_outlets.rename(columns={'Parent Account ID':'Account ID'})    


##Merge the accounts and outlets and fill the blank customers
df_accounts = df_accounts.merge(df_accounts_outlets, how="left", on="Account ID")
df_accounts['Industry ID_y']= df_accounts['Industry ID_y'].fillna(0)
df_accounts['Industry ID_x']= df_accounts['Industry ID_x'].fillna(0)

for i in range(0,df_accounts.shape[0]):
    if df_accounts['Industry ID_x'][i] ==0 and df_accounts['Industry ID_y'][i] !=0 :
        df_accounts['Industry ID_x'][i] = df_accounts['Industry ID_y'][i]
    
df_accounts = df_accounts.drop(['Industry ID_y','Account ID'],axis=1)
df_accounts = df_accounts.rename(columns={'Industry ID_x':'Industry ID'})    

#Activities
report_id = '00O4L0000022s1tUAA'
sf_report_url = sf_org + report_id + export_params
response = requests.get(sf_report_url, headers=sf.headers, cookies={'sid': sf.session_id})
new_report = response.content.decode('utf-8')
df_activities = pd.read_csv(StringIO(new_report))
df_activities['Start'] =  pd.to_datetime(df_activities['Start'], format="%d/%m/%Y")
df_activities = df_activities[df_activities['Start']<=datetime.today().strftime("%d/%m/%Y")]

#Opportunities
report_id = '00O4L0000022pz5UAA'
sf_report_url = sf_org + report_id + export_params
response = requests.get(sf_report_url, headers=sf.headers, cookies={'sid': sf.session_id})
new_report = response.content.decode('utf-8')
df_opportunities = pd.read_csv(StringIO(new_report))



# Get the lattest opportunity
app = pd.DataFrame(columns=df_opportunities.columns)
idi = np.unique(df_opportunities['AccountID18'])
df_opportunities['Created Date'] = pd.to_datetime(df_opportunities['Created Date'], format='%d/%m/%Y')
for i in idi:
    df = df_opportunities[df_opportunities['AccountID18']==i]
    df = df.reset_index(drop=True)
    if len(df['AccountID18'])>1:
        df = df.sort_values('Created Date',ascending=False)
        df = df[df['Created Date']==df['Created Date'][0]]
        
        for j in range(0,len(df['Created Date'])):
            if df['Stage'][j] == "Closed Won" or df['Stage'][j] == "Closed Lost":
                df = df.loc[[j]]
                df = df.reset_index(drop=True)
                break
        df = df.loc[[0]]
        app = app.append(df)
        
    else:
        app = app.append(df)
    
df_opportunities = app

#Meetings

report_id = '00O8e000000VHRXEA4'
sf_report_url = sf_org + report_id + export_params
report_results = sf.restful('analytics/reports/{}'.format(report_id))
response = requests.get(sf_report_url, headers=sf.headers, cookies={'sid': sf.session_id})
new_report = response.content.decode('utf-8')
df_meetings = pd.read_csv(StringIO(new_report))
df_meetings = df_meetings[df_meetings['Created Date'] == df_meetings['Referred Date']]
df_meetings = df_meetings.drop(['Referred Date'], axis=1)


#Get the latest meeting
app = pd.DataFrame(columns=df_meetings.columns)
idi = np.unique(df_meetings['AccountID18'])
df_meetings['Created Date'] =  pd.to_datetime(df_meetings['Created Date'], format='%d/%m/%Y')

for i in idi:
    df = df_meetings[df_meetings['AccountID18']==i]
    df = df.reset_index(drop=True)
    df = df.sort_values('Created Date',ascending=False)
    df = df.loc[[0]]
    app = app.append(df)

df_meetings = app
        
    
#Get only activities associated to the accounts
df_activities = df_activities.merge(df_accounts['AccountID18'],how="inner",on="AccountID18")


#Get only activities that occur before the account is closed
index1 = df_opportunities['Stage']=="Closed Lost"
index2 = df_opportunities['Stage']=="Closed Won"
index = index1 + index2

df_opp2 = df_opportunities[index]
x = df_opp2['Last Stage Change Date'][0]
def clean(x):
    x = x[0:10]
    return x
    
df_opp2['Last Stage Change Date']= df_opp2['Last Stage Change Date'].apply(clean)
df_opp2['Last Stage Change Date'] = pd.to_datetime(df_opp2['Last Stage Change Date'],format='%d/%m/%Y')

df_activities= df_activities.merge(df_opp2[['AccountID18','Last Stage Change Date']],how="left",on="AccountID18")
df_activities['check']= df_activities['Start'] > df_activities['Last Stage Change Date']
df_activities_afterclosure =df_activities[df_activities['check'] == True]



#Export
df_activities = df_activities[df_activities['check'] == False]
df_accounts.to_excel(r'C:\Users\gvegn\OneDrive\Desktop\Documents\11. Data Francisco\Controlo\Data\Accounts.xlsx')
df_activities.to_excel(r'C:\Users\gvegn\OneDrive\Desktop\Documents\11. Data Francisco\Controlo\Data\Activities.xlsx')
df_opportunities.to_excel(r'C:\Users\gvegn\OneDrive\Desktop\Documents\11. Data Francisco\Controlo\Data\Opportunities.xlsx')
df_meetings.to_excel(r'C:\Users\gvegn\OneDrive\Desktop\Documents\11. Data Francisco\Controlo\Data\Meetings.xlsx')
df_activities_afterclosure.to_excel(r'C:\Users\gvegn\OneDrive\Desktop\Documents\11. Data Francisco\Controlo\Data\ActivitiesAfterClosure.xlsx')












