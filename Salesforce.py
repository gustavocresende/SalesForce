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
password='5PShec5TX13',
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

df_accounts_NoIndustry = df_accounts[df_accounts['Industry ID'].isnull()==True]
df_accounts_NoIndustry  = df_accounts_NoIndustry.merge(df_accounts_outlets[['Parent Account ID','Industry ID']],how="left",left_on="Account ID",right_on="Parent Account ID")
df_accounts_NoIndustry = df_accounts_NoIndustry.drop(['Parent Account ID','Industry ID_x'],axis=1)
df_accounts_NoIndustry  = df_accounts_NoIndustry.rename(columns={'Industry ID_y': 'Industry ID'})
df_accounts_NoIndustry = df_accounts_NoIndustry.groupby(['AccountID18','Account Name','Industry ID']).agg(NIndustries = ('Industry ID','count'))
df_accounts_NoIndustry = df_accounts_NoIndustry.reset_index()

n = len(df_accounts_NoIndustry.columns)-1
df_accounts_NoIndustry_2 = pd.DataFrame(columns=df_accounts_NoIndustry.columns[0:n])


uniqueID = np.unique(df_accounts_NoIndustry['AccountID18'])

for idi in uniqueID:
    df = df_accounts_NoIndustry[df_accounts_NoIndustry['AccountID18']==idi]
    df = df.loc[[df['NIndustries'].idxmax()],['AccountID18','Account Name','Industry ID']]
    df_accounts_NoIndustry_2= df_accounts_NoIndustry_2.append(df)
    

df_accounts = df_accounts.merge(df_accounts_NoIndustry_2, how="left", on="AccountID18")


df_accounts['Industry ID_y']= df_accounts['Industry ID_y'].fillna(0)
for i in range(0,df_accounts.shape[0]):
    if df_accounts['Industry ID_y'][i] !=0:
        df_accounts['Industry ID_x'][i] = df_accounts['Industry ID_y'][i]
    
df_accounts = df_accounts.drop(['Industry ID_y','Account ID','Account Name_y'],axis=1)
df_accounts = df_accounts.rename(columns={'Account Name_x':'Account Name','Industry ID_x':'Industry ID'})    
        
   
    


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
app = pd.DataFrame(columns=df_opportunities.columns)


# Get the lattest opportunity
idi = np.unique(df_opportunities['AccountID18'])
for i in idi:
    df = df_opportunities[df_opportunities['AccountID18']==i]
    df = df.reset_index(drop=True)
    if len(df['AccountID18'])>1:
        df['Created Date'] = pd.to_datetime(df['Created Date'], format='%d/%m/%Y')
        
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
app = pd.DataFrame(columns=df_meetings.columns)

#Get the latest meeting
idi = np.unique(df_meetings['AccountID18'])
for i in idi:
    df = df_meetings[df_meetings['AccountID18']==i]
    df = df.reset_index(drop=True)
    if len(df['AccountID18'])>1:
        df['Created Date'] = pd.to_datetime(df['Created Date'], format='%d/%m/%Y')
        
        df = df.sort_values('Created Date',ascending=False)
        df = df.loc[[0]]
        app = app.append(df)
    else:
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









