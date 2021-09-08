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

#Activities
report_id = '00O4L0000022s1tUAA'
sf_report_url = sf_org + report_id + export_params
response = requests.get(sf_report_url, headers=sf.headers, cookies={'sid': sf.session_id})
new_report = response.content.decode('utf-8')
df_activities = pd.read_csv(StringIO(new_report))
df_activities['Start'] =  pd.to_datetime(df_activities['Start'], format="%d/%m/%Y")
df_activities = df_activities[df_activities['Start']<=datetime.today()]

#Opportunities
report_id = '00O8e000000VK50EAG'
sf_report_url = sf_org + report_id + export_params
response = requests.get(sf_report_url, headers=sf.headers, cookies={'sid': sf.session_id})
new_report = response.content.decode('utf-8')
df_opportunities = pd.read_csv(StringIO(new_report))



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
df_meetings['Date'] =  pd.to_datetime(df_meetings['Date'], format='%d/%m/%Y')
for i in idi:
    df = df_meetings[df_meetings['AccountID18']==i]
    df = df.sort_values('Date',ascending=False)
    df = df.reset_index(drop=True)
    df = df.loc[[0]]
    app = app.append(df)

df_meetings = app
        


#Get only activities/meetings after 26/07/2021
df_activities = df_activities[df_activities['Start']>=datetime(2021, 7, 26)]
df_meetings=df_meetings[df_meetings['Created Date']>=datetime(2021, 7, 26)]

#Export

df_accounts.to_excel(r'C:\Users\gvegn\OneDrive\Desktop\Documents\11. Data Francisco\Controlo\Data\Accounts.xlsx')
df_activities.to_excel(r'C:\Users\gvegn\OneDrive\Desktop\Documents\11. Data Francisco\Controlo\Data\Activities.xlsx')
df_opportunities.to_excel(r'C:\Users\gvegn\OneDrive\Desktop\Documents\11. Data Francisco\Controlo\Data\Opportunities.xlsx')
df_meetings.to_excel(r'C:\Users\gvegn\OneDrive\Desktop\Documents\11. Data Francisco\Controlo\Data\Meetings.xlsx')












