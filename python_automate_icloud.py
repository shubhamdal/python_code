# Automate_icloud


import os
import sys
import radarclient
from radarclient.radartool import AbstractSubcommand, RadarToolCommandLineDriver
from radarclient import RadarClient, AuthenticationStrategySPNego, Person, ClientSystemIdentifier
from datetime import datetime
import pandas as pd
from datetime import datetime   
from datetime import timedelta
import getpass


system_identifier = ClientSystemIdentifier('MyApp1', '1.0')
radar_client = RadarClient(AuthenticationStrategySPNego(), system_identifier)
curr_date=datetime.date(datetime.now()) 
username = getpass.getuser()


def create_df(result_radar,state):
    i=0
    listID=[]
    listTicketType=[]
    listTitle=[]
    listCreatedate=[]
    listEnddate=[]
    listPriority=[]
    listAssignee=[]
    listEvent=[]
    listPlatform=[]
    listTypeOfRequest=[]
    listComplexity=[]
    listRemarks=[]
    listDRI=[]
    listResolvedBy=[]
    for radar in result_radar:
    #     print( str(radar.id)+" "+str(radar.title)+" "+str(radar.createdAt.date())+" "+str(radar.assignee)+" "+str(radar.event) )
        listID.append('<rdar://problem/'+str(radar.id)+'> '+str(radar.title))
        listTicketType.append('MR')
        listTitle.append(str(radar.title))
        listCreatedate.append(str(radar.createdAt.date()))
        if state=='analyze':
            listEnddate.append('')
        else:
            listEnddate.append((radar.lastModifiedAt.date()))
        listPriority.append(radar.priority)
        listEvent.append(str(str(radar.event)[38:-1])) # business user group
        listPlatform.append('TD')
        listTypeOfRequest.append('Ad-hoc')
        listComplexity.append('Medium')
        listRemarks.append('')
        if 'None' in str(radar.dri):
            listDRI.append('')
            dri=''
        else:
            listDRI.append(str(radar.dri)[19:-1])
            dri=str(radar.dri)[19:-1]
        if 'None' in str(radar.resolvedBy):
            listResolvedBy.append('')
            resolvedBy=''
        else:
            listResolvedBy.append(str(radar.resolvedBy)[19:-1])
            resolvedBy=str(radar.resolvedBy)[19:-1]
        if (dri != '') and (resolvedBy != ''):
            listAssignee.append(dri+","+resolvedBy)
        else:
            listAssignee.append(dri+resolvedBy)
        i=i+1
    df  = pd.DataFrame()
    df['ID']=listID
    df['TicketType']=listTicketType
    df['Title']=listTitle
    df['Createdate']=listCreatedate
    df['Enddate']=listEnddate
    df['Priority']=listPriority
    df['Assignee']=listAssignee
    df['Event']=listEvent
    df['Platform']=listPlatform
    df['TypeOfRequest']=listTypeOfRequest
    df['Complexity']=listComplexity
    df['Remarks']=listRemarks
#     df['DRI']=listDRI
#     df['resolvedBy']=listResolvedBy>
    df=df.sort_values(['ID'], ascending=[True])
    return df
 


# Analyze state
components_analyze={ "component": { "name": "AMP Data Ops", "version": "MR" }, "state": "Analyze" }
result_radar=radar_client.find_radars(components_analyze, additional_fields=['state','resolution','title','lastModifiedAt','createdAt','assignee','event','priority','assigneeLastModifiedAt','resolvedBy','dri'], limit=None, batch_size=50, progress_callback=None, return_find_results_directly=True)
df_analyze=create_df(result_radar,'analyze')
df_analyze.to_excel("/Users/"+username+"/documents/radarAPI/radar_analyze.xlsx",index=False, header=True) 

#  state can be verify as well as closed - need to check from monday to friday
 
# Analyze verify-closed
components_verify={ "component": { "name": "AMP Data Ops", "version": "MR" }, "state":[ "Verify"] }
result_radar=radar_client.find_radars(components_verify, additional_fields=['state','resolution','title','lastModifiedAt','createdAt','assignee','event','priority','assigneeLastModifiedAt','resolvedBy','dri'], limit=None, batch_size=50, progress_callback=None, return_find_results_directly=True)
df_verify=create_df(result_radar,'Verify')
previous_week_monday_date= ( (curr_date - timedelta(days=curr_date.weekday()))  - pd.Timedelta(weeks=1))
previous_week_friday_date=(curr_date - (timedelta(days=curr_date.weekday()))-pd.Timedelta(days=2))
# df_verify=df_verify[  (df_verify['Enddate'] >= previous_week_monday_date)  ]
df_verify=df_verify[  (df_verify['Enddate'] >= previous_week_friday_date)  ]
df_verify.to_excel("/Users/"+username+"/documents/radarAPI/radar_verify.xlsx",index=False, header=True) 



