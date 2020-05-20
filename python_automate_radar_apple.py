import dateutil.relativedelta
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



b=curr_date-dateutil.relativedelta.relativedelta(months=6)
b_string = b.strftime("%y-%m-%d")
b_string='20'+b_string
str(b_string)
components_analyze={ "component": { "name": "AMP Data Ops", "version": "MR" }}

result_radar=radar_client.find_radars(components_analyze, additional_fields=['state','resolution','title','lastModifiedAt','createdAt','assignee','event','priority','assigneeLastModifiedAt','resolvedBy','dri'], limit=None, progress_callback=None, return_find_results_directly=True)
df_analyze=create_df(result_radar,"Analyze") # our create_df function required arg


df_last6months=df_analyze[(df_analyze['Createdate'] > b_string)]
df_f=df_last6months[['Event','Createdate']]



df_f['month'] = pd.DatetimeIndex(df_f['Createdate']).month
df_f[['Event','Createdate']].groupby(['Event']).agg('count')

df_final=df_f[['Event','month']].groupby(['month']).agg('count')





df_final.plot.bar()

