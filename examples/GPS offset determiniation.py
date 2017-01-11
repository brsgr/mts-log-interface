import mtsloginterface as mts
from mts import mts_query
import pandas as pd
import pickle
import numpy as np

# This script attempts to determine the average Y pos for a particualr row in order to calculate the miscalibration
# present in RTG R8091. It does so by comparing R8091 to R8143, which worked in the same row on 12/28/2016
# data from location is stored in a pickle, save.p. MTS query data may be dated since it purges every 2 weeks.

rtg_query ="""
select a.hist_id,a.che_id,
case
when substring(a.origin,7,1) = 'D' and substring(a.origin,1,4) in ('G204', 'G304', 'G404', 'G504')
then a.origin
when substring(a.destination,7,1) = 'D' and substring(a.destination,1,4) in ('G204', 'G304', 'G404', 'G504')
then a.destination
end as location
,
case
when substring(a.origin,7,1) = 'D' and substring(a.origin,1,4) in ('G204', 'G304', 'G404', 'G504')
then a.event_time_in
when substring(a.destination,7,1) = 'D' and substring(a.destination,1,4) in ('G204', 'G304', 'G404', 'G504')
then a.out
end as event_time

 from(

select a.* from db_apmt.dbo.rtg_fuller_summary  as a
where  (substring(a.origin,7,1) = 'D' or  substring(a.destination,7,1) = 'D')
and (substring(a.origin,1,4) in ('G204', 'G304', 'G404', 'G504') or substring(a.destination,1,4) in ('G204', 'G304', 'G404', 'G504') )
and datepart(day, out) = 28

)A
order by event_time
"""

transitions = mts_query(rtg_query)
transitions['Time Sent'] = transitions['event_time'].apply(lambda x: x.replace(microsecond=0))
transitions['CHE ID'] = transitions['che_id']
print(transitions.head())


gps_loc = pickle.load(open('save.p', 'rb'))
print(gps_loc.head())

merge1 = pd.merge(transitions, gps_loc, on=['Time Sent', 'CHE ID'], how='left')

print(merge1[merge1.che_id == 'R8143'])