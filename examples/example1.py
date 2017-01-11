## This example uses the whereport_ping log search function to try to determine why T6267 failed to transition to a
## truck at 3/29/2016 07:07:37. This outputs all whereport pings for T6267

import datetime
import mtsloginterface as mts

from mts import mts_query

where_query = 'select * from whereports_by_wp'  # Database table storing whereport numbers
whereportlist = mts_query(where_query)
where_dict = {}
for index, i in enumerate(whereportlist):  # Creates dictionary with WPID:(che, side)
    where_dict[i[0]] = i[2:0:-1]

path = 'Z:\\inetpub\\ftproot\\WhereNet\\Server\\Log'
archive = 'Z:\\inetpub\\ftproot\\WhereNet\\Server\\Log\\Archive'
start = datetime.datetime(2016, 3, 29, 7, 7, 1)
end = datetime.datetime(2016, 3, 29, 7, 8, 1)

pings = mts.whereport_pings(path, archive, start, end)
for i in pings.logs:
    try:
        if where_dict[i[3]][0] == 'T6267':
            print(i, where_dict[i[3]][0])
    except KeyError:
        pass