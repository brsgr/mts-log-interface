## This example searches for whereport pings against truck numbered 163998 betwen 7:30 and 8:00 on 03292016

import datetime
import mtsloginterface as mts

path = 'Z:\\inetpub\\ftproot\\WhereNet\\Server\\Log'
archive = 'Z:\\inetpub\\ftproot\\WhereNet\\Server\\Log\\Archive'
start = datetime.datetime(2016, 3, 29, 6, 20, 1)
end = datetime.datetime(2016, 3, 29, 6, 50, 1)

pings = mts.whereport_pings(path, archive, start, end, filters=['163998'])
for i in pings.logs:
    print(i)