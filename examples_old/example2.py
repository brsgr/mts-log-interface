## This script checks to see all RFID tag pings by truck number 163998

import datetime
import mtsloginterface as mts

path = 'Z:\\inetpub\\ftproot\\WhereNet\\Server\\Log'
archive = 'Z:\\inetpub\\ftproot\\WhereNet\\Server\\Log\\Archive'
start = datetime.datetime(2016, 3, 29, 6, 45, 1)
end = datetime.datetime(2016, 3, 29, 7, 0, 1)

a = mts.rfid_tag_locations(path, archive, start, end, filters=['T6344'])
for i in a.logs:
    print(i)