## This script checks to see all RFID tag pings by tag number 33986660 between 0613 and 0814 on 03292016

import mtsloginterface as mts
import datetime

path = 'Z:\\inetpub\\ftproot\\WhereNet\\Server\\Log'
archive = 'Z:\\inetpub\\ftproot\\WhereNet\\Server\\Log\\Archive'
start = datetime.datetime(2016, 3, 29, 7, 13, 1)
end = datetime.datetime(2016, 3, 29, 8, 15, 1)

a = mts.rfid_tag_locations(path, archive, start, end, filters=['33986660'])
for i in a.logs:
    print(i)