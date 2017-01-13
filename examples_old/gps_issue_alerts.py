import datetime
import logging
import mtsloginterface as mts
import re
import time

# This script executes every hour to determine if equipment has defective GPS components. It compares transition events
# to transition events in which no GPS data is sent. The results are logged to a file in the script's root directory


while True:

    start_time = datetime.datetime.now()  #  Sets start time as datetime data type

    # First utilize the mtsloginterface library to quickly find the releveant log files
    path = 'Z:\\inetpub\\ftproot\\WhereNet\\Server\\Log'
    archive = 'Z:\\inetpub\\ftproot\\WhereNet\\Server\\Log\\Archive'
    type = 'MTSTelemISvcLog'  # GPS and lift data can be taken from
    end = start_time
    start = start_time - datetime.timedelta(hours=1)  # The start and end look at the past hour
    connection = mts.MTSLogConnection(path, archive, type, start, end)
    file_list = connection.return_logs()
    logs_list = connection.download_logs(filters=[''])  # Quickly download log files from relevant type and date into a list

    bad_gps_counter = {}  # Initialize dictionary for counting transitions with no GSP Data
    transition_counter = []  # Initialize list for counting all transitions

# The first loop identifies container transitions among all telemetry events

    for i in logs_list:
        if ('msg=FELEvent' in i or 'msg=RTGEvent' in i) and 'Pwr' not in i:  # Looks only for RTG or FEL Transitions
            matchA = re.search(r'\w*/\w*/\w\w\w\w \w\w:\w\w:\w\w \w\w', i)  # Regex to find datetime
            matchB = re.search(r'[RMT]\d\d\d\d', i)  # Regex to fine CHEID
            if 'Lift' in i:  # If statement to determine Lift or Set
                matchC = 'Lift'
            elif 'Set' in i:
                matchC = 'Set'
            matchD = re.search(r't~~+.+~\|', i)  # Regex to find
            if matchA and matchB and matchC and matchD:
                    transition_counter.append([matchA.group(), matchB.group(), matchC, matchD.group()])  # combines A-D to populate transition counter list
            #  With this we can look through the transitions for those with NULL GPS data

    for i in transition_counter:  # loops through all transitions found by the last loop
        if i[1] not in bad_gps_counter.keys():  # If there is no pre-existing key, create one
            bad_gps_counter[i[1]] = [0, 0]
        if i[3] != 't~~~~~~~~~~~~0~~~~~~|' and i[3] != 't~~~~~~~~~~~0~~~NA~|':  # If the GPS data for a transtition IS NOT bad, increase the first element in the list
                bad_gps_counter[i[1]][0] += 1
        bad_gps_counter[i[1]][1] += 1  # Add one to the second element, which simply counts transitions

    for i in bad_gps_counter:
        print(i, bad_gps_counter[i])  # Print out the keys and dictionary

    for i in bad_gps_counter:
        if bad_gps_counter[i][0] == 0 and bad_gps_counter[i][1] > 2:  # If the machine has 0 moves with a GPS location
            broken_GPS = True
            print('%s is not getting location on transitions    %s' % (i, str(start_time)))  # Prints che, GPS info, and time
            logging.basicConfig(filename="badgpslogging.tlog", format='%(asctime)s %(message)s')  #logging configuration
            logging.warning('%s is not getting location on transitions' % i)  # Logs broken GPS info

    end_time = datetime.datetime.now()
    print('\nElapsed time: %s seconds' % (end_time - start_time).seconds)  # Outputs execution time

    time.sleep(600)
