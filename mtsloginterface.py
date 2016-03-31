import gzip
import re
from os import listdir
import os.path
import datetime
import csv


class MTSLogConnection(object):  # Class for establishing connection to log server
    def __init__(self, path, archive_path, type, datestart, dateend):
        # For APM, point path to \\10.11.29.31\\inetpub\\ftproot\\wherenet\\server\\log
        self.path = path
        self.type = type
        self.datestart = datestart
        self.dateend = dateend
        self.archive_path = archive_path

    def return_logs(self):
        # Returns list of relevant log files (including path). It must combine a list generated from the archives
        # directory with a list from the path directory

        # code to derive list of archive directories
        archive_directs = [j for j in (self.archive_path + '\\' + i for i in listdir(self.archive_path)
                                       if os.path.isdir(self.archive_path + '\\' + i))]
        logfilelist = []  # Iniitalize list of logfiles
        logfilelist_filtered = []  # Initialize list of filtered log files, this will be returned by the functiion
        for direct in archive_directs:  # Loop through archive directories (labeled by MM_DD_YYYY)
            if DateStringContain(self.datestart, direct) or DateStringContain(self.dateend, direct):  # filter by date
                for file in listdir(direct):
                    logfilelist.append(direct + '\\' + file)  # Add possible directories to list

        logfilelist += [self.path + '\\' + i for i in listdir(self.path)]  # combine logs and archives

        for file in logfilelist:  # loop through list of all log files to filter out the uneeded ones
            log_file_type = re.search(r'\w*\.', file)  # regex to check log file type
            try:
                if log_file_type.group()[:-1] in self.type and str(self.datestart.year) in file:  # Filters by log_type
                    b = re.search(r'\d\d\d\d\d\d\d\d\d\d\d\d\d\d-', file).group()[:-1]  # regex to find log file start date
                    c = re.search(r'\d\d\d\d\d\d\d\d\d\d\d\d\d\d\.', file).group()[:-1]  # regex to find log file end date

                    # Convert these to datetime data type
                    file_start_time = datetime.datetime(int(b[0:4]), int(b[4:6]), int(b[6:8]), int(b[8:10]), int(b[10:12]),
                                                        int(b[12:14]))

                    file_end_time = datetime.datetime(int(c[0:4]), int(c[4:6]), int(c[6:8]), int(c[8:10]), int(c[10:12]),
                                                      int(c[12:14]))
                    if (file_end_time > self.datestart) & (self.dateend > file_start_time):  # filters log files by the date range
                        logfilelist_filtered.append(file)  # Adds the filter file paths to a list
            except AttributeError:
                pass
        return logfilelist_filtered

    def download_logs(self, filters):
        # Uses the return_logs method to actually break down the log files into a list
        # The filters argument is a list that acts as a simple set of string filters for each log lin
        logfilelist = self.return_logs()
        logitemlist = []
        for file in logfilelist:
            a = gzip.open(file)
            for i in a:
                if any(j in i.decode('Latin-1') for j in filters):  # Checks logs for any input paramaters
                    logitemlist.append(i.decode('Latin-1'))  # Populates list of all log files
        return logitemlist


class MTSLog(object):  # Class for parsed log outputs list; differs in that it includes column names attribute
    def __init__(self, logs, columns):
        self.logs = logs
        self.columns = columns

    def csv_export(self, file):  # export to csv method
        with open(file, 'w', newline='') as myfile:
            wr = csv.writer(myfile)
            wr.writerows([self.columns] + self.logs)



def DateStringContain(date, string):
    # Determines if a string contains the month and day elements of a datetime This is used for filtering directories
    # 'date' is in the datetime format and string is a .... string
    return str(date.month) in string and str(date.day) in string

# The next series of functions utilize the MTSLogConnection class to
# quickly parse the logs for commonly sought out data

def whereport_pings(path, archive, start, end, filters=['']):
    # Create MTSLog of all whereport pings for a given start and end time
    # List format is [datetime, tagid, whereport_name, whereport_id, ZoneName, [X_pos, YPos)

    logitemlist = MTSLogConnection(path, archive, 'FrontLoaderSvcLog', start, end).download_logs(filters)
    # Establish MTS Connection and download the logs, note that the download_logs method includes the optional filters
    # argument
    pings = []  # Initialize
    for i in logitemlist:
        if re.search(r'WherePortID=\w\w\w\w', i):  # regex conditional searches for a blink with a whereport ID in the log
            matchA = re.search(r'\w*/\w*/\w\w\w\w \w\w:\w\w:\w\w \w\w', i)  # Regex to find datetime
            matchB = re.search(r'3\d\d\d\d\d\d\d', i)  # regex to find tag #
            matchC = re.search(r'ResourceID=.+\|', i)  # regex to find tag id
            matchD = re.search(r'WherePortID=\d\d\d\d', i)  # regex to find whereport id
            matchE = re.search(r'ZoneName.+?\|', i)  # Regex to find Zone name
            matchF = re.search(r'X.+?\|', i)  # regex to find X coord
            matchG = re.search(r'Y.+?\|', i)  # regex to find Y coord
            # Use matchF and matchG to create integer coordinates
            try:  # Avoid Value error if regex does not return an integer
                positionx = int(matchF.group()[2:-1])
                positiony = int(matchG.group()[2:-1])
            except ValueError:
                positionx = matchF.group()[2:-1]
                positiony = matchG.group()[2:-1]
            try:  # Avoid Attribute error if no ResourceID is found
                C = matchC.group()[11:-1]
            except AttributeError:
                C = 'DF'
            if matchA and matchB and matchD:  # Create list of regex matches, this is the functions result
                pings.append([datetime.datetime.strptime(matchA.group(), '%m/%d/%Y %I:%M:%S %p'), matchB.group(),
                             C, matchD.group()[12:], matchE.group()[9:-1], positionx, positiony])
    column_names = ['Time', 'TagID', 'ResourceID', 'WhereportID', 'ZoneName', 'XPos', 'YPos']
    return MTSLog(pings, column_names)

def gps_location_events(path, archive, start, end, filters=['']):
    # Create a list of all location events at path/archive between the times 'start' and 'end'
    # output is in list[list] with format [datetime received, datetime sent, latency, CHE_id, [X_pos, Y_pos, compass]]
    logitemlist = MTSLogConnection(path, archive, 'MTSTelemISvcLog', start, end).download_logs(filters)
    # Establish MTS Connection and download the logs, note that the download_logs method includes the optional filters
    # argument
    locationlist = []
    for i in logitemlist:
        if 'msg=LocationEvent' in i:
            matchA = re.search(r'\w*/\w*/\w\w\w\w \w\w:\w\w:\w\w \w\w', i)  # Regex to find time received
            matchB = re.search(r't~\d\d\d\d\d\d\d\d\d\d', i)  # regex to find timestamp
            A_Time = datetime.datetime.strptime(matchA.group(), '%m/%d/%Y %I:%M:%S %p')
            B_Time = datetime.datetime.fromtimestamp(int(matchB.group()[2:]))
            latency = (A_Time-B_Time).seconds  # Calculates latency in seconds
            matchC = re.search(r'~\w\d\d\d\d~', i)  # regex for che_id
            matchD = re.search(r'~~~\d*\.\d*', i)  # regex for X_pos
            matchE = re.search(r'0~\d*\.\d*', i)  # regex for Y_pos
            matchF = re.search(r'\d*~N', i)  # regex for head direction

            locationlist.append([A_Time, B_Time, latency, matchC.group()[1:-1], float(matchD.group()[3:]),
                                 float(matchE.group()[2:]), int(matchF.group()[:-2])])
    column_names = ['Time Received', 'Time Sent', 'Latency', 'CHE ID', 'Xpos', 'Ypos', 'Compass']
    return MTSLog(locationlist, column_names)


def rfid_tag_locations(path, archive, start, end, filters=['']):
    # Creates a list of all RFID tag blinks. This includes DRAYMAN, FEL, RTGS, and UTR. Don't confuse this with location
    # events which only includes GPS positions, not RFID positions
    logitemlist = MTSLogConnection(path, archive, 'MTSTelemISvcLog', start, end).download_logs(filters)
    # Establish MTS Connection and download the logs, note that the download_logs method includes the optional filters
    # argument
    blink_list = []  # Initialize list of RFID blinks
    for i in logitemlist:
        if 'Algorithm=3' in i:
            matchA = re.search(r'\w*/\w*/\w\w\w\w \w\w:\w\w:\w\w \w\w', i)  # Regex to find time received
            A_Time = datetime.datetime.strptime(matchA.group(), '%m/%d/%Y %I:%M:%S %p')
            matchB = re.search(r'TagID=\d*', i)  # regex to find tag #
            matchC = re.search(r'ResourceType=.+?\|', i)
            matchD = re.search(r'ResourceID=.+?[-|]', i)
            matchE = re.search(r'ZoneName.+?\|', i)  # Regex to find Zone name
            matchF = re.search(r'X.+?\|', i)  # regex to find X coord
            matchG = re.search(r'Y.+?\|', i)  # regex to find Y coord
            #  Use MatchF and MatchG to create integer coordinates
            try:
                positionx = int(matchF.group()[2:-1])
                positiony = int(matchG.group()[2:-1])
            except ValueError:
                positionx = matchF.group()[2:-1]
                positiony = matchG.group()[2:-1]

            blink_list.append([A_Time, matchB.group()[6:], matchC.group()[13:-1], matchD.group()[11:-1]
                               , matchE.group()[9:-1], positionx, positiony])
    column_names = ['Time', 'TagID', 'Tag_Type', 'ResourceID', 'ZoneName', 'Xpos', 'Ypos']
    return MTSLog(blink_list, column_names)


def in_lane_messages(path, archive, start, end, filters=['']):
    # Creates a list of all scoring (RTG and FEL) over time range. Note that the CHEids are stored as sourceIDs
    # from resource manager. You'll need to set up a dictionary to display them if they differ
    logitemlist = MTSLogConnection(path, archive, 'TTSGateSvcLog', start, end).download_logs(filters)
    scorelist = []
    return MTSLog(logitemlist, '')



if __name__ == '__main__':
    path = 'Z:\\inetpub\\ftproot\\WhereNet\\Server\\Log'
    archive = 'Z:\\inetpub\\ftproot\\WhereNet\\Server\\Log\\Archive'
    type = 'MTSTelemISvcLog'
    start = datetime.datetime(2016, 3, 30, 6, 55, 1)
    end = datetime.datetime(2016, 3, 30, 7, 0, 1)

    a = in_lane_messages(path, archive, start, end)
    for i in a.logs:
        if 'equipmentinLane' in i:
            print(i)







