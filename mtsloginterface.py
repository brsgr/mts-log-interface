import gzip
import re
from os import listdir
import os.path
import datetime
import csv
import pandas as pd
import numpy as np

from whereport_functions import *


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
    def __init__(self, logs, columns, file_names):
        self.logs = logs
        self.columns = columns
        self.file_names = file_names

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

def whereport_pings(path, archive, start, end):
    # Create df of all whereport pings for a given start and end time

    file_names = MTSLogConnection(path, archive, 'FrontLoaderSvcLog', start, end).return_logs()
    # Establish MTS Connection to receive log list
    # initialize empty data frame and loop through the found log files
    return_df = pd.DataFrame()
    for log_file in file_names:
        df = pd.read_csv(log_file, compression='gzip', encoding="Latin-1", header=None)
        df.columns = ['date', 'unixtime', 'blink', 'tag', 'info']

        df['tag_no'] = df['info'].apply(return_tag_no)
        df['tag_id'] = df['info'].apply(return_tag_id)
        df['wp_id'] = df['info'].apply(return_wp_id)
        df['zone_name'] = df['info'].apply(return_zone_name)
        df['x_pos'] = df['info'].apply(return_x_coord)
        df['y_pos'] = df['info'].apply(return_y_coord)

        df = df[df['wp_id'].notnull()]
        df.drop('info', 1, inplace=True)
        df.reset_index(drop=True, inplace=True)

        return_df = return_df.append(df)

    return return_df


def gps_location_events(path, archive, start, end, heading=False):
    # Create a df of all GPS location events for all CHE
    file_names = MTSLogConnection(path, archive, 'MTSTelemISvcLog', start, end).return_logs()
    # Establish MTS Connection and download the logs
    # initialize empty data frame and loop through the found log files
    return_df = pd.DataFrame()
    for log_file in file_names:
        df = pd.read_csv(log_file, compression='gzip', encoding="Latin-1", header=None, error_bad_lines=False)
        df.columns = ['date', 'unixtime', 'blink', 'tag', 'info']
        df = df[df['blink'] == 'Receive']

        df['che_id'] = df['info'].apply(return_che_id_gps)
        df['x_pos'] = df['info'].apply(return_x_coord_gps)
        df['y_pos'] = df['info'].apply(return_y_coord_gps)
        if heading is True:  # Heading is an optional column due to computation time
            df['heading'] = df['info'].apply(return_head_gps)

        df.drop('info', 1, inplace=True)
        df.reset_index(drop=True, inplace=True)

        return_df = return_df.append(df)

    return return_df


def rfid_tag_locations(path, archive, start, end):
    # Create a df of all GPS location events for all CHE
    file_names = MTSLogConnection(path, archive, 'MTSTelemISvcLog', start, end).return_logs()
    # Establish MTS Connection and download the logs
    # initialize empty data frame and loop through the found log files
    return_df = pd.DataFrame()
    for log_file in file_names:
        df = pd.read_csv(log_file, compression='gzip', encoding="Latin-1", header=None, error_bad_lines=False)
        df.columns = ['date', 'unixtime', 'blink', 'tag', 'info']
        df = df[df['info'].str.contains('Algorithm=3|Algorithm=2|Algorithm=1')]

        df['tag_id'] = df['info'].apply(return_tag_no_rfid)
        df['che_id'] = df['info'].apply(return_che_id_rfid)
        df['resource_type'] = df['info'].apply(return_resource_type_rfid)
        df['zone_name'] = df['info'].apply(return_zone_name_rfid)
        df['x_pos'] = df['info'].apply(return_x_coord_rfid)
        df['y_pos'] = df['info'].apply(return_y_coord_rfid)

        df.drop('info', 1, inplace=True)
        df.reset_index(drop=True, inplace=True)
        return_df = return_df.append(df)

    return return_df



if __name__ == '__main__':
    path = 'Z:\\inetpub\\ftproot\\WhereNet\\Server\\Log'
    archive = 'Z:\\inetpub\\ftproot\\WhereNet\\Server\\Log\\Archive'
    type = 'MTSTelemISvcLog'
    start = datetime.datetime(2017, 1, 13, 10, 14, 59)
    end = datetime.datetime(2017, 1, 13, 10, 20, 59)

    import time
    start_time = time.localtime()
    start_time = datetime.datetime(*start_time[:6])
    pd.set_option('max_columns', 50)
    desired_width = 320
    pd.set_option('display.width', desired_width)


    a = rfid_tag_locations(path, archive, start, end)
    print_full(a)

    end_time = time.localtime()
    end_time = datetime.datetime(*end_time[:6])
    print('\nElapsed time: %s seconds' % (end_time - start_time).seconds)  # Outputs execution time






