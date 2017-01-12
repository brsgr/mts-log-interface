import pandas as pd
import numpy as np
import re


def print_full(x):
    pd.set_option('display.max_rows', len(x))
    print(x)
    pd.reset_option('display.max_rows')


# Functions for extracting relevant information ising regex
def return_tag_no(i):
    match = re.search(r'3\d\d\d\d\d\d\d', i)  # regex to find tag #
    try:
        return match.group()
    except AttributeError:
        return np.NAN

def return_tag_id(i):
    match = re.search(r'ResourceID=.+\|', i)  # regex to find tag id
    try:  # Avoid Attribute error if no ResourceID is found
        C = match.group()[11:-1]
    except AttributeError:
        C = 'DF'
    return C


def return_wp_id(i):
    match = re.search(r'WherePortID=\d\d\d\d', i)  # regex to find whereport id
    try:
        return match.group()[12:]
    except AttributeError:
        return np.NAN

def return_zone_name(i):
    match = re.search(r'ZoneName.+?\|', i)  # Regex to find Zone name
    try:
        return match.group()[9:-1]
    except AttributeError:
        return np.NAN

def return_x_coord(i):
    match = re.search(r'X.+?\|', i)  # regex to find X coord
    try:  # Avoid Value error if regex does not return an integer
        position = int(match.group()[2:-1])
    except ValueError:
        position = match.group()[2:-1]
    except AttributeError:
        return np.NAN
    return position


def return_y_coord(i):
    match = re.search(r'Y.+?\|', i)  # regex to find Y coord
    try:  # Avoid Value error if regex does not return an integer
        position = int(match.group()[2:-1])
    except ValueError:
        position = match.group()[2:-1]
    except AttributeError:
        return np.NAN
    return position


def return_che_id_gps(i):
    match = re.search(r'~\w\d\d\d\d~', i)  # regex for che_id
    try:
        return match.group()[1:-1]
    except AttributeError:
        return np.NAN


def return_x_coord_gps(i):
    """
    Extracts X coordinates for RTGs, FELs, and UTRs. Conditonals are requireds since they are stored differently
    Note that UTR positioning is in lat/long whereas the other CHE are in x/y
    """
    if 'LocationEvent' in i:  # if it is RTG/FEL
        match = re.search(r'~~~\d*\.\d*', i)  # regex to find X coord
        try:  # Avoid attribute error if regex finds nothing
            position = float(match.group()[3:])
        except AttributeError:
            return np.NAN
        return position
    elif 'TTEvent' in i:  # If it is UTR
        match = re.search(r'Locate~\d*\.\d*', i)  # regex to find X coord
        try:  # Avoid attribute error if regex finds nothing
            position = float(match.group()[7:])
        except AttributeError:
            return np.NAN
        return position
    else:
        return np.NAN


def return_y_coord_gps(i):
    """
    Extracts Y coordinates for RTGs, FELs, and UTRs. Conditonals are requireds since they are stored differently
    Note that UTR positioning is in lat/long whereas the other CHE are in x/y
    """
    if 'LocationEvent' in i:
        match = re.search(r'0~\d*\.\d*', i) # regex to find Y coord
        try:  # Avoid attribute error if regex finds nothing
            position = float(match.group()[2:])
        except AttributeError:
            return np.NAN
        return position
    elif 'TTEvent' in i:
        match = re.search(r'~-\d*\.\d*', i)  # regex to find X coord
        try:  # Avoid attribute error if regex finds nothing
            position = float(match.group()[1:])
        except AttributeError:
            return np.NAN
        return position
    else:
        return np.NAN


def return_head_gps(i):
    match = re.search(r'\d*\.?\d*~N', i)  # regex for head direction
    try:  # Avoid attribute error if regex finds nothing
        position = float(match.group()[:-2])
    except AttributeError:
        return np.NAN
    except ValueError:
        return np.NAN
    return position