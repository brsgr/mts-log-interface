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