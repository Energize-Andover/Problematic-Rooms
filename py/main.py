# Copyright 2018 Building Energy Gateway.  All rights reserved.

import time
import pandas as pd
import building_data_requests
import numbers
import datetime
import schedule
import csv

def getAir(output):
    temps = ""
    start_time = time.time()

    # Read spreadsheet into a dataframe.
    # Each row contains the following:
    #   - Label
    #   - Facility
    #   - Instance ID of CO2 sensor
    #   - Instance ID of temperature sensor
    df = pd.read_csv('../csv/ahs_air_Ignore.csv', na_filter=False, comment='#')
    # Iterate over the rows of the dataframe, getting temperature and CO2 values for each location
    for index, row in df.iterrows():
        # Retrieve data
        if row['Ignore']!='YES':
            temp_value, temp_units = building_data_requests.get_value(row['Facility'], row['Temperature'])
            co2_value, co2_units = building_data_requests.get_value(row['Facility'], row['CO2'])

        # Prepare to print
        temp_value = int(temp_value) if isinstance(temp_value, numbers.Number) else ''
        temp_units = temp_units if temp_units else ''
        co2_value = int(co2_value) if isinstance(co2_value, numbers.Number) else ''
        co2_units = co2_units if co2_units else ''

        # Output CSV format
        if output.lower() == "all":
            if row['Ignore'] != 'YES':
                temps += ('{0},{1},{2}'.format(row['Label'], temp_value , co2_value)) + '\n'
        elif output.lower() == "co2":
            if row['Ignore'] != 'YES':
                temps += (co2_value) + ','
        elif output.lower() == "temp":
            if row['Ignore'] != 'YES':
                temps += (temp_value) + ','
        elif output.lower() == "location":
            if row['Ignore'] != 'YES':
                temps += (row['Label']) + ','
        else:
            if row['Ignore'] != 'YES':
                temps += ('{0},{1},{2},{3},{4}'.format(row['Label'], temp_value, temp_units, co2_value, co2_units)) + '\n'


    # Report elapsed time
    elapsed_time = round((time.time() - start_time) * 1000) / 1000


"""
    getBulkAir("")
    getBulkAir("co2")
    getBulkAir("location")
    getBulkAir("temp")
"""
def getBulkAir(output):
    temps = ""
    start_time = time.time()

    # Read spreadsheet into a dataframe.
    # Each row contains the following:
    #   - Label
    #   - Facility
    #   - Instance ID of CO2 sensor
    #   - Instance ID of temperature sensor
    df = pd.read_csv('../csv/ahs_air_Ignore.csv', na_filter=False, comment='#')

    # Initialize empty bulk request
    bulk_rq = []

    # Iterate over the rows of the dataframe, adding elements to the bulk request
    for index, row in df.iterrows():

        # Append facility/instance pairs to bulk request
        if row['Ignore'] != 'YES':
            if row['Temperature']:
                bulk_rq.append({'facility': row['Facility'], 'instance': row['Temperature']})
            if row['CO2']:
                bulk_rq.append({'facility': row['Facility'], 'instance': row['CO2']})

    # Issue get-bulk request
    bulk_rsp = building_data_requests.get_bulk(bulk_rq)

    # Extract map from get-bulk response
    map = bulk_rsp['rsp_map']

    # Iterate over the rows of the dataframe, displaying temperature and CO2 values extracted from map
    for index, row in df.iterrows():

        # Initialize empty display values
        temp_value = ''
        temp_units = ''
        co2_value = ''
        co2_units = ''

        # Get facility of current row
        facility = row['Facility']

        # Try to extract current row's temperature and CO2 values from map
        if facility in map:

            instance = str(row['Temperature'])
            if instance and (instance in map[facility]):
                rsp = map[facility][instance]
                property = rsp['property']
                temp_value = int(rsp[property]) if isinstance(rsp[property], numbers.Number) else ''
                temp_units = rsp['units']

            instance = str(row['CO2'])
            if instance and (instance in map[facility]):
                rsp = map[facility][instance]
                property = rsp['property']
                co2_value = int(rsp[property]) if isinstance(rsp[property], numbers.Number) else ''
                co2_units = rsp['units']
        # Output CSV format
        if output.lower() == "all":
            if row['Ignore'] != 'YES':
                temps += ('{0},{1},{2}'.format(row['Label'], temp_value , co2_value)) + '\n'
        elif output.lower() == "co2":
            if row['Ignore'] != 'YES':
                temps += str(co2_value) + ','
        elif output.lower() == "temp":
            if row['Ignore'] != 'YES':
                temps += str(temp_value) + ','
        elif output.lower() == "location":
            if row['Ignore'] != 'YES':
                temps += (row['Label']) + ','
        else:
            if row['Ignore'] != 'YES':
                temps += ('{0},{1},{2},{3},{4}'.format(row['Label'], temp_value, temp_units, co2_value, co2_units)) + '\n'

    # Report elapsed time
    elapsed_time = round((time.time() - start_time) * 1000) / 1000
    return temps

def dataRecorder():
    location = getBulkAir("location")
    temp = getBulkAir("temp")
    co2 = getBulkAir("co2")
    try:
        df = pd.read_csv('../data/inputco2.csv', na_filter=False, comment='#')
    except:
        f = open("../data/inputco2.csv", "a")
        f.write(location + "\n")
        f.close()

    f = open("../data/inputco2.csv", "a")
    f.write(co2 + "\n")
    f.close()

    try:
        df = pd.read_csv('../data/inputtemp.csv', na_filter=False, comment='#')
    except:
        f = open("../data/inputtemp.csv", "a")
        f.write(location + "\n")
        f.close()

    f = open("../data/inputtemp.csv", "a")
    f.write(temp+ "\n")
    f.close()

    co2Series = pd.read_csv('../data/inputco2.csv').transpose()
    tempSeries = pd.read_csv('../data/inputtemp.csv').transpose()


    largeco2 = co2Series.nlargest(n=14, columns=0, keep='first').transpose()
    largetemp = tempSeries.nlargest(n=14, columns=0, keep='first').transpose()
    smallco2 = co2Series.nsmallest(n=14, columns=0, keep='first').transpose()
    smalltemp = tempSeries.nsmallest(n=14, columns=0, keep='first').transpose()


    try:
        df = pd.read_csv('../data/top14co2.csv', comment='#')
        largeco2.drop(largeco2.index[0], inplace=True)
        largeco2.to_csv("../data/top14co2.csv", mode='a')
    except:
        f = open("../data/top14co2.csv", "a")
        f.write(",1,2,3,4,5,6,7,8,9,10\n")
        f.close()
        largeco2.drop(largeco2.index[0], inplace=True)
        largeco2.to_csv('../data/top14co2.csv', mode='a')

    try:
        df = pd.read_csv('../data/top14temp.csv', comment='#')
        largetemp.drop(largetemp.index[0], inplace=True)
        largetemp.to_csv("../data/top14temp.csv", mode='a')
    except:
        f = open("../data/top14temp.csv", "a")
        f.write(",1,2,3,4,5,6,7,8,9,10\n")
        f.close()
        largetemp.drop(largetemp.index[0], inplace=True)
        largetemp.to_csv('../data/top14temp.csv', mode='a')

    try:
        df = pd.read_csv('../data/bottom14co2.csv', comment='#')
        smallco2.drop(smallco2.index[0], inplace=True)
        smallco2.to_csv("../data/bottom14co2.csv", mode='a')
    except:
        f = open("../data/bottom14co2.csv", "a")
        f.write(",1,2,3,4,5,6,7,8,9,10\n")
        f.close()
        smallco2.drop(smallco2.index[0], inplace=True)
        smallco2.to_csv('../data/bottom14co2.csv', mode='a')

    try:
        df = pd.read_csv('../data/bottom14temp.csv', comment='#').dropna(how='all', axis=1)
        smalltemp.drop(smalltemp.index[0], inplace=True)
        smalltemp.to_csv("../data/bottom14temp.csv", mode='a')
    except:
        f = open("../data/bottom14temp.csv", "a")
        f.write(",1,2,3,4,5,6,7,8,9,10\n")
        f.close()
        smalltemp.drop(smalltemp.index[0], inplace=True)
        smalltemp.to_csv('../data/bottom14temp.csv', mode='a')

    open("../data/inputtemp.csv", "w").close()
    open("../data/inputco2.csv", "w").close()

def getTop(path,top):
    fileRead = open(path, "r")

    reader = csv.reader(fileRead)
    for i, row in enumerate(reader):
        if i == 0:
            break
    line1 = dict.fromkeys(row, 0)
    for j, row2 in enumerate(reader):
        if j == 0:
            break
    line2 = dict.fromkeys(row2, 0)
    for z, row3 in enumerate(reader):
        if z == 0:
            break
    line3 = dict.fromkeys(row3, 0)
    try:
        for j, row4 in enumerate(reader):
            if j == 0:
                break
        line4 = dict.fromkeys(row4, 0)
        for z, row5 in enumerate(reader):
            if z == 0:
                break
        line5 = dict.fromkeys(row5, 0)
        for key in line2:
            if (key in line1):
                line1[key] = line1[key] + 1
            else:
                line1[key] = 0
        for key in line3:
            if (key in line1):
                line1[key] = line1[key] + 1
            else:
                line1[key] = 0
        for key in line4:
            if (key in line1):
                line1[key] = line1[key] + 1
            else:
                line1[key] = 0
        for key in line5:
            if (key in line1):
                line1[key] = line1[key] + 1
            else:
                line1[key] = 0
        t = sorted(line1.items(), key=lambda x: -x[1])[:(top + 1)]
        retvalue = ''
        for x in t[1:]:
            retvalue += ',' + ("{0}".format(*x))
        return (retvalue)
    except:
        for key in line2:
            if (key in line1):
                line1[key] = line1[key] + 1
            else:
                line1[key] = 0
        for key in line3:
            if (key in line1):
                line1[key] = line1[key] + 1
            else:
                line1[key] = 0
        t = sorted(line1.items(), key=lambda x: -x[1])[:(top + 1)]
        retvalue = ''
        for x in t[1:]:
            retvalue += ',' + ("{0}".format(*x))
        return (retvalue)


def clearData():
    open("../data/bottom7co2.csv", "w").close()
    open("../data/bottom7temp.csv", "w").close()
    open("../data/bottom14co2.csv", "w").close()
    open("../data/bottom14temp.csv", "w").close()
    open("../data/inputtemp.csv", "w").close()
    open("../data/inputco2.csv", "w").close()
    open("../data/top7co2.csv", "w").close()
    open("../data/top7temp.csv", "w").close()
    open("../data/top14co2.csv", "w").close()
    open("../data/top14temp.csv", "w").close()

def safeClearData():

    open("../data/bottom14co2.csv", "w").close()
    open("../data/bottom14temp.csv", "w").close()

    open("../data/inputtemp.csv", "w").close()
    open("../data/inputco2.csv", "w").close()

    open("../data/top14co2.csv", "w").close()
    open("../data/top14temp.csv", "w").close()

def endDay():
    f = open("../data/bottom7co2.csv", "a")
    f.write(getTop("../data/bottom14co2.csv",7)+"\n")
    f.close()

    f = open("../data/bottom7temp.csv", "a")
    f.write(getTop("../data/bottom14temp.csv",7)+"\n")
    f.close()

    f = open("../data/top7co2.csv", "a")
    f.write(getTop("../data/top14co2.csv",7)+"\n")
    f.close()

    f = open("../data/top7temp.csv", "a")
    f.write(getTop("../data/top14temp.csv",7)+"\n")
    f.close()
    safeClearData()

def endWeek():
    today = datetime.date.today().strftime("_%B_%d_%Y_")
    f = open("../reports/report"+today+".txt", "w+")
    f.write("TEMPERATURE"+"\n\n"+"Top 5 Temp: "+getTop("../data/top7temp.csv", 5) + "\n" +"Bottom 5 Temp: "+getTop("../data/bottom7temp.csv", 5)+"\n"+"CARBON DIOXIDE"+"\n\n"+"Bottom 5 CO2: "+getTop("../data/bottom7co2.csv", 5)+"\n"+"Top 5 Temp:"+getTop("../data/top7co2.csv", 5))
    clearData()

def quickTest():
    dataRecorder()
    dataRecorder()
    dataRecorder()
    endDay()
    print("DAY 1")
    dataRecorder()
    dataRecorder()
    dataRecorder()
    endDay()
    print("DAY 2")
    dataRecorder()
    dataRecorder()
    dataRecorder()
    endDay()
    print("DAY 3")
    dataRecorder()
    dataRecorder()
    dataRecorder()
    endDay()
    print("DAY 4")
    dataRecorder()
    dataRecorder()
    dataRecorder()
    endDay()
    print("DAY 5")
    endWeek()



schedule.every().monday.at("08:00").do(dataRecorder)
schedule.every().monday.at("11:00").do(dataRecorder)
schedule.every().monday.at("14:00").do(dataRecorder)
schedule.every().monday.at("15:00").do(endDay)

schedule.every().tuesday.at("08:00").do(dataRecorder)
schedule.every().tuesday.at("11:00").do(dataRecorder)
schedule.every().tuesday.at("14:00").do(dataRecorder)
schedule.every().tuesday.at("15:00").do(endDay)

schedule.every().wednesday.at("08:00").do(dataRecorder)
schedule.every().wednesday.at("11:00").do(dataRecorder)
schedule.every().wednesday.at("14:00").do(dataRecorder)
schedule.every().wednesday.at("15:00").do(endDay)

schedule.every().thursday.at("08:00").do(dataRecorder)
schedule.every().thursday.at("11:00").do(dataRecorder)
schedule.every().thursday.at("14:00").do(dataRecorder)
schedule.every().thursday.at("15:00").do(endDay)

schedule.every().friday.at("08:00").do(dataRecorder)
schedule.every().friday.at("11:00").do(dataRecorder)
schedule.every().friday.at("14:00").do(dataRecorder)
schedule.every().friday.at("15:00").do(endDay)
schedule.every().friday.at("16:00").do(endWeek)

while True:
    schedule.run_pending()
    time.sleep(1)