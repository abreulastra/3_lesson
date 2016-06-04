# -*- coding: utf-8 -*-
"""
Created on Tue May 31 12:51:05 2016

@author: AbreuLastra_Work
"""

import requests
import pandas as pd
import sqlite3 as lite
import time
# a package for parsing a string into a Python datetime object
from dateutil.parser import parse 
from pandas.io.json import json_normalize
import collections

r = requests.get('http://www.citibikenyc.com/stations/json')

df = json_normalize(r.json()['stationBeanList'])


""" We are skiping all this code to iterate this program 60 times
key_list = [] #unique list of keys for each station listing
for station in r.json()['stationBeanList']:
    for k in station.keys():
        if k not in key_list:
            key_list.append(k)
            


r.json()['stationBeanList'][0]




df['availableBikes'].hist()
plt.show()

df['totalDocks'].hist()
plt.show()

#This gives the mean and median value for total docs

df['totalDocks'].mean()
df['totalDocks'].median()

#This averages every variable, by service status in the statusValue variables
df.groupby("statusValue").agg([len])['availableDocks']

df.groupby(["statusValue"]).mean()

#This creates a framework that filters in the stattions in services
condition = (df['statusValue'] == 'In Service')
df[condition]['totalDocks'].mean()

df[df['statusValue'] == 'In Service']['totalDocks'].median()
"""

## Now we want to design a database to store the available bikes for every station, by minute

con = lite.connect('citi_bike.db')
cur = con.cursor()
with con:
    cur.execute('DROP TABLE IF EXISTS citibike_reference')    
    cur.execute('CREATE TABLE citibike_reference (id INT PRIMARY KEY, totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, longitude NUMERIC, postalCode TEXT, testStation TEXT, stAddress1 TEXT, stationName TEXT, landMark TEXT, latitude NUMERIC, location TEXT)')

# WE ARE GOING TO INSERT EVREY TIME AN SQL STATEMENT

sql = "INSERT INTO citibike_reference(id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
#THIS POPULATES DE VALUES IN THE DATABASE, FOR EVERY PERIOD
with con:
    for station in r.json()['stationBeanList']:
        cur.execute(sql,(station['id'], station['totalDocks'], station['city'], station['altitude'], station['stAddress2'], station['longitude'], station['postalCode'], station['testStation'], station['stAddress1'], station['stationName'], station['landMark'], station['latitude'], station['location']))


#extract id column to build identifier
station_ids = df['id'].tolist()
#add "_" to the station name
station_ids = ['_' + str(x) + ' INT' for x in station_ids]

#create table 
# concatenating for every line the new reference and the values
with con:
    cur.execute('DROP TABLE IF EXISTS available_bikes')    
    cur.execute("CREATE TABLE available_bikes (execution_time INT, " + ", ".join(station_ids) + ");")
    con.commit()

## now we need to loop this code over an hour

for x in range(60):
    
    # Upddate the database    
    r = requests.get('http://www.citibikenyc.com/stations/json')
        
    exec_time = parse(r.json()['executionTime'])
    
    with con:
        cur.execute("INSERT INTO available_bikes (execution_time) VALUES (?)", (exec_time.strftime('%s'),))
        con.commit() 
    
    id_bikes = collections.defaultdict(int)
    #loop through the stations in the station list
    for station in r.json()['stationBeanList']:
        id_bikes[station['id']] = station['availableBikes']
        
    #iterate through the defaultdict to update the values in the database
    with con:
        for k, v in id_bikes.iteritems():
            cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) + " WHERE execution_time = " + exec_time.strftime('%s') + ";")
            con.commit() 
           
        
    print(x)
    print(exec_time)
    if x % 5 == 0:
        example = pd.read_sql('select * from available_bikes', con)
        print(example.tail())       
    print(exec_time)
    time.sleep(60)

con.close()