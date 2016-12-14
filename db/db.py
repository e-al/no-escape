#!/usr/bin/python

# -*- coding: utf-8 -*-

import datetime
import sys
import re

import psycopg2

class Airport:
    """This represents an airport"""

    def __init__(self):
        self.name = ""
        self.longitude = 0.
        self.latitude = 0.

    def to_tuple(self):
        return self.name, self.longitude, self.latitude


class MeteoStation:
    """Represents meteo station that is parsed from NOAA data"""

    def __init__(self, id='999999', longitude=0., latitude=0., elevation=0):
        self.id = id
        self.longitude = longitude
        self.latitude = latitude
        self.elevation = elevation

    def to_tuple(self):
        return self.id, self.longitude, self.latitude, self.elevation

class MeteoReading:
    """Represents one data point from NOAA data"""

    def __init__(self, station=None, date_time=None, pressure=0):
        self.station = station
        self.date_time = date_time
        self.pressure = pressure

    def to_tuple(self):
        return self.station, self.date_time, self.pressure


class MeteoParser:
    """Parses Integrated Surface Global Hourly Data from NOAA"""

    def __init__(self):
        self.datetime_format = "%Y%m%d%H%M"

        # regex for mandatory data, we have to match it since we suppose the beginning of the string
        self.parse_re_mandatory = re.compile(
            "(?P<len>[0-9]{4})"  # length of the data
            "(?P<usaf_id>.{6})"  # station ID in USAF  format
            "(?P<wban_id>[0-9]{5})"  # station ID in WBAN format
            "(?P<date>[0-9]{8})"  # date in format YYYYMMDD
            "(?P<time>[0-9]{4})"  # time in format HHMM
            "(?:.)"  # data source flag, not used
            "(?P<lat>(\+|\-)[0-9]{5})"  # latitude of the coordinate of the station
            "(?P<lon>(\+|\-)[0-9]{6})"  # longitude of the coordinate of the station
            "(?:.{5})"  # code, not required for our purposes
            "(?P<elev>(\+|\-)[0-9]{4})"  # elevation above the sea lvl of the station
            "(?:.{49})"  # not required fields
            "(?P<air_pres>[0-9]{5})"  # atm pressure relative to mean sea lvl
        )
        # regex for additional (optional) data, we have to search it, since additional data is located within the string
        self.parse_re_add = re.compile(
            "(?:ADD(.*)?MA1)"  # id of additional data section
            "(?:.{6})"  # not required fields
            "(?P<air_pres>[0-9]{5})"  # absolute atm pressure
        )

    def parse_files(self, filenames=[]):
        """Returns {MeteoStation, [MeteoReadings]}"""
        result = dict()
        for file in filenames:
            readings = self.parse_file(filenames)
            if len(readings[1]) != 0:
                result[readings[0]] = readings[1]

        return result

    def parse_file(self, filename=""):
        """Returns (MeteoStation, [MeteoReadings])"""

        readings = []

        if not filename:
            return []
        try:
            with open(filename) as f:
                for line in f:
                    mandatory_data_match = self.parse_re_mandatory.match(line)

                    if not mandatory_data_match:
                        continue  # we do not want this station, no data is available for it

                    id = mandatory_data_match.group('usaf_id')
                    long = mandatory_data_match.group('lon')
                    lat = mandatory_data_match.group('lat')
                    elevation = mandatory_data_match.group('elev')
                    date = mandatory_data_match.group('date')
                    time = mandatory_data_match.group('time')
                    pressure = mandatory_data_match.group('air_pres')

                    add_data_match = self.parse_re_add.search(line)
                    if add_data_match:
                        new_pressure = add_data_match.group('air_pres')
                        if new_pressure != "99999":
                            pressure = new_pressure

                    # convert date and time into datetime format
                    dt = datetime.datetime.strptime(date + time, self.datetime_format)
                    station = MeteoStation(id, long, lat, elevation)
                    readings.append(MeteoReading(station, dt, pressure))

        except IOError:
            print("Cannot open file %s" % filename)
            return []

        return readings


class DBConnector:
    """Exposes interfaces to access and modify weather/airport DB"""

    def __init__(self, dbname='weatherdb', user='e-al'):
        self.dbname = dbname
        self.user = user

        self.con = None

        try:
            self.con = psycopg2.connect(database=dbname, user=user)
            # enable extensions for geo-calculations
            cur = self.con.cursor()
            cur.execute("CREATE EXTENSION IF NOT EXISTS cube")
            cur.execute("CREATE EXTENSION IF NOT EXISTS earthdistance")

        except psycopg2.DatabaseError as e:
            print('Error %s' % e)
            sys.exit(1)

    def populate_airports(self, airports=[]):
        if not airports:
            return
        if not self.con:
            raise RuntimeError("Not connected to DB")
        try:
            cur = self.con.cursor()
            cur.execute("DROP TABLE IF EXISTS airports")
            cur.execute("CREATE TABLE airports(id SERIAL PRIMARY KEY, name VARCHAR(100), lon DOUBLE, lat DOUBLE)")
            query = "INSERT INTO airports (id, name, lon, lat) VALUES (%s, %s, %s)"

            cur.executemany(query, [x.to_tuple() for x in airports])

        except psycopg2.DatabaseError as e:
            print("DB error: Cannot populate airports")
            raise

    def populate_meteo_stations(self, stations=[]):
        if not len(stations):
            return
        if not self.con:
            raise RuntimeError("Not connected to DB")
        try:
            cur = self.con.cursor()
            cur.execute("DROP TABLE IF EXISTS stations")
            cur.execute("CREATE TABLE stations(id SERIAL PRIMARY KEY, usaf_id INTEGER, lon DOUBLE, lat DOUBLE,"
                        " elevation DOUBLE)")
            query = "INSERT INTO stations (id, usaf_id, lon, lat, elevation) VALUES (%s, %s, %s, %s, %s)"

            cur.executemany(query, [x.to_tuple() for x in stations])

        except psycopg2.DatabaseError as e:
            print("DB error: Cannot populate weather stations")
            raise

    def populate_meteo_readings(self, readings=[]):
        if not len(readings):
            return
        if not self.con:
            raise RuntimeError("Not connected to DB")
        try:
            cur = self.con.cursor()
            cur.execute("DROP TABLE IF EXISTS readings")
            cur.execute("CREATE TABLE readings(id SERIAL PRIMARY KEY, station_id INTEGER REFERENCES stations,"
                        " datetime TIMESTAMP, pressure DOUBLE)")
            query = "INSERT INTO readings (id, station_id, datetime, pressure) VALUES (%s, %s, %s, %s)"

            cur.executemany(query, [x.to_tuple() for x in readings])

        except psycopg2.DatabaseError as e:
            print("DB error: Cannot populate weather readings")
            raise
        pass

    def get_airports_from_airpressure(self, pressure, time):
        """Lookup the airports that had air pressure similar to the one that is passed around the time"""
        # first get weather stations that had same pressure readings around passed time
        # for the time being we try the exact values

        query = "SELECT (station_id where )"

    def disconnect(self):
        if self.con:
            self.con.close()


mp = MeteoParser()
# cur = con.cursor()
# cur.execute('SELECT version()')
# ver = cur.fetchone()
# print(ver)

readings = mp.parse_file("../test/062800-99999-2016")

print(readings)