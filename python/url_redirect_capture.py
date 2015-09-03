#!/usr/bin/env python

"""

Filename:           url_redirect_capture.py
Customer:           Beach
Project:            Pivotal Blog Analysis
Created:            August 20th, 2015
Updated:
Contact:            jvawdrey@pivotal.io
Description:        Script to capture redirect URLs
Notes:              * Requires python and below 3rd party
                      dependencies are installed (lines 21 to 23)
                    * Change lines 30 to 37 and 40 below to setup database
                      connection
                    * To run call 'python /python/url_redirect_capture.py'

"""

# 3rd party dependencies
import psycopg2     # Postgres client
import sys          # System-specific parameters and function
import urllib2      # Extensible library for opening URLs

__author__ = "Jarrod Vawdrey (jvawdrey@pivotal.io)"
__version__ = "0.0.1"
__status__ = "Development"

# postgres/greenplum/hawq information
DBNAME="postgres"               # database name
DBUSER="postgres"               # database username
DBPASSWORD="changeme"           # database password
DBHOST="localhost"              # database host
INPUTTABLE="public.testdata"    # database table containing URLs to lookup
INPUTCOLUMN="url"               # column in INPUTTABLE with URL to lookup
OUTPUTTABLE="public.results"    # results table name ()

# other settings
NUMERRORURL=10         # Max number of unsuccessful URL lookups before exiting

# Prepare a header for urllib2 request
HEADER = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11',
       'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
       'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
       'Accept-Encoding': 'none',
       'Accept-Language': 'en-US,en;q=0.8',
       'Connection': 'keep-alive'}

# connect to database (database name, database user, database password, database host)
def connect_db(dbname, dbuser, dbpassword, dbhost):
    try:
        conn = psycopg2.connect("dbname=" + dbname + " user=" + dbuser + " host=" + dbhost + " password=" + dbpassword)
        cur = conn.cursor()
        e = None
        print "Connected to database " + dbname
        return (conn, cur, e)
    except psycopg2.Error as e:
        conn = None
        cur = None
        print "Unable to connect to database " + dbname
        return (conn, cur, e.diag.message_primary)

# disconnect from database
def disconnect_db(connection, cursor):
    try:
        if connection:
            connection.rollback()
        cursor.close()
        connection.close()
        print "Disconnected from database"
    except:
        print "Error disconnecting from database"

# drop database table
def dropTable(connection, cursor, tableName):
    try:
        print "Dropping table '" + tableName + "'"
        cursor.execute("DROP TABLE IF EXISTS " + tableName)
        connection.commit()
        return(None)
    except psycopg2.Error as e:
        return(e.diag.message_primary)

# create database table
def createTable(connection, cursor, tableName):
    print "Creating table '" + tableName + "'"
    # build create table string
    sqlString = "CREATE TABLE " + tableName + " (original_url text, redirect_url text)"
    try:
        # execute and commit query
        cursor.execute(sqlString)
        connection.commit()
        return(None)
    except psycopg2.Error as e:
        return(e.diag.message_primary)

# extractURL list from database input table
def extractURL(connection, cursor, tableName, columnName):
    # build sql query string
    sqlString = "SELECT " + columnName + " FROM (SELECT " + columnName + " FROM " + tableName + " GROUP BY 1) foo ORDER BY 1;"
    try:
        # execute and commit query
        cursor.execute(sqlString)
        res = cursor.fetchall()
        res = [elem[0] for elem in res]
        if (len(res) == 0):
            return(None, "No URL found in column " + columnName)
        else:
            return(res, None)
    except psycopg2.Error as e:
        return(None, e.diag.message_primary)

# insert results into output table
def insertResultsIntoTable(connection, cur, tableName, tup):
    try:
        args_str = ",".join(cur.mogrify("(%s,%s)", x) for x in tup)
        cur.execute("INSERT INTO " + tableName + " VALUES " + args_str)
        connection.commit()
        return None
    except psycopg2.Error as e:
        if connection:
            connection.rollback()
        return e
    except Exception as e1:
        return e
    except Error as e2:
        return e

# capture redirect URL
def captureRedirect(url):
    try:
        request = urllib2.Request(url, headers=HEADER)
        opener = urllib2.build_opener()
        f = opener.open(request)
        r = f.url
        return(r, None)
    except urllib2.URLError as e0:
        return(None,str(e0))
    except urllib2.HTTPError as e1:
        return(None,str(e1))
    except:
        return(None,"Error capturing redirect for URL " + url)

# main driver (calls above function in sequence)
def main():

    # Connect to database
    conn, cur, e = connect_db(DBNAME, DBUSER, DBPASSWORD, DBHOST)
    # If database connection not made exit
    if (e is not None):
        print "Exiting: Unable to connect to database \n" + e
        sys.exit()

    # drop results table if already exists
    e = dropTable(conn, cur, OUTPUTTABLE);
    # If error with dropping table then exit
    if (e is not None):
        print "Exiting: Unable to drop table \n" + e
        sys.exit()

    # create results table
    e = createTable(conn, cur, OUTPUTTABLE);
    # If error with inserting data then exit
    if (e is not None):
        print "Exiting: Error creating table \n" + e
        sys.exit()

    # Grab list of URLs to lookup from database table
    # Note - Currently setup to grab all records at one time ... this will fail
    # with data which is too larger to fit in memory
    urlList,e = extractURL(conn, cur, INPUTTABLE, INPUTCOLUMN)
    # If error with inserting data then exit
    if (e is not None):
        print "Exiting: Error finding list of lookup URL \n" + e
        sys.exit()

    # Create an list to hold results
    finalResults = []

    # Unsuccessful counter
    numerror=0

    # for each URL capture redirect address
    N=len(urlList)
    for i in range(0, N):

        print "[" + str(i + 1) + " of " + str(N) + "] " + urlList[i]

        res,e = captureRedirect(urlList[i])
        # If error capturing redirect URL then exit
        if (e is not None):
            print "Unsuccessful lookup " + str(numerror) + " of " + str(NUMERRORURL) + " - URL " + urlList[i] + "\n" + e
            numerror += 1
            res = ""
            if (numerror > NUMERRORURL):
                print "Exiting: Reached max number of unsuccessful URL lookups \n" + e
                sys.exit()
        finalResults.append((urlList[i],res))

    # insert results into results table
    e = insertResultsIntoTable(conn, cur, OUTPUTTABLE, finalResults)
    # If error with inserting data then exit
    if (e is not None):
        print "Exiting: Error inserting records into database table \n" + e
        sys.exit()

    # Disconnect from database
    disconnect_db(conn, cur)

    # system exit
    sys.exit()

# call driver
main()
