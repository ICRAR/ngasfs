import sqlite3 as lite
import sys
import atpy
import time
import datetime
import os

database = 'dumpfs.sqlite'

SERVER_LOCATION = 'http://pleiades.icrar.org:7777/'

#Create a table list of all available files on the server.
T = atpy.Table(SERVER_LOCATION + 'QUERY?query=files_list&format=list',type='ascii')

fileName = []
size = []
ctime = []
itime = []

#Extract all filenames and append to a single list.
for s in T['col3']:
    fileName.append(s)

#Extract all sizes and append to a single list.
for i in T['col6']:
    size.append(i)

#Extract all change times and append to a single list.
for t in T['col14']:
    ctime.append(t)

#Extract all access/modify times and append to a single list.
for t in T['col9']:
    itime.append(t)



con = None
#Create a connection to the database if it exists.
if os.path.isfile(database):
    con = lite.connect(database)
else:
    print "Error: Database doesn't exist!"
    exit()

#Use the connection to execute the following:
with con:
    
    cur = con.cursor() #Create cursor
    cur.execute("SELECT * FROM dentry ORDER BY parent_id DESC")
    parent_id = cur.fetchone() #Get highest existng parent_id
    if parent_id == None:
        parent_id = 1 #If None then set to 1
    else:
        parent_id = parent_id[1]
    cur.execute("SELECT * FROM inode ORDER BY id DESC")
    inodeID = cur.fetchone()[0] #Get highest inode id
    cur.execute("SELECT * FROM inode ORDER BY inode_num DESC")
    inode_num = cur.fetchone()[1] #Get highest inode_num
    cur.execute("SELECT * FROM dentry ORDER BY id DESC")
    dentryID = cur.fetchone() #Get highest dentry id
    if dentryID == None:
        dentryID = 0 #If None then set to 0
    else:
        dentryID = dentryID[0]
    cur.execute("SELECT * FROM data_list ORDER BY id DESC")
    data_listID = cur.fetchone() #Get highests data_list id
    if data_listID == None:
        data_listID = 0 #If None then set to 0
    else:
        data_listID = data_listID[0]

    for i in range(len(fileName)):
        """
        For every object in our original table, strip the nessesary data and then add it to the tables.
        This adds the filename, filesize and access/modify/change times to the database for everytime.
        This creates 'fake' files in the DB the appear when it is mounted.
        """
        cTime = ctime[i].replace("T", " ")
        cTime = time.mktime(datetime.datetime.strptime(cTime, "%Y-%m-%d %H:%M:%S.%f").timetuple())
        iTime = itime[i].replace("T", " ")
        iTime = time.mktime(datetime.datetime.strptime(iTime, "%Y-%m-%d %H:%M:%S.%f").timetuple())
        iString = "INSERT INTO inode VALUES({0}, {1}, 1, 1000, 1000, {2}, {2}, {3}, {4}, 33060, 0)".format(inodeID+i+1, inode_num+i+1, iTime, cTime, size[i])
        deString = "INSERT INTO dentry VALUES({0}, {1}, '{2}', {3})".format(dentryID+i+1, parent_id, fileName[i], inode_num+i+1)
        daString = "INSERT INTO data_list VALUES({0}, {1}, 0, 0)".format(data_listID+i+1, inodeID+i+1)
        cur.execute(iString)
        cur.execute(deString)
        cur.execute(daString)
