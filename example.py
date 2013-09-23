#!/usr/bin/env python

import pymysql

#conn = pymysql.connect(host='127.0.0.1', unix_socket='/tmp/mysql.sock', user='root', passwd=None, db='mysql')
conn = pymysql.connect(host='127.0.0.1', port=3306, user='root', passwd='', db='mysql')
   
cur = conn.cursor()

cur.execute("SELECT Host,User FROM user")
cur.connection.commit() # The previous line will not perform the query until this line comes along.
# print cur.description

# r = cur.fetchall()
# print r
# ...or...
for r in cur.fetchall():
   print(r)

cur.close()
conn.close()

