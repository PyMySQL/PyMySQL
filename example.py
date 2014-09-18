#!/usr/bin/env python

from __future__ import print_function

import pymysql

conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='', db='mysql')

cur = conn.cursor()

cur.execute("SELECT Host,User FROM user")

print(cur.description)

print()

for row in cur:
   print(row)

cur.close()
conn.close()
