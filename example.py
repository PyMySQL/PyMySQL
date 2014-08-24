#!/usr/bin/env python
from __future__ import print_function

from tornado import ioloop, gen
import tornado_mysql

@gen.coroutine
def main():
    conn = yield tornado_mysql.connect(host='127.0.0.1', port=3306, user='root', passwd='', db='mysql')
    cur = conn.cursor()
    yield cur.execute("SELECT Host,User FROM user")
    print(cur.description)
    print()
    for row in cur:
       print(row)
    cur.close()
    conn.close()


ioloop.IOLoop.current().run_sync(main)
