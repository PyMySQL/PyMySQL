========
CyMySQL
========

What's CyMySQL
--------------

This package contains a python MySQL client library.

It is a fork project from PyMySQL https://pymysql.readthedocs.io/en/latest/.

PyMySQL is written by Yutaka Matsubara <yutaka.matsubara@gmail.com>
as a pure python database driver.

CyMySQL accerarates by Cython, and support not only python 2 but also python 3.
It still can work without Cython as a pure python driver.

It is maintained by Hajime Nakagami <nakagami@gmail.com>.

Documentation on the MySQL client/server protocol can be found here:
http://dev.mysql.com/doc/internals/en/client-server-protocol.html

Requirements
-------------

- Python 2.6, 2.7, 3.3+
- MySQL 4.1 or higher
    
Installation & Example
-----------------------

Install cython (optional) ::

   # pip install cython

Install cymysql ::

   # pip install cymysql

Example ::

   import cymysql
   conn = cymysql.connect(host='127.0.0.1', user='root', passwd='', db='database_name', charset='utf8')
   cur = conn.cursor()
   cur.execute('select foo, bar from baz')
   for r in cur.fetchall():
      print(r[0], r[1])

