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

- Python 2.7, 3.5+
- MySQL 5.5 or higher
    
Installation
--------------

Install cython (optional)
++++++++++++++++++++++++++++++

::

   # pip install cython

Install cymysql
++++++++++++++++++++++++++++++

::

   # pip install cymysql

MySQL 8.0 and insecure connection
+++++++++++++++++++++++++++++++++++

If you use caching_sha2_password authentication plugin (MySQL 8.0 default)
and connect with 'not ssl and not unix_socket' you shoud install pycryptodome

::

   # pip install pycryptodome


Example
---------------

::

   import cymysql
   conn = cymysql.connect(host='127.0.0.1', user='root', passwd='', db='database_name')
   cur = conn.cursor()
   cur.execute('select foo, bar from baz')
   for r in cur.fetchall():
      print(r[0], r[1])

