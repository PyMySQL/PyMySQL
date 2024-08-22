========
CyMySQL
========

What's CyMySQL
--------------

This package contains a python MySQL client library.

It is a fork project from PyMySQL https://pymysql.readthedocs.io/en/latest/.

CyMySQL accerarates by Cython, and support not only python 2 but also python 3.
It still can work without Cython as a pure python driver.

Documentation on the MySQL client/server protocol can be found here:
http://dev.mysql.com/doc/internals/en/client-server-protocol.html

Requirements
-------------

- Python 2.7, 3.8+
- MySQL 5.7 or higher
    
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

Install pycryptodome(depending on a situation)
++++++++++++++++++++++++++++++++++++++++++++++++++++++++

::

   # pip install pycryptodome

If you use caching_sha2_password authentication plugin
and connect with 'not ssl and not unix_socket' you shoud install pycryptodome.
It means that if the following error occur).

::

   ModuleNotFoundError: No module named 'Crypto'



Example
---------------

Python Database API Specification v2.0
+++++++++++++++++++++++++++++++++++++++++

https://peps.python.org/pep-0249/

::

   import cymysql
   conn = cymysql.connect(host='127.0.0.1', user='root', passwd='', db='database_name')
   cur = conn.cursor()
   cur.execute('select foo, bar from baz')
   for r in cur.fetchall():
      print(r[0], r[1])

asyncio
++++++++++++++++++++++++++++++++++++++

In Python3, you can use asyncio to write the following.

This API is experimental.
If there are any mistakes, please correct them in the pull request and send.

Use connect
::

   import asyncio
   import cymysql

   async def conn_example():
       conn = await cymysql.aio.connect(
           host="127.0.0.1",
           user="root",
           passwd="",
           db="database_name",
       )
       cur = conn.cursor()
       await cur.execute("SELECT 42")
       print(await cur.fetchall())
   asyncio.run(conn_example())

Use pool
::

   import asyncio
   import cymysql

   async def pool_example(loop):
       pool = await cymysql.aio.create_pool(
           host="127.0.0.1",
           user="root",
           passwd="",
           db="database_name",
           loop=loop,
       )
       async with pool.acquire() as conn:
           async with conn.cursor() as cur:
               await cur.execute("SELECT 42")
               print(await cur.fetchall())
       pool.close()
       await pool.wait_closed()

   loop = asyncio.get_event_loop()
   loop.run_until_complete(pool_example(loop))
   loop.close()
