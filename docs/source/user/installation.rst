.. _installation:

============
Installation
============

The last stable release is available on PyPI and can be installed with ``pip``::

    $ python3 -m pip install PyMySQL

To use "sha256_password" or "caching_sha2_password" for authenticate,
you need to install additional dependency::

   $ python3 -m pip install PyMySQL[rsa]

Requirements
-------------

* Python -- one of the following:

  - CPython_ >= 3.7
  - Latest PyPy_ 3

* MySQL Server -- one of the following:

  - MySQL_ >= 5.7
  - MariaDB_ >= 10.3

.. _CPython: http://www.python.org/
.. _PyPy: http://pypy.org/
.. _MySQL: http://www.mysql.com/
.. _MariaDB: https://mariadb.org/
