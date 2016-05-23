.. _installation:

============
Installation
============

The last stable release is available on PyPI and can be installed with ``pip``::

    $ pip install PyMySQL

Alternatively (e.g. if ``pip`` is not available), a tarball can be downloaded
from GitHub and installed with Setuptools::

    $ # X.X is the desired PyMySQL version (e.g. 0.5 or 0.6).
    $ curl -L https://github.com/PyMySQL/PyMySQL/tarball/pymysql-X.X | tar xz
    $ cd PyMySQL*
    $ python setup.py install
    $ # The folder PyMySQL* can be safely removed now.

However, you might have to install some requirements first.

Requirements
-------------

* Python -- one of the following:

  - CPython_ >= 2.6 or >= 3.3
  - PyPy_ >= 4.0
  - IronPython_ 2.7

* MySQL Server -- one of the following:

  - MySQL_ >= 4.1  (tested with only 5.5~)
  - MariaDB_ >= 5.1

.. _CPython: http://www.python.org/
.. _PyPy: http://pypy.org/
.. _IronPython: http://ironpython.net/
.. _MySQL: http://www.mysql.com/
.. _MariaDB: https://mariadb.org/