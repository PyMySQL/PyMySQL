.. _development:

===========
Development
===========

You can help developing PyMySQL by `contributing on GitHub`_.

.. _contributing on GitHub: https://github.com/PyMySQL/PyMySQL

Building the documentation
--------------------------

Go to the ``docs`` directory and run ``make html``.


Test Suite
-----------

If you would like to run the test suite, create a database for testing like this::

    mysql -e 'create database test_pymysql  DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;'
    mysql -e 'create database test_pymysql2 DEFAULT CHARACTER SET utf8 DEFAULT COLLATE utf8_general_ci;'

Then, copy the file ``.travis/database.json`` to ``pymysql/tests/databases.json``
and edit the new file to match your MySQL configuration::

    $ cp .travis/database.json pymysql/tests/databases.json
    $ $EDITOR pymysql/tests/databases.json

To run all the tests, execute the script ``runtests.py``::

    $ python runtests.py

A ``tox.ini`` file is also provided for conveniently running tests on multiple
Python versions::

    $ tox
