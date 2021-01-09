# Changes

## v1.0.2

Release date: 2021-01-09

* Fix `user`, `password`, `host`, `database` are still positional arguments.
  All arguments of `connect()` are now keyword-only. (#941)


## v1.0.1

Release date: 2021-01-08

* Stop emitting DeprecationWarning for use of ``db`` and ``passwd``.
  Note that they are still deprecated. (#939)
* Add ``python_requires=">=3.6"`` to setup.py. (#936)


## v1.0.0

Release date: 2021-01-07

Backward incompatible changes:

* Python 2.7 and 3.5 are not supported.
* ``connect()`` uses keyword-only arguments. User must use keyword argument.
* ``connect()`` kwargs ``db`` and ``passwd`` are now deprecated; Use ``database`` and ``password`` instead.
* old_password authentication method (used by MySQL older than 4.1) is not supported.
* MySQL 5.5 and MariaDB 5.5 are not officially supported, although it may still works.
* Removed ``escape_dict``, ``escape_sequence``, and ``escape_string`` from ``pymysql``
  module. They are still in ``pymysql.converters``.

Other changes:

* Connection supports context manager API. ``__exit__`` closes the connection. (#886)
* Add MySQL Connector/Python compatible TLS options (#903)
* Major code cleanup; PyMySQL now uses black and flake8.


## v0.10.1

Release date: 2020-09-10

* Fix missing import of ProgrammingError. (#878)
* Fix auth switch request handling. (#890)


## v0.10.0

Release date: 2020-07-18

This version is the last version supporting Python 2.7.

* MariaDB ed25519 auth is supported.
* Python 3.4 support is dropped.
* Context manager interface is removed from `Connection`. It will be added
  with different meaning.
* MySQL warnings are not shown by default because many user report issue to
  PyMySQL issue tracker when they see warning. You need to call "SHOW WARNINGS"
  explicitly when you want to see warnings.
* Formatting of float object is changed from "3.14" to "3.14e0".
* Use cp1252 codec for latin1 charset.
* Fix decimal literal.
* TRUNCATED_WRONG_VALUE_FOR_FIELD, and ILLEGAL_VALUE_FOR_TYPE are now
  DataError instead of InternalError.


## 0.9.3

Release date: 2018-12-18

* cryptography dependency is optional now.
* Fix old_password (used before MySQL 4.1) support.
* Deprecate old_password.
* Stop sending ``sys.argv[0]`` for connection attribute "program_name".
* Close connection when unknown error is happened.
* Deprecate context manager API of Connection object.

## 0.9.2

Release date: 2018-07-04

* Disabled unintentinally enabled debug log
* Removed unintentionally installed tests


## 0.9.1

Release date: 2018-07-03

* Fixed caching_sha2_password and sha256_password raise TypeError on PY2
  (#700, #702)


## 0.9.0

Release date: 2018-06-27

* Change default charset from latin1 to utf8mb4.  (because MySQL 8 changed) (#692)
* Support sha256_password and caching_sha2_password auth method (#682)
* Add cryptography dependency, because it's needed for new auth methods.
* Remove deprecated `no_delay` option (#694)
* Support connection attributes (#679)
* Map LOCK_DEADLOCK to OperationalError (#693)

## 0.8.1

Release date: 2018-05-07

* Reduce `cursor.callproc()` roundtrip time. (#636)

* Fixed `cursor.query()` is hunged after multi statement failed. (#647)

* WRONG_DB_NAME and WRONG_COLUMN_NAME is ProgrammingError for now. (#629)

* Many test suite improvements, especially adding MySQL 8.0 and using Docker.
  Thanks to Daniel Black.

* Dropped support for old Python and MySQL which is not tested long time.


## 0.8

Release date: 2017-12-20

* **BACKWARD INCOMPATIBLE** ``binary_prefix`` option is added and off
  by default because of compatibility with mysqlclient.
  When you need PyMySQL 0.7 behavior, you have to pass ``binary_prefix=True``.
  (#549)

* **BACKWARD INCOMPATIBLE** ``MULTI_STATEMENTS`` client flag is no longer
  set by default, while it was on PyMySQL 0.7.  You need to pass
  ``client_flag=CLIENT.MULTI_STATEMENTS`` when you connect to explicitly
  enable multi-statement mode. (#590)

* Fixed AuthSwitch packet handling.

* Raise OperationalError for MariaDB's constraint error. (#607)

* executemany() accepts query without space between ``VALUES`` and ``(``.  (#597)

* Support config file containing option without value. (#588)

* Fixed Connection.ping() returned unintended value.


## 0.7.11

Release date: 2017-04-06

* Fixed Connection.close() failed when failed to send COM_CLOSE packet.
* Cursor.executemany() accepts query ends with semicolon.
* ssl parameters can be read from my.cnf.


## 0.7.10

Release date: 2017-02-14

* **SECURITY FIX**: Raise RuntimeError when received LOAD_LOCAL packet while
  ``loacal_infile=False``.  (Thanks to Bryan Helmig)

* Raise SERVER_LOST error for MariaDB's shutdown packet (#540)

* Change default connect_timeout to 10.

* Add bind_address option (#529)


## 0.7.9

Release date: 2016-09-03

* Fix PyMySQL stop reading rows when first column is empty string (#513)
  Reverts DEPRECATE_EOF introduced in 0.7.7.

## 0.7.8

Release date: 2016-09-01

* Revert error message change in 0.7.7.
  (SQLAlchemy parses error message, #507)

## 0.7.7

Release date: 2016-08-30

* Add new unicode collation (#498)
* Fix conv option is not used for encoding objects.
* Experimental support for DEPRECATE_EOF protocol.

## 0.7.6

Release date: 2016-07-29

* Fix SELECT JSON type cause UnicodeError
* Avoid float convertion while parsing microseconds
* Warning has number
* SSCursor supports warnings

## 0.7.5

Release date: 2016-06-28

* Fix exception raised while importing when getpwuid() fails (#472)
* SSCursor supports LOAD DATA LOCAL INFILE (#473)
* Fix encoding error happen for JSON type (#477)
* Fix test fail on Python 2.7 and MySQL 5.7 (#478)

## 0.7.4

Release date: 2016-05-26

* Fix AttributeError may happen while Connection.__del__ (#463)
* Fix SyntaxError in test_cursor. (#464)
* frozenset support for query value. (#461)
* Start using readthedocs.io

## 0.7.3

Release date: 2016-05-19

* Add read_timeout and write_timeout option.
* Support serialization customization by `conv` option.
* Unknown type is converted by `str()`, for MySQLdb compatibility.
* Support '%%' in `Cursor.executemany()`
* Support REPLACE statement in `Cursor.executemany()`
* Fix handling incomplete row caused by 'SHOW SLAVE HOSTS'.
* Fix decode error when use_unicode=False on PY3
* Fix port option in my.cnf file is ignored.


## 0.7.2

Release date: 2016-02-24

* Fix misuse of `max_allowed_packet` parameter. (#426, #407 and #397)
* Add %(name)s plceholder support to `Cursor.executemany()`. (#427, thanks to
  @WorldException)

## 0.7.1

Release date: 2016-01-14

* Fix auth fail with MySQL 5.1
* Fix escaping unicode fails on Python 2

## 0.7

Release date: 2016-01-10

* Faster binary escaping
* Add `"_binary" prefix` to string literal for binary types.
  binary types are: `bytearray` on Python 2, `bytes` and `bytearray` on Python 3.
  This is because recent MySQL show warnings when string literal is invalid for
  connection encoding.
* `pymysql.Binary()` returns `bytearray` on Python 2.  This is required to distinguish
  binary and string.
* Auth plugin support.
* no_delay option is ignored.  It will be removed in PyMySQL 0.8.


## 0.6.7

Release date: 2015-09-30

* Allow self signed certificate
* Add max_allowed_packet option
* Fix error when bytes in executemany
* Support geometry type
* Add coveralls badge to README
* Fix some bugs relating to warnings
* Add Cursor.mogrify() method
* no_delay option is deprecated and True by default
* Fix options from my.cnf overrides options from arguments
* Allow socket like object.  (It's not feature for end users)
* Strip quotes while reading options from my.cnf file
* Fix encoding issue in executemany()

## 0.6.6

* Add context manager to cursor
* Fix can't encode blob that is not utf-8 on PY3. (regression of 0.6.4,
  Thanks to @wiggzz)

## 0.6.5
Skipped

## 0.6.4
* Support "LOAD LOCAL INFILE".  Thanks @wraziens
* Show MySQL warnings after execute query.
* Fix MySQLError may be wrapped with OperationalError while connectiong. (#274)
* SSCursor no longer attempts to expire un-collected rows within __del__,
  delaying termination of an interrupted program; cleanup of uncollected
  rows is left to the Connection on next execute, which emits a
  warning at that time. (#287)
* Support datetime and time with microsecond. (#303)
* Use surrogateescape to format bytes on Python 3.
* OperationalError raised from connect() have information about original
  exception. (#304)
* `init_command` now support multi statement.
* `Connection.escape()` method now accepts second argument compatible to
  MySQL-Python.

## 0.6.3
* Fixed multiple result sets with SSCursor.
* Fixed connection timeout.
* Fixed literal set syntax to work on Py2.6.
* Allow for mysql negative values with 0 hour timedelta.
* Added Connection.begin().

## 0.6.2
* Fixed old password on Python 3.
* Added support for bulk insert in Cursor.executemany().
* Added support for microseconds in datetimes and dates before 1900.
* Several other bug fixes.

## 0.6.1
* Added cursor._last_executed for MySQLdb compatibility
* Cursor.fetchall() and .fetchmany now return list, not tuple
* Allow "length of auth-plugin-data" = 0
* Cursor.connection references connection object without weakref

## 0.6
* Improved Py3k support
* Improved PyPy support
* Added IPv6 support
* Added Thing2Literal for Django/MySQLdb compatibility
* Removed errorhandler
* Fixed GC errors
* Improved test suite
* Many bug fixes
* Many performance improvements

## 0.4
* Miscellaneous bug fixes
* Implementation of SSL support
* Implementation of kill()
* Cleaned up charset functionality
* Fixed BIT type handling
* Connections raise exceptions after they are close()'d
* Full Py3k and unicode support

## 0.3
* Implemented most of the extended DBAPI 2.0 spec including callproc()
* Fixed error handling to include the message from the server and support
  multiple protocol versions.
* Implemented ping()
* Implemented unicode support (probably needs better testing)
* Removed DeprecationWarnings
* Ran against the MySQLdb unit tests to check for bugs
* Added support for client_flag, charset, sql_mode, read_default_file,
  use_unicode, cursorclass, init_command, and connect_timeout.
* Refactoring for some more compatibility with MySQLdb including a fake
  pymysql.version_info attribute.
* Now runs with no warnings with the -3 command-line switch
* Added test cases for all outstanding tickets and closed most of them.
* Basic Jython support added.
* Fixed empty result sets bug.
* Integrated new unit tests and refactored the example into one.
* Fixed bug with decimal conversion.
* Fixed string encoding bug. Now unicode and binary data work!
* Added very basic docstrings.

## 0.2
* Changed connection parameter name 'password' to 'passwd'
  to make it more plugin replaceable for the other mysql clients.
* Changed pack()/unpack() calls so it runs on 64 bit OSes too.
* Added support for unix_socket.
* Added support for no password.
* Renamed decorders to decoders.
* Better handling of non-existing decoder.
