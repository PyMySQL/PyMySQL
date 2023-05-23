"""
PyMySQL: A pure-Python MySQL client library.

Copyright (c) 2010-2016 PyMySQL contributors

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
"""
import sys

from .constants import FIELD_TYPE
from .err import (
    Warning,
    Error,
    InterfaceError,
    DataError,
    DatabaseError,
    OperationalError,
    IntegrityError,
    InternalError,
    NotSupportedError,
    ProgrammingError,
    MySQLError,
)
from .times import (
    Date,
    Time,
    Timestamp,
    DateFromTicks,
    TimeFromTicks,
    TimestampFromTicks,
)


VERSION = (1, 0, 3)
if len(VERSION) > 3:
    VERSION_STRING = "%d.%d.%d_%s" % VERSION
else:
    VERSION_STRING = "%d.%d.%d" % VERSION
threadsafety = 1
apilevel = "2.0"
paramstyle = "pyformat"

from . import connections  # noqa: E402


class DBAPISet(frozenset):
    def __ne__(self, other):
        if isinstance(other, set):
            return frozenset.__ne__(self, other)
        else:
            return other not in self

    def __eq__(self, other):
        if isinstance(other, frozenset):
            return frozenset.__eq__(self, other)
        else:
            return other in self

    def __hash__(self):
        return frozenset.__hash__(self)


STRING = DBAPISet([FIELD_TYPE.ENUM, FIELD_TYPE.STRING, FIELD_TYPE.VAR_STRING])
BINARY = DBAPISet(
    [
        FIELD_TYPE.BLOB,
        FIELD_TYPE.LONG_BLOB,
        FIELD_TYPE.MEDIUM_BLOB,
        FIELD_TYPE.TINY_BLOB,
    ]
)
NUMBER = DBAPISet(
    [
        FIELD_TYPE.DECIMAL,
        FIELD_TYPE.DOUBLE,
        FIELD_TYPE.FLOAT,
        FIELD_TYPE.INT24,
        FIELD_TYPE.LONG,
        FIELD_TYPE.LONGLONG,
        FIELD_TYPE.TINY,
        FIELD_TYPE.YEAR,
    ]
)
DATE = DBAPISet([FIELD_TYPE.DATE, FIELD_TYPE.NEWDATE])
TIME = DBAPISet([FIELD_TYPE.TIME])
TIMESTAMP = DBAPISet([FIELD_TYPE.TIMESTAMP, FIELD_TYPE.DATETIME])
DATETIME = TIMESTAMP
ROWID = DBAPISet()


def Binary(x):
    """Return x as a binary type."""
    return bytes(x)


Connect = connect = Connection = connections.Connection


def get_client_info():  # for MySQLdb compatibility
    return VERSION_STRING


# we include a doctored version_info here for MySQLdb compatibility
version_info = (1, 4, 0, "final", 0)

NULL = "NULL"

__version__ = get_client_info()


def thread_safe():
    return True  # match MySQLdb.thread_safe()


def install_as_MySQLdb():
    """
    After this function is called, any application that imports MySQLdb or
    _mysql will unwittingly actually use pymysql.
    """
    sys.modules["MySQLdb"] = sys.modules["_mysql"] = sys.modules["pymysql"]


__all__ = [
    "BINARY",
    "Binary",
    "Connect",
    "Connection",
    "DATE",
    "Date",
    "Time",
    "Timestamp",
    "DateFromTicks",
    "TimeFromTicks",
    "TimestampFromTicks",
    "DataError",
    "DatabaseError",
    "Error",
    "FIELD_TYPE",
    "IntegrityError",
    "InterfaceError",
    "InternalError",
    "MySQLError",
    "NULL",
    "NUMBER",
    "NotSupportedError",
    "DBAPISet",
    "OperationalError",
    "ProgrammingError",
    "ROWID",
    "STRING",
    "TIME",
    "TIMESTAMP",
    "Warning",
    "apilevel",
    "connect",
    "connections",
    "constants",
    "converters",
    "cursors",
    "get_client_info",
    "paramstyle",
    "threadsafety",
    "version_info",
    "install_as_MySQLdb",
    "__version__",
]
