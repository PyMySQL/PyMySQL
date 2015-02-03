'''
Tornado-MySQL: A pure-Python MySQL client library for Tornado.

Copyright (c) 2010, 2013-2014 PyMySQL contributors

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

'''

VERSION = (0, 6, 3, None)

from ._compat import text_type, JYTHON, IRONPYTHON
from .constants import FIELD_TYPE
from .converters import escape_dict, escape_sequence, escape_string
from .err import Warning, Error, InterfaceError, DataError, \
     DatabaseError, OperationalError, IntegrityError, InternalError, \
     NotSupportedError, ProgrammingError, MySQLError
from .times import Date, Time, Timestamp, \
    DateFromTicks, TimeFromTicks, TimestampFromTicks

import sys
from tornado import gen


threadsafety = 1
apilevel = "2.0"
paramstyle = "format"

class DBAPISet(frozenset):


    def __ne__(self, other):
        if isinstance(other, set):
            return super(DBAPISet, self).__ne__(self, other)
        else:
            return other not in self

    def __eq__(self, other):
        if isinstance(other, frozenset):
            return frozenset.__eq__(self, other)
        else:
            return other in self

    def __hash__(self):
        return frozenset.__hash__(self)


STRING    = DBAPISet([FIELD_TYPE.ENUM, FIELD_TYPE.STRING,
                     FIELD_TYPE.VAR_STRING])
BINARY    = DBAPISet([FIELD_TYPE.BLOB, FIELD_TYPE.LONG_BLOB,
                     FIELD_TYPE.MEDIUM_BLOB, FIELD_TYPE.TINY_BLOB])
NUMBER    = DBAPISet([FIELD_TYPE.DECIMAL, FIELD_TYPE.DOUBLE, FIELD_TYPE.FLOAT,
                     FIELD_TYPE.INT24, FIELD_TYPE.LONG, FIELD_TYPE.LONGLONG,
                     FIELD_TYPE.TINY, FIELD_TYPE.YEAR])
DATE      = DBAPISet([FIELD_TYPE.DATE, FIELD_TYPE.NEWDATE])
TIME      = DBAPISet([FIELD_TYPE.TIME])
TIMESTAMP = DBAPISet([FIELD_TYPE.TIMESTAMP, FIELD_TYPE.DATETIME])
DATETIME  = TIMESTAMP
ROWID     = DBAPISet()

def Binary(x):
    """Return x as a binary type."""
    if isinstance(x, text_type) and not (JYTHON or IRONPYTHON):
        return x.encode()
    return bytes(x)

@gen.coroutine
def connect(*args, **kwargs):
    """See connections.Connection.__init__() for information about defaults."""
    from .connections import Connection
    conn = Connection(*args, **kwargs)
    yield conn.connect()
    raise gen.Return(conn)

from . import connections as _orig_conn
if _orig_conn.Connection.__init__.__doc__ is not None:
    connect.__doc__ = _orig_conn.Connection.__init__.__doc__ + ("""
See connections.Connection.__init__() for information about defaults.
""")
del _orig_conn

def get_client_info():  # for MySQLdb compatibility
    return '.'.join(map(str, VERSION))


# we include a doctored version_info here for MySQLdb compatibility
version_info = (1,2,2,"final",0)

NULL = "NULL"

__version__ = get_client_info()

__all__ = [
    'BINARY', 'Binary', 'connect', 'Connection', 'DATE', 'Date',
    'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks', 'TimestampFromTicks',
    'DataError', 'DatabaseError', 'Error', 'FIELD_TYPE', 'IntegrityError',
    'InterfaceError', 'InternalError', 'MySQLError', 'NULL', 'NUMBER',
    'NotSupportedError', 'DBAPISet', 'OperationalError', 'ProgrammingError',
    'ROWID', 'STRING', 'TIME', 'TIMESTAMP', 'Warning', 'apilevel',
    'connections', 'constants', 'converters', 'cursors',
    'escape_dict', 'escape_sequence', 'escape_string', 'get_client_info',
    'paramstyle', 'threadsafety', 'version_info',

    "NULL","__version__",
    ]
