VERSION = (0, 2, None)

from pymysql.exceptions import Warning, Error, InterfaceError, DataError, \
     DatabaseError, OperationalError, IntegrityError, InternalError, \
     NotSupportedError, ProgrammingError
from pymysql.constants import FIELD_TYPE
from pymysql.times import Date, Time, Timestamp, \
    DateFromTicks, TimeFromTicks, TimestampFromTicks

from sets import ImmutableSet

threadsafety = 1
apilevel = "2.0"
paramstyle = "format"

class DBAPISet(ImmutableSet):


    def __ne__(self, other):
        from sets import BaseSet
        if isinstance(other, BaseSet):
            return super(DBAPISet, self).__ne__(self, other)
        else:
            return other not in self

    def __eq__(self, other):
        from sets import BaseSet
        if isinstance(other, BaseSet):
            return super(DBAPISet, self).__eq__(self, other)
        else:
            return other in self


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
    return str(x)

def Connect(*args, **kwargs):
    from connections import Connection
    return Connection(*args, **kwargs)

connect = Connection = Connect


__all__ = [
    'BINARY', 'Binary', 'Connect', 'Connection', 'DATE', 'Date',
    'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks', 'TimestampFromTicks',
    'DataError', 'DatabaseError', 'Error', 'FIELD_TYPE', 'IntegrityError',
    'InterfaceError', 'InternalError', 'MySQLError', 'NULL', 'NUMBER',
    'NotSupportedError', 'DBAPISet', 'OperationalError', 'ProgrammingError',
    'ROWID', 'STRING', 'TIME', 'TIMESTAMP', 'Warning', 'apilevel', 'connect',
    'connections', 'constants', 'converters', 'cursors', 'debug', 'escape',
    'escape_dict', 'escape_sequence', 'escape_string', 'get_client_info',
    'paramstyle', 'string_literal', 'threadsafety', 'version_info',
    ]
