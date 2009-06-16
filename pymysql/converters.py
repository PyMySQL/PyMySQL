import re
import datetime
import array
from pymysql.times import Date, Time, TimeDelta, Timestamp
from pymysql.constants import FIELD_TYPE

ESCAPE_REGEX = re.compile(r"[\0\n\r\032\'\"\\]", re.IGNORECASE)

def escape_item(val):
    encorder = encoders[type(val)]
    return encorder(val)

def escape_dict(val):
    n = {}
    for k, v in val.items():
        quoted = escape_item(v)
        n[k] = quoted
    return n

def escape_sequence(val):
    n = []
    for item in val:
        quoted = escape_item(item)
        n.append(quoted)
    return tuple(n)

def escape_bool(value):
    return str(int(value))

def escape_object(value):
    return str(val)

def escape_float(value):
    return '%.15g' % value

def escape_sequence(value):
    return value

def escape_string(value):
    
    def rep(m):
        n = m.group(0)
        if n == "\0":
            return "\\0"
        elif n == "\n":
            return "\\n"
        elif n == "\r":
            return "\\r"
        elif n == "\032":
            return "\\Z"
        else:
            return "\\"+n
    s = re.sub(ESCAPE_REGEX, rep, value)
    return s


def escape_timedelta(obj):
    seconds = int(obj.seconds) % 60
    minutes = int(obj.seconds / 60) % 60
    hours = int(obj.seconds / 3600) % 24
    return '%d %02d:%02d:%02d' % (obj.days, hours, minutes, seconds)

def escape_datetime(obj):
    return obj.strftime("%Y-%m-%d %H:%M:%S")


def convert_datetime(obj):
    """Returns a DATETIME or TIMESTAMP column value as a datetime object:
    
      >>> datetime_or_None('2007-02-25 23:06:20')
      datetime.datetime(2007, 2, 25, 23, 6, 20)
      >>> datetime_or_None('2007-02-25T23:06:20')
      datetime.datetime(2007, 2, 25, 23, 6, 20)
    
    Illegal values are returned as None:
    
      >>> datetime_or_None('2007-02-31T23:06:20') is None
      True
      >>> datetime_or_None('0000-00-00 00:00:00') is None
      True
   
    """
    if ' ' in obj:
        sep = ' '
    elif 'T' in obj:
        sep = 'T'
    else:
        return date_or_None(obj)

    try:
        ymd, hms = obj.split(sep, 1)
        return datetime.datetime(*[ int(x) for x in ymd.split('-')+hms.split(':') ])
    except ValueError:
        return date_or_None(obj)

def convert_timedelta(obj):
    """Returns a TIME column as a timedelta object:

      >>> timedelta_or_None('25:06:17')
      datetime.timedelta(1, 3977)
      >>> timedelta_or_None('-25:06:17')
      datetime.timedelta(-2, 83177)
      
    Illegal values are returned as None:
    
      >>> timedelta_or_None('random crap') is None
      True
   
    Note that MySQL always returns TIME columns as (+|-)HH:MM:SS, but
    can accept values as (+|-)DD HH:MM:SS. The latter format will not
    be parsed correctly by this function.
    """
    from math import modf
    try:
        hours, minutes, seconds = obj.split(':')
        tdelta = datetime.timedelta(
            hours = int(hours),
            minutes = int(minutes),
            seconds = int(seconds),
            microseconds = int(modf(float(seconds))[0]*1000000),
            )
        if hours < 0:
            return -tdelta
        else:
            return tdelta
    except ValueError:
        return None

def convert_time(obj):
    """Returns a TIME column as a time object:

      >>> time_or_None('15:06:17')
      datetime.time(15, 6, 17)
      
    Illegal values are returned as None:
 
      >>> time_or_None('-25:06:17') is None
      True
      >>> time_or_None('random crap') is None
      True
   
    Note that MySQL always returns TIME columns as (+|-)HH:MM:SS, but
    can accept values as (+|-)DD HH:MM:SS. The latter format will not
    be parsed correctly by this function.
    
    Also note that MySQL's TIME column corresponds more closely to
    Python's timedelta and not time. However if you want TIME columns
    to be treated as time-of-day and not a time offset, then you can
    use set this function as the converter for FIELD_TYPE.TIME.
    """
    from math import modf
    try:
        hour, minute, second = obj.split(':')
        return datetime.time(hour=int(hour), minute=int(minute), second=int(second),
                    microsecond=int(modf(float(second))[0]*1000000))
    except ValueError:
        return None

def convert_date(obj):
    """Returns a DATE column as a date object:

      >>> date_or_None('2007-02-26')
      datetime.date(2007, 2, 26)
      
    Illegal values are returned as None:
 
      >>> date_or_None('2007-02-31') is None
      True
      >>> date_or_None('0000-00-00') is None
      True
    
    """
    try:
        return datetime.date(*[ int(x) for x in obj.split('-', 2) ])
    except ValueError:
        return None

def convert_mysql_timestamp(timestamp):
    """Convert a MySQL TIMESTAMP to a Timestamp object.

    MySQL >= 4.1 returns TIMESTAMP in the same format as DATETIME:
    
      >>> mysql_timestamp_converter('2007-02-25 22:32:17')
      datetime.datetime(2007, 2, 25, 22, 32, 17)
    
    MySQL < 4.1 uses a big string of numbers:
    
      >>> mysql_timestamp_converter('20070225223217')
      datetime.datetime(2007, 2, 25, 22, 32, 17)
    
    Illegal values are returned as None:
    
      >>> mysql_timestamp_converter('2007-02-31 22:32:17') is None
      True
      >>> mysql_timestamp_converter('00000000000000') is None
      True
      
    """
    if timestamp[4] == '-':
        return datetime_or_None(timestamp)
    timestamp += "0"*(14-len(timestamp)) # padding
    year, month, day, hour, minute, second = \
        int(timestamp[:4]), int(timestamp[4:6]), int(timestamp[6:8]), \
        int(timestamp[8:10]), int(timestamp[10:12]), int(timestamp[12:14])
    try:
        return datetime.datetime(year, month, day, hour, minute, second)
    except ValueError:
        return None

    
encoders = {
        bool: escape_bool,
        int: escape_object,
        long: escape_object,
        float: escape_float,
        str: escape_string,
        tuple: escape_sequence,
        list:escape_sequence,
        dict:escape_dict,
        datetime.datetime : escape_datetime,
        datetime.timedelta : escape_timedelta
        }

decoders = {
        FIELD_TYPE.TINY: int,
        FIELD_TYPE.SHORT: int,
        FIELD_TYPE.LONG: long,
        FIELD_TYPE.FLOAT: float,
        FIELD_TYPE.DOUBLE: float,
        FIELD_TYPE.DECIMAL: float,
        FIELD_TYPE.NEWDECIMAL: float,
        FIELD_TYPE.LONGLONG: long,
        FIELD_TYPE.INT24: int,
        FIELD_TYPE.YEAR: int,
        FIELD_TYPE.TIMESTAMP: convert_mysql_timestamp,
        FIELD_TYPE.DATETIME: convert_datetime,
        FIELD_TYPE.TIME: convert_timedelta,
        FIELD_TYPE.DATE: convert_date,
        FIELD_TYPE.BLOB: str,
        FIELD_TYPE.STRING: str,
        FIELD_TYPE.VAR_STRING: str,
        FIELD_TYPE.VARCHAR: str
        }

try:
    # python version > 2.3
    from decimal import Decimal
    decoders[FIELD_TYPE.DECIMAL] = Decimal
    decoders[FIELD_TYPE.NEWDECIMAL] = Decimal
except ImportError:
    pass
