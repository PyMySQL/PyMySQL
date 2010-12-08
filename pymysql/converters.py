import re
import datetime
import time
import array
import struct

from times import Date, Time, TimeDelta, Timestamp
from constants import FIELD_TYPE

try:
    set
except NameError:
    try:
        from sets import BaseSet as set
    except ImportError:
        from sets import Set as set

ESCAPE_REGEX = re.compile(r"[\0\n\r\032\'\"\\]")
ESCAPE_MAP = {'\0': '\\0', '\n': '\\n', '\r': '\\r', '\032': '\\Z',
              '\'': '\\\'', '"': '\\"', '\\': '\\\\'}

def escape_item(val, charset):
    encoder = encoders[type(val)]
    return encoder(val, charset)

def escape_dict(val, charset):
    n = {}
    for k, v in val.items():
        quoted = escape_item(v, charset)
        n[k] = quoted
    return n

def escape_sequence(val, charset):
    n = []
    for item in val:
        quoted = escape_item(item, charset)
        n.append(quoted)
    return tuple(n)

def escape_bool(value, charset):
    return str(int(value)).encode(charset)

def escape_object(value, charset):
    return str(value).encode(charset)

escape_int = escape_long = escape_object

def escape_float(value, charset):
    return ('%.15g' % value).encode(charset)

def escape_string(value, charset):
    r = ("'%s'" % ESCAPE_REGEX.sub(
        lambda match: ESCAPE_MAP.get(match.group(0)), value))
    # TODO: make sure that encodings are handled correctly here.
    # Since we may be dealing with binary data, the encoding
    # routine below is commented out.
    #if not charset is None:
    #    r = r.encode(charset)
    return r
    
def escape_unicode(value, charset):
    # pass None as the charset because we already encode it
    return escape_string(value.encode(charset), None)

def escape_None(value, charset):
    return 'NULL'.encode(charset)

def escape_timedelta(obj, charset):
    seconds = int(obj.seconds) % 60
    minutes = int(obj.seconds // 60) % 60
    hours = int(obj.seconds // 3600) % 24 + int(obj.days) * 24
    return escape_string('%02d:%02d:%02d' % (hours, minutes, seconds), charset)

def escape_time(obj, charset):
    s = "%02d:%02d:%02d" % (int(obj.hour), int(obj.minute),
                            int(obj.second))
    if obj.microsecond:
        s += ".%f" % obj.microsecond

    return escape_string(s, charset)

def escape_datetime(obj, charset):
    return escape_string(obj.strftime("%Y-%m-%d %H:%M:%S"), charset)

def escape_date(obj, charset):
    return escape_string(obj.strftime("%Y-%m-%d"), charset)

def escape_struct_time(obj, charset):
    return escape_datetime(datetime.datetime(*obj[:6]), charset)

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
        return convert_date(obj)

    try:
        ymd, hms = obj.split(sep, 1)
        return datetime.datetime(*[ int(x) for x in ymd.split('-')+hms.split(':') ])
    except ValueError:
        return convert_date(obj)

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
        hours, minutes, seconds = tuple([int(x) for x in obj.split(':')])
        tdelta = datetime.timedelta(
            hours = int(hours),
            minutes = int(minutes),
            seconds = int(seconds),
            microseconds = int(modf(float(seconds))[0]*1000000),
            )
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
        return convert_datetime(timestamp)
    timestamp += "0"*(14-len(timestamp)) # padding
    year, month, day, hour, minute, second = \
        int(timestamp[:4]), int(timestamp[4:6]), int(timestamp[6:8]), \
        int(timestamp[8:10]), int(timestamp[10:12]), int(timestamp[12:14])
    try:
        return datetime.datetime(year, month, day, hour, minute, second)
    except ValueError:
        return None

def convert_set(s):
    # TODO: this may not be correct
    return set(s.split(","))

def convert_bit(b):
    b = "\x00" * (8 - len(b)) + b # pad w/ zeroes
    return struct.unpack(">Q", b)[0]
    
encoders = {
        bool: escape_bool,
        int: escape_int,
        long: escape_long,
        float: escape_float,
        str: escape_string,
        unicode: escape_unicode,
        tuple: escape_sequence,
        list:escape_sequence,
        set:escape_sequence,
        dict:escape_dict,
        type(None):escape_None,
        datetime.date: escape_date,
        datetime.datetime : escape_datetime,
        datetime.timedelta : escape_timedelta,
        datetime.time : escape_time,
        time.struct_time : escape_struct_time,
        }

decoders = {
        FIELD_TYPE.BIT: convert_bit,
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
        FIELD_TYPE.SET: convert_set,
        #FIELD_TYPE.BLOB: str,
        #FIELD_TYPE.STRING: str,
        #FIELD_TYPE.VAR_STRING: str,
        #FIELD_TYPE.VARCHAR: str
        }
conversions = decoders  # for MySQLdb compatibility

def decode_characters(connection, field, data):
    if field.charsetnr == 63 or not connection.use_unicode:
        # binary data, leave it alone
        return data
    return data.decode(connection.charset)

# These take a field instance rather than just the data.
field_decoders = {
    FIELD_TYPE.BLOB: decode_characters,
    FIELD_TYPE.TINY_BLOB: decode_characters,
    FIELD_TYPE.MEDIUM_BLOB: decode_characters,
    FIELD_TYPE.LONG_BLOB: decode_characters,
    FIELD_TYPE.STRING: decode_characters,
    FIELD_TYPE.VAR_STRING: decode_characters,
    FIELD_TYPE.VARCHAR: decode_characters,
}

try:
    # python version > 2.3
    from decimal import Decimal
    decoders[FIELD_TYPE.DECIMAL] = Decimal
    decoders[FIELD_TYPE.NEWDECIMAL] = Decimal

    def escape_decimal(obj, charset):
        return unicode(obj).encode(charset)
    encoders[Decimal] = escape_decimal

except ImportError:
    pass
