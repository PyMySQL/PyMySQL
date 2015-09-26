import re
import datetime
import time
import sys
import decimal

from cymysql.constants import FIELD_TYPE
from cymysql.charset import charset_by_id

PYTHON3 = sys.version_info[0] > 2

ESCAPE_REGEX = re.compile(r"[\0\n\r\032\'\"\\]")
ESCAPE_MAP = {'\0': '\\0', '\n': '\\n', '\r': '\\r', '\032': '\\Z',
              '\'': '\\\'', '"': '\\"', '\\': '\\\\'}


def escape_dict(val, charset):
    return dict([(k, escape_item(v, charset)) for k, v in val.items()])

def escape_sequence(val, charset):
    return "(" + ",".join([escape_item(v, charset) for v in val]) + ")"

def escape_set(val, charset):
    return ",".join([escape_item(v, charset) for v in val])

def escape_bool(value):
    return str(int(value))

def escape_object(value):
    return str(value)

escape_int = escape_long = escape_object

def escape_float(value):
    return ('%.15g' % value)

def escape_string(value):
    return ("'%s'" % ESCAPE_REGEX.sub(
            lambda match: ESCAPE_MAP.get(match.group(0)), value))

def escape_bytes(value):
    if len(value)==0:
        return "''"
    return '0x' + ''.join([('0'+hex(c)[2:])[-2:] for c in value])

def escape_None(value):
    return 'NULL'

def escape_timedelta(obj):
    return "'%02d:%02d:%02d'" % (
        (obj.seconds // 3600) % 24 + obj.days * 24,     # hours
        (obj.seconds // 60) % 60,                       # minutes
        obj.seconds % 60                                # seconds
    )

def escape_time(obj):
    if obj.microsecond:
        return "'%02d:%02d:%02d.%f'" % (
            obj.hour, obj.minute, obj.second, obj.microsecond)
    else:
        return "'%02d:%02d:%02d'" % (obj.hour, obj.minute, obj.second)


def escape_datetime(obj):
    return "'%04d-%02d-%02d %02d:%02d:%02d'" % (
            obj.year, obj.month, obj.day, obj.hour, obj.minute, obj.second)

def escape_date(obj):
    return "'%04d-%02d-%02d'" % (obj.year, obj.month, obj.day)

def escape_struct_time(obj):
    return escape_datetime(datetime.datetime(*obj[:6]))

def escape_decimal(obj):
    return str(obj)

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
    if PYTHON3 and not isinstance(obj, str):
        obj = obj.decode('ascii')
    if ' ' in obj:
        sep = ' '
    elif 'T' in obj:
        sep = 'T'
    else:
        return convert_date(obj)

    try:
        ymd, hms = obj.split(sep, 1)
        if '.' in hms:
            hms, usecs = hms.split('.')
            return datetime.datetime(
                *[ int(x) for x in ymd.split('-')+hms.split(':')+[float('0.' + usecs) * 1e6] ])
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
    if PYTHON3 and not isinstance(obj, str):
        obj = obj.decode('ascii')
    try:
        microseconds = 0
        if "." in obj:
            (obj, tail) = obj.split('.')
            microseconds = int(tail)
        hours, minutes, seconds = obj.split(':')
        tdelta = datetime.timedelta(
            hours = int(hours),
            minutes = int(minutes),
            seconds = int(seconds),
            microseconds = microseconds
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
    try:
        microseconds = 0
        if "." in obj:
            (obj, tail) = obj.split('.')
            microseconds = int(tail)
        hours, minutes, seconds = obj.split(':')
        return datetime.time(hour=int(hours), minute=int(minutes),
                             second=int(seconds), microsecond=microseconds)
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
    if PYTHON3 and not isinstance(obj, str):
        obj = obj.decode('ascii')
    try:
        return datetime.date(*[ int(x) for x in obj.split('-', 2) ])
    except ValueError:
        return None

def convert_mysql_timestamp(obj):
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
    if PYTHON3 and not isinstance(obj, str):
        obj = obj.decode('ascii')
    if obj[4] == '-':
        return convert_datetime(obj)
    obj += "0"*(14-len(obj))    # padding
    try:
        return datetime.datetime(
            int(obj[:4]),       # year
            int(obj[4:6]),      # month
            int(obj[6:8]),      # day
            int(obj[8:10]),     # hour
            int(obj[10:12]),    # minute
            int(obj[12:14])     # second
        )
    except ValueError:
        return None

def convert_set(s):
    return set(s.split(","))

def convert_bit(b):
    # the snippet above is right, but MySQLdb doesn't process bits,
    # so we shouldn't either
    return b

def convert_characters(data, charset=None, field=None, use_unicode=None):
    if field.is_set:
        return convert_set(data.decode(field.charset))
    if field.is_binary:
        if PYTHON3 and field.charset != 'binary':
            return data.decode(field.charset)
        else:
            return data

    if use_unicode or PYTHON3:
        return data.decode(field.charset)
    elif charset != field.charset:
        return data.decode(field.charset).encode(charset)

def convert_decimal(obj):
    if PYTHON3 and not isinstance(obj, str):
        obj = obj.decode('ascii')
    return decimal.Decimal(obj)


decoders = {
        FIELD_TYPE.BIT: convert_bit,
        FIELD_TYPE.TINY: int,
        FIELD_TYPE.SHORT: int,
        FIELD_TYPE.LONG: int if PYTHON3 else long,
        FIELD_TYPE.FLOAT: float,
        FIELD_TYPE.DOUBLE: float,
        FIELD_TYPE.DECIMAL: float,
        FIELD_TYPE.NEWDECIMAL: float,
        FIELD_TYPE.LONGLONG: int if PYTHON3 else long,
        FIELD_TYPE.INT24: int,
        FIELD_TYPE.DECIMAL:  convert_decimal,
        FIELD_TYPE.NEWDECIMAL: convert_decimal,
        FIELD_TYPE.YEAR: int,
        FIELD_TYPE.TIMESTAMP: convert_mysql_timestamp,
        FIELD_TYPE.DATETIME: convert_datetime,
        FIELD_TYPE.TIME: convert_timedelta,
        FIELD_TYPE.DATE: convert_date,
        FIELD_TYPE.SET: convert_set,
        FIELD_TYPE.BLOB: convert_characters,
        FIELD_TYPE.TINY_BLOB: convert_characters,
        FIELD_TYPE.MEDIUM_BLOB: convert_characters,
        FIELD_TYPE.LONG_BLOB: convert_characters,
        FIELD_TYPE.STRING: convert_characters,
        FIELD_TYPE.VAR_STRING: convert_characters,
        FIELD_TYPE.VARCHAR: convert_characters,
        }

encoders = {
        bool: escape_bool,
        int: escape_int,
        float: escape_float,
        decimal.Decimal: escape_decimal,
        str: escape_string,
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

if PYTHON3:
    encoders[bytes] = escape_bytes
else:
    encoders[unicode] = escape_string
    encoders[long] = escape_long


def escape_item(val, charset, encoders=encoders):
    if type(val) in [tuple, list, set]:
        return escape_sequence(val, charset)
    if type(val) is dict:
        return escape_dict(val, charset)
    return encoders[type(val)](val)

