import re
import datetime
import time
import sys
from decimal import Decimal

from cymysql.constants import FIELD_TYPE, FLAG
try:
    from cymysql.charsetx import charset_by_id
except ImportError:
    from cymysql.charset import charset_by_id

PYTHON3 = sys.version_info[0] > 2

ESCAPE_REGEX = re.compile(r"[\0\n\r\032\'\"\\]")
ESCAPE_MAP = {'\0': '\\0', '\n': '\\n', '\r': '\\r', '\032': '\\Z',
              '\'': '\\\'', '"': '\\"', '\\': '\\\\'}

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
    return "(" + ",".join(n) + ")"

def escape_set(val, charset):
    val = map(lambda x: escape_item(x, charset), val)
    return ','.join(val)

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

def escape_unicode(value):
    return escape_string(value)

def escape_None(value):
    return 'NULL'

def escape_timedelta(obj):
    seconds = int(obj.seconds) % 60
    minutes = int(obj.seconds // 60) % 60
    hours = int(obj.seconds // 3600) % 24 + int(obj.days) * 24
    return escape_string('%02d:%02d:%02d' % (hours, minutes, seconds))

def escape_time(obj):
    s = "%02d:%02d:%02d" % (int(obj.hour), int(obj.minute),
                            int(obj.second))
    if obj.microsecond:
        s += ".%f" % obj.microsecond

    return escape_string(s)

def escape_datetime(obj):
    return escape_string("%04d-%02d-%02d %02d:%02d:%02d" % (
            obj.year, obj.month, obj.day, obj.hour, obj.minute, obj.second))

def escape_date(obj):
    return escape_string("%04d-%02d-%02d" % (obj.year, obj.month, obj.day))

def escape_struct_time(obj):
    return escape_datetime(datetime.datetime(*obj[:6]))

def escape_decimal(obj):
    return str(obj)

def convert_datetime(obj, charset='ascii', field=None, use_unicode=None):
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
    if ((PYTHON3 and not isinstance(obj, str)) or 
        (not PYTHON3 and not isinstance(obj, unicode))):
        obj = obj.decode(charset)
    if ' ' in obj:
        sep = ' '
    elif 'T' in obj:
        sep = 'T'
    else:
        return convert_date(obj, charset, field, use_unicode)

    try:
        ymd, hms = obj.split(sep, 1)
        return datetime.datetime(*[ int(x) for x in ymd.split('-')+hms.split(':') ])
    except ValueError:
        return convert_date(obj, charset, field, use_unicode)

def convert_timedelta(obj, charset=None, field=None, use_unicode=None):
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
    try:
        microseconds = 0
        if ((PYTHON3 and not isinstance(obj, str)) or 
            (not PYTHON3 and not isinstance(obj, unicode))):
            obj = obj.decode(charset)
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

def convert_time(obj, charset=None, field=None, use_unicode=None):
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

def convert_date(obj, charset=None, field=None, use_unicode=None):
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
        if ((PYTHON3 and not isinstance(obj, str)) or
            (not PYTHON3 and not isinstance(obj, unicode))):
            obj = obj.decode(charset)
        return datetime.date(*[ int(x) for x in obj.split('-', 2) ])
    except ValueError:
        return None

def convert_mysql_timestamp(timestamp, charset=None, field=None, use_unicode=None):
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
    if ((PYTHON3 and not isinstance(timestamp, str)) or
        (not PYTHON3 and not isinstance(timestamp, unicode))):
        timestamp = timestamp.decode(charset)

    if timestamp[4] == '-':
        return convert_datetime(timestamp, charset, field, use_unicode)
    timestamp += "0"*(14-len(timestamp)) # padding
    year, month, day, hour, minute, second = \
        int(timestamp[:4]), int(timestamp[4:6]), int(timestamp[6:8]), \
        int(timestamp[8:10]), int(timestamp[10:12]), int(timestamp[12:14])
    try:
        return datetime.datetime(year, month, day, hour, minute, second)
    except ValueError:
        return None

def convert_set(s):
    return set(s.split(","))

def convert_bit(b, charset=None, field=None, use_unicode=None):
    #b = "\x00" * (8 - len(b)) + b # pad w/ zeroes
    #return struct.unpack(">Q", b)[0]
    #
    # the snippet above is right, but MySQLdb doesn't process bits,
    # so we shouldn't either
    return b

def convert_blob(data, charset=None, field=None, use_unicode=None):
    return convert_characters(data, charset, field, use_unicode)

def convert_characters(data, charset=None, field=None, use_unicode=None):
    field_charset = charset_by_id(field.charsetnr).name
    if field.flags & FLAG.SET:
        return convert_set(data.decode(field_charset))
    if field.flags & FLAG.BINARY:
        if PYTHON3 and field_charset != 'binary':
            return data.decode(field_charset)
        else:
            return data

    if use_unicode or PYTHON3:
        data = data.decode(field_charset)
    elif charset != field_charset:
        data = data.decode(field_charset)
        data = data.encode(charset)
    return data

def convert_int(data, charset=None, field=None, use_unicode=None):
    return int(data)

def convert_long(data, charset=None, field=None, use_unicode=None):
    if PYTHON3:
        return int(data)
    else:
        return long(data)

def convert_float(data, charset=None, field=None, use_unicode=None):
    return float(data)

def convert_decimal(data, charset=None, field=None, use_unicode=None):
    data = data.decode(charset)
    return Decimal(data)

decoders = {
        FIELD_TYPE.BIT: convert_bit,
        FIELD_TYPE.TINY: convert_int,
        FIELD_TYPE.SHORT: convert_int,
        FIELD_TYPE.LONG: convert_long,
        FIELD_TYPE.FLOAT: convert_float,
        FIELD_TYPE.DOUBLE: convert_float,
        FIELD_TYPE.DECIMAL: convert_float,
        FIELD_TYPE.NEWDECIMAL: convert_float,
        FIELD_TYPE.LONGLONG: convert_long,
        FIELD_TYPE.INT24: convert_int,
        FIELD_TYPE.DECIMAL:  convert_decimal,
        FIELD_TYPE.NEWDECIMAL: convert_decimal,
        FIELD_TYPE.YEAR: convert_int,
        FIELD_TYPE.TIMESTAMP: convert_mysql_timestamp,
        FIELD_TYPE.DATETIME: convert_datetime,
        FIELD_TYPE.TIME: convert_timedelta,
        FIELD_TYPE.DATE: convert_date,
        FIELD_TYPE.SET: convert_set,
        FIELD_TYPE.BLOB: convert_blob,
        FIELD_TYPE.TINY_BLOB: convert_blob,
        FIELD_TYPE.MEDIUM_BLOB: convert_blob,
        FIELD_TYPE.LONG_BLOB: convert_blob,
        FIELD_TYPE.STRING: convert_characters,
        FIELD_TYPE.VAR_STRING: convert_characters,
        FIELD_TYPE.VARCHAR: convert_characters,
        }

encoders = {
        bool: escape_bool,
        int: escape_int,
        float: escape_float,
        Decimal: escape_decimal,
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
    encoders[unicode] = escape_unicode
    encoders[long] = escape_long


def escape_item(val, charset, encoders=encoders):
    if type(val) in [tuple, list, set]:
        return escape_sequence(val, charset)
    if type(val) is dict:
        return escape_dict(val, charset)
    encoder = encoders[type(val)]
    val = encoder(val)
    if type(val) is str:
        return val
    val = val.encode(charset)
    return val

