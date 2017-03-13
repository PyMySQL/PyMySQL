.. _conversion:

===============
Data Conversion
===============

By default PyMySQL will convert MySQL data types into native Python types. The
full list of conversions is:

=============== =============================================
MySQL type      Python type
=============== =============================================
``BIT``         [No conversion]
``TINY``        ``Int``
``SHORT``       ``Int``
``LONG``        ``Int``
``FLOAT``       ``Float``
``DOUBLE``      ``Float``
``LONGLONG``    ``Int``
``INT24``       ``Int``
``YEAR``        ``Int``
``TIMESTAMP``   ``datetime.datetime`` or ``datetime.date`` *
``DATETIME``    ``datetime.datetime`` *
``TIME``        ``datetime.timedelta`` *
``DATE``        ``datetime.date`` *
``SET``         ``Set``
``BLOB``        ``String``
``TINY_BLOB``   ``String``
``MEDIUM_BLOB`` ``String``
``LONG_BLOB``   ``String``
``STRING``      ``String``
``VAR_STRING``  ``String``
``VARCHAR``     ``String``
``DECIMAL``     ``Decimal``
``NEWDECIMAL``  ``Decimal``
=============== =============================================

Items marked * are returned as ``None`` if they are of an invalid format or are
invalid dates. For example, these valid ``DATETIME`` values::

    '2007-02-25 23:06:20'
    '2007-02-25T23:06:20'

would both be converted to the following Python object::

    datetime.datetime(2007, 2, 25, 23, 6, 20)

But these ``DATETIME`` values would both be converted to ``None``::

    '2007-02-31T23:06:20'  # There is no 31st February.
    '0000-00-00 00:00:00'

This can mean that values accepted as valid by MySQL are converted to
``None``. For example ``DATE`` values such as these can be valid in MySQL, but
will be converted to ``None`` by PyMySQL::

   '2017-02-00'
   '2017-00-00'
   '0000-00-00'

See `this StackOverflow answer <http://stackoverflow.com/a/6882884/250962>`_ for
more detail on MySQL's handling of dates/times like these.

If you know your database contains such dates you may want to prevent PyMSQL
converting them (see below).

Preventing conversion
---------------------

PyMySQL's automatic type conversion can be altered when establishing the
initial connection to the database.

For example, to prevent the conversion of date and time fields into native
Python ``datetime`` types, you would delete the appropriate MySQL types from
the dict of converters like this::

    from pymysql.constants import FIELD_TYPE
    from pymysql.converters import conversions as conv

    conv = conv.copy()

    del conv[FIELD_TYPE.DATE]
    del conv[FIELD_TYPE.DATETIME]
    del conv[FIELD_TYPE.TIME]

    conn = pymysql.connect(..., conv=conv)

i.e., connect as you would normally but pass in a modified dict of field types
and conversions.

The field types deleted will be returned as Strings, rather than being
converted into any other native Python types.

See the table above for the possible MySQL field types, or the ``decoders``
dict in `pymysql/converters.py <https://github.com/PyMySQL/PyMySQL/blob/master/pymysql/converters.py>`_.


