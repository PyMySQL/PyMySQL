# -*- coding: utf-8 -*-
from __future__ import print_function, absolute_import
import re

from ._compat import range_type, text_type
from .charset import charset_by_name, charset_to_encoding
from .err import (
    Warning, Error, InterfaceError, DataError,
    DatabaseError, OperationalError, IntegrityError, InternalError,
    NotSupportedError, ProgrammingError)

insert_values = re.compile(r'\svalues\s*(\(.+\))', re.IGNORECASE)


def _escape(conn, value, has_binary=False):
    result = conn.escape(value)
    if has_binary:
        # Make sure we don't mix bytes and unicode
        if isinstance(result, text_type):
            result = result.encode(conn.encoding)
    return result


class Cursor(object):
    '''
    This is the object you use to interact with the database.
    '''
    def __init__(self, connection):
        '''
        Do not create an instance of a Cursor yourself. Call
        connections.Connection.cursor().
        '''
        self.connection = connection
        self.description = None
        self.rownumber = 0
        self.rowcount = -1
        self.arraysize = 1
        self._executed = None
        self._result = None
        self._rows = None

    def __del__(self):
        '''
        When this gets GC'd close it.
        '''
        self.close()

    def close(self):
        '''
        Closing a cursor just exhausts all remaining data.
        '''
        conn = self.connection
        if conn is None:
            return
        try:
            while self.nextset():
                pass
        finally:
            self.connection = None

    def _get_db(self):
        if not self.connection:
            raise ProgrammingError("Cursor closed")
        return self.connection

    def _check_executed(self):
        if not self._executed:
            raise ProgrammingError("execute() first")

    def _conv_row(self, row):
        return row

    def setinputsizes(self, *args):
        """Does nothing, required by DB API."""

    def setoutputsizes(self, *args):
        """Does nothing, required by DB API."""

    def nextset(self):
        ''' Get the next query set '''
        conn = self._get_db()
        current_result = self._result
        if current_result is None or current_result is not conn._result:
            return None
        if not current_result.has_next:
            return None
        conn.next_result()
        self._do_get_result()
        return True

    def execute(self, query, args=None):
        ''' Execute a query '''
        conn = self._get_db()

        while self.nextset():
            pass

        # TODO: make sure that conn.escape is correct

        if args is not None:

            is_binary = conn.use_unicode and not charset_by_name(conn.charset).is_binary

            if isinstance(args, (tuple, list)):
                escaped_args = tuple(_escape(conn, arg, is_binary) for arg in args)
            elif isinstance(args, dict):
                escaped_args = dict((key, _escape(conn, val, is_binary)) for (key, val) in args.items())
            else:
                #If it's not a dictionary let's try escaping it anyways.
                #Worst case it will throw a Value error
                escaped_args = _escape(conn, args, is_binary)

            query = query % escaped_args

        result = self._query(query)
        self._executed = query
        return result

    def executemany(self, query, args):
        ''' Run several data against one query '''
        if not args:
            return

        self.rowcount = sum(self.execute(query, arg) for arg in args)
        return self.rowcount

    def callproc(self, procname, args=()):
        """Execute stored procedure procname with args

        procname -- string, name of procedure to execute on server

        args -- Sequence of parameters to use with procedure

        Returns the original args.

        Compatibility warning: PEP-249 specifies that any modified
        parameters must be returned. This is currently impossible
        as they are only available by storing them in a server
        variable and then retrieved by a query. Since stored
        procedures return zero or more result sets, there is no
        reliable way to get at OUT or INOUT parameters via callproc.
        The server variables are named @_procname_n, where procname
        is the parameter above and n is the position of the parameter
        (from zero). Once all result sets generated by the procedure
        have been fetched, you can issue a SELECT @_procname_0, ...
        query using .execute() to get any OUT or INOUT values.

        Compatibility warning: The act of calling a stored procedure
        itself creates an empty result set. This appears after any
        result sets generated by the procedure. This is non-standard
        behavior with respect to the DB-API. Be sure to use nextset()
        to advance through all result sets; otherwise you may get
        disconnected.
        """
        conn = self._get_db()
        for index, arg in enumerate(args):
            q = "SET @_%s_%d=%s" % (procname, index, conn.escape(arg))
            self._query(q)
            self.nextset()

        q = "CALL %s(%s)" % (procname,
                             ','.join(['@_%s_%d' % (procname, i)
                                       for i in range_type(len(args))]))
        self._query(q)
        self._executed = q
        return args

    def fetchone(self):
        ''' Fetch the next row '''
        self._check_executed()
        if self._rows is None or self.rownumber >= len(self._rows):
            return None
        result = self._rows[self.rownumber]
        self.rownumber += 1
        return result

    def fetchmany(self, size=None):
        ''' Fetch several rows '''
        self._check_executed()
        if self._rows is None:
            return None
        end = self.rownumber + (size or self.arraysize)
        result = self._rows[self.rownumber:end]
        self.rownumber = min(end, len(self._rows))
        return result

    def fetchall(self):
        ''' Fetch all the rows '''
        self._check_executed()
        if self._rows is None:
            return None
        if self.rownumber:
            result = self._rows[self.rownumber:]
        else:
            result = self._rows
        self.rownumber = len(self._rows)
        return result

    def scroll(self, value, mode='relative'):
        self._check_executed()
        if mode == 'relative':
            r = self.rownumber + value
        elif mode == 'absolute':
            r = value
        else:
            raise ProgrammingError("unknown scroll mode %s" % mode)

        if not (0 <= r < len(self._rows)):
            raise IndexError("out of range")
        self.rownumber = r

    def _query(self, q):
        conn = self._get_db()
        self._last_executed = q
        conn.query(q)
        self._do_get_result()
        return self.rowcount

    def _do_get_result(self):
        conn = self._get_db()

        self.rownumber = 0
        self._result = result = conn._result

        self.rowcount = result.affected_rows
        self.description = result.description
        self.lastrowid = result.insert_id
        self._rows = result.rows

    def __iter__(self):
        return iter(self.fetchone, None)

    Warning = Warning
    Error = Error
    InterfaceError = InterfaceError
    DatabaseError = DatabaseError
    DataError = DataError
    OperationalError = OperationalError
    IntegrityError = IntegrityError
    InternalError = InternalError
    ProgrammingError = ProgrammingError
    NotSupportedError = NotSupportedError


class DictCursorMixin(object):
    # You can override this to use OrderedDict or other dict-like types.
    dict_type = dict

    def _do_get_result(self):
        super(DictCursorMixin, self)._do_get_result()
        fields = []
        if self.description:
            for f in self._result.fields:
                name = f.name
                if name in fields:
                    name = f.table_name + '.' + name
                fields.append(name)
            self._fields = fields

        if fields and self._rows:
            self._rows = [self._conv_row(r) for r in self._rows]

    def _conv_row(self, row):
        if row is None:
            return None
        return self.dict_type(zip(self._fields, row))


class DictCursor(DictCursorMixin, Cursor):
    """A cursor which returns results as a dictionary"""


class SSCursor(Cursor):
    """
    Unbuffered Cursor, mainly useful for queries that return a lot of data,
    or for connections to remote servers over a slow network.

    Instead of copying every row of data into a buffer, this will fetch
    rows as needed. The upside of this, is the client uses much less memory,
    and rows are returned much faster when traveling over a slow network,
    or if the result set is very big.

    There are limitations, though. The MySQL protocol doesn't support
    returning the total number of rows, so the only way to tell how many rows
    there are is to iterate over every row returned. Also, it currently isn't
    possible to scroll backwards, as only the current row is held in memory.
    """

    def _conv_row(self, row):
        return row

    def close(self):
        conn = self.connection
        if conn is None:
            return

        if self._result is not None and self._result is conn._result:
            self._result._finish_unbuffered_query()

        try:
            while self.nextset():
                pass
        finally:
            self.connection = None

    def _query(self, q):
        conn = self._get_db()
        self._last_executed = q
        conn.query(q, unbuffered=True)
        self._do_get_result()
        return self.rowcount

    def read_next(self):
        """ Read next row """
        return self._conv_row(self._result._read_rowdata_packet_unbuffered())

    def fetchone(self):
        """ Fetch next row """
        self._check_executed()
        row = self.read_next()
        if row is None:
            return None
        self.rownumber += 1
        return row

    def fetchall(self):
        """
        Fetch all, as per MySQLdb. Pretty useless for large queries, as
        it is buffered. See fetchall_unbuffered(), if you want an unbuffered
        generator version of this method.

        """
        return list(self.fetchall_unbuffered())

    def fetchall_unbuffered(self):
        """
        Fetch all, implemented as a generator, which isn't to standard,
        however, it doesn't make sense to return everything in a list, as that
        would use ridiculous memory for large result sets.
        """
        return iter(self.fetchone, None)

    def __iter__(self):
        return self.fetchall_unbuffered()

    def fetchmany(self, size=None):
        """ Fetch many """

        self._check_executed()
        if size is None:
            size = self.arraysize

        rows = []
        for i in range_type(size):
            row = self.read_next()
            if row is None:
                break
            rows.append(row)
            self.rownumber += 1
        return rows

    def scroll(self, value, mode='relative'):
        self._check_executed()

        if mode == 'relative':
            if value < 0:
                raise NotSupportedError(
                        "Backwards scrolling not supported by this cursor")

            for _ in range_type(value):
                self.read_next()
            self.rownumber += value
        elif mode == 'absolute':
            if value < self.rownumber:
                raise NotSupportedError(
                    "Backwards scrolling not supported by this cursor")

            end = value - self.rownumber
            for _ in range_type(end):
                self.read_next()
            self.rownumber = value
        else:
            raise ProgrammingError("unknown scroll mode %s" % mode)


class SSDictCursor(DictCursorMixin, SSCursor):
    """ An unbuffered cursor, which returns results as a dictionary """
