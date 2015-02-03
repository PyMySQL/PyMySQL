# -*- coding: utf-8 -*-
from __future__ import print_function, absolute_import
import re
import warnings

from tornado import gen

from ._compat import range_type, text_type, PY2

from . import err


#: Regular expression for :meth:`Cursor.executemany`.
#: executemany only suports simple bulk insert.
#: You can use it to load large dataset.
RE_INSERT_VALUES = re.compile(r"""(INSERT\s.+\sVALUES\s+)(\(\s*%s\s*(?:,\s*%s\s*)*\))(\s*(?:ON DUPLICATE.*)?)\Z""",
                              re.IGNORECASE | re.DOTALL)


class Cursor(object):
    """Cursor is used to interact with the database."""

    #: Max stetement size which :meth:`executemany` generates.
    #:
    #: Max size of allowed statement is max_allowed_packet - packet_header_size.
    #: Default value of max_allowed_packet is 1048576.
    max_stmt_length = 1024000

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

    @gen.coroutine
    def close(self):
        '''
        Closing a cursor just exhausts all remaining data.
        '''
        conn = self.connection
        if conn is None:
            return
        try:
            while (yield self.nextset()):
                pass
        finally:
            self.connection = None

    def _get_db(self):
        if not self.connection:
            raise err.ProgrammingError("Cursor closed")
        return self.connection

    def _check_executed(self):
        if not self._executed:
            raise err.ProgrammingError("execute() first")

    def _conv_row(self, row):
        return row

    def setinputsizes(self, *args):
        """Does nothing, required by DB API."""

    def setoutputsizes(self, *args):
        """Does nothing, required by DB API."""

    @gen.coroutine
    def _nextset(self, unbuffered=False):
        conn = self._get_db()
        current_result = self._result
        if current_result is None or current_result is not conn._result:
            raise gen.Return()
        if not current_result.has_next:
            raise gen.Return()
        yield conn.next_result(unbuffered=unbuffered)
        yield self._do_get_result()
        raise gen.Return(True)

    @gen.coroutine
    def nextset(self):
        """Get the next query set"""
        res = yield self._nextset(False)
        raise gen.Return(res)

    def _escape_args(self, args, conn):
        if isinstance(args, (tuple, list)):
            return tuple(conn.escape(arg) for arg in args)
        elif isinstance(args, dict):
            return dict((key, conn.escape(val)) for (key, val) in args.items())
        else:
            #If it's not a dictionary let's try escaping it anyways.
            #Worst case it will throw a Value error
            return conn.escape(args)

    @gen.coroutine
    def execute(self, query, args=None):
        '''Execute a query'''
        conn = self._get_db()

        while (yield self.nextset()):
            pass

        if PY2:  # Use bytes on Python 2 always
            encoding = conn.encoding

            def ensure_bytes(x):
                if isinstance(x, unicode):
                    x = x.encode(encoding)
                return x

            query = ensure_bytes(query)

            if args is not None:
                if isinstance(args, (tuple, list)):
                    args = tuple(map(ensure_bytes, args))
                elif isinstance(args, dict):
                    args = dict((ensure_bytes(key), ensure_bytes(val)) for (key, val) in args.items())
                else:
                    args = ensure_bytes(args)

        if args is not None:
            query = query % self._escape_args(args, conn)

        yield self._query(query)
        self._executed = query
        raise gen.Return(self.rowcount)

    @gen.coroutine
    def executemany(self, query, args):
        """Run several data against one query

        PyMySQL can execute bulkinsert for query like 'INSERT ... VALUES (%s)'.
        In other form of queries, just run :meth:`execute` many times.
        """
        if not args:
            return

        m = RE_INSERT_VALUES.match(query)
        if m:
            q_prefix = m.group(1)
            q_values = m.group(2).rstrip()
            q_postfix = m.group(3) or ''
            assert q_values[0] == '(' and q_values[-1] == ')'
            yield self._do_execute_many(q_prefix, q_values, q_postfix, args,
                                        self.max_stmt_length,
                                        self._get_db().encoding)
        else:
            rows = 0
            for arg in args:
                yield self.execute(query, arg)
                rows += self.rowcount
            self.rowcount = rows
        raise gen.Return(self.rowcount)

    @gen.coroutine
    def _do_execute_many(self, prefix, values, postfix, args, max_stmt_length, encoding):
        conn = self._get_db()
        escape = self._escape_args
        if isinstance(prefix, text_type):
            prefix = prefix.encode(encoding)
        if isinstance(postfix, text_type):
            postfix = postfix.encode(encoding)
        sql = bytearray(prefix)
        args = iter(args)
        v = values % escape(next(args), conn)
        if isinstance(v, text_type):
            v = v.encode(encoding)
        sql += v
        rows = 0
        for arg in args:
            v = values % escape(arg, conn)
            if isinstance(v, text_type):
                v = v.encode(encoding)
            if len(sql) + len(v) + 1 > max_stmt_length:
                yield self.execute(bytes(sql + postfix))
                rows += self.rowcount
                sql = bytearray(prefix)
            else:
                sql += b','
            sql += v
        yield self.execute(bytes(sql + postfix))
        rows += self.rowcount
        self.rowcount = rows

    @gen.coroutine
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
            yield self._query(q)
            yield self.nextset()

        q = "CALL %s(%s)" % (procname,
                             ','.join(['@_%s_%d' % (procname, i)
                                       for i in range_type(len(args))]))
        yield self._query(q)
        self._executed = q
        yield gen.Return(args)

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
            return ()
        end = self.rownumber + (size or self.arraysize)
        result = self._rows[self.rownumber:end]
        self.rownumber = min(end, len(self._rows))
        return result

    def fetchall(self):
        ''' Fetch all the rows '''
        self._check_executed()
        if self._rows is None:
            return ()
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
            raise err.ProgrammingError("unknown scroll mode %s" % mode)

        if not (0 <= r < len(self._rows)):
            raise IndexError("out of range")
        self.rownumber = r

    @gen.coroutine
    def _query(self, q):
        conn = self._get_db()
        self._last_executed = q
        yield conn.query(q)
        yield self._do_get_result()

    @gen.coroutine
    def _do_get_result(self):
        conn = self._get_db()

        self.rownumber = 0
        self._result = result = conn._result

        self.rowcount = result.affected_rows
        self.description = result.description
        self.lastrowid = result.insert_id
        self._rows = result.rows

        if result.warning_count > 0:
            yield self._show_warnings(conn)

    @gen.coroutine
    def _show_warnings(self, conn):
        ws = yield conn.show_warnings()
        for w in ws:
            warnings.warn(w[-1], err.Warning, 4)

    def __iter__(self):
        return iter(self.fetchone, None)

    Warning = err.Warning
    Error = err.Error
    InterfaceError = err.InterfaceError
    DatabaseError = err.DatabaseError
    DataError = err.DataError
    OperationalError = err.OperationalError
    IntegrityError = err.IntegrityError
    InternalError = err.InternalError
    ProgrammingError = err.ProgrammingError
    NotSupportedError = err.NotSupportedError


class DictCursorMixin(object):
    # You can override this to use OrderedDict or other dict-like types.
    dict_type = dict

    @gen.coroutine
    def _do_get_result(self):
        yield super(DictCursorMixin, self)._do_get_result()
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

    @gen.coroutine
    def close(self):
        conn = self.connection
        if conn is None:
            return

        if self._result is not None and self._result is conn._result:
            yield self._result._finish_unbuffered_query()

        try:
            while (yield self.nextset()):
                pass
        finally:
            self.connection = None

    @gen.coroutine
    def _query(self, q):
        conn = self._get_db()
        self._last_executed = q
        yield conn.query(q, unbuffered=True)
        yield self._do_get_result()
        raise gen.Return(self.rowcount)

    @gen.coroutine
    def nextset(self):
        res = yield self._nextset(True)
        raise gen.Return(res)

    @gen.coroutine
    def read_next(self):
        """ Read next row """
        row = yield self._result._read_rowdata_packet_unbuffered()
        row = self._conv_row(row)
        raise gen.Return(row)

    @gen.coroutine
    def fetchone(self):
        """ Fetch next row """
        self._check_executed()
        row = yield self.read_next()
        if row is None:
            raise gen.Return(None)
        self.rownumber += 1
        raise gen.Return(row)

    @gen.coroutine
    def fetchall(self):
        """
        Fetch all, as per MySQLdb. Pretty useless for large queries, as
        it is buffered. See fetchall_unbuffered(), if you want an unbuffered
        generator version of this method.
        """
        rows = []
        while True:
            row = yield self.fetchone()
            if row is None:
                break
            rows.append(row)
        raise gen.Return(rows)

    @gen.coroutine
    def fetchmany(self, size=None):
        """ Fetch many """
        self._check_executed()
        if size is None:
            size = self.arraysize

        rows = []
        for i in range_type(size):
            row = yield self.read_next()
            if row is None:
                break
            rows.append(row)
            self.rownumber += 1
        raise gen.Return(rows)

    @gen.coroutine
    def scroll(self, value, mode='relative'):
        self._check_executed()

        if mode == 'relative':
            if value < 0:
                raise err.NotSupportedError(
                        "Backwards scrolling not supported by this cursor")

            for _ in range_type(value):
                yield self.read_next()
            self.rownumber += value
        elif mode == 'absolute':
            if value < self.rownumber:
                raise err.NotSupportedError(
                    "Backwards scrolling not supported by this cursor")

            end = value - self.rownumber
            for _ in range_type(end):
                yield self.read_next()
            self.rownumber = value
        else:
            raise err.ProgrammingError("unknown scroll mode %s" % mode)


class SSDictCursor(DictCursorMixin, SSCursor):
    """ An unbuffered cursor, which returns results as a dictionary """
