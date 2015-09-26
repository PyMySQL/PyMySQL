# -*- coding: utf-8 -*-
import weakref
import struct
import sys

from cymysql.err import Warning, Error, InterfaceError, DataError, \
             DatabaseError, OperationalError, IntegrityError, InternalError, \
            NotSupportedError, ProgrammingError

PYTHON3 = sys.version_info[0] > 2

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
        self.arraysize = 1
        self._executed = None
        self.messages = []
        self._result = None

    def __del__(self):
        '''
        When this gets GC'd close it.
        '''
        self.close()

    def errorhandler(self, errorclass, errorvalue):
        if self.connection:
            self.connection.errorhandler(self, errorclass, errorvalue)
        else:
            raise errorclass(errorvalue)

    @property
    def rowcount(self):
        if self._result and self._result.affected_rows is not None:
            return self._result.affected_rows
        return -1

    @property
    def description(self):
        return self._result.description if self._result else None

    @property
    def lastrowid(self):
        return self._result.insert_id if self._result else None

    def close(self):
        '''
        Closing a cursor just exhausts all remaining data.
        '''
        if not self.connection:
            return
        try:
            while self.nextset():
                pass
        except:
            pass

        self.connection = None

    def _get_db(self):
        if not self.connection:
            self.errorhandler(ProgrammingError, (-1, "cursor closed"))
        return self.connection

    def _check_executed(self):
        if not self._executed:
            self.errorhandler(ProgrammingError, (-1, "execute() first"))

    def _flush(self):
        if self._result:
            self._result.read_rest_rowdata_packet()

    def setinputsizes(self, *args):
        """Does nothing, required by DB API."""

    def setoutputsizes(self, *args):
        """Does nothing, required by DB API."""

    def nextset(self):
        ''' Get the next query set '''
        if self._executed:
            self.fetchall()
        del self.messages[:]

        if not self._result or not self._result.has_next:
            return None
        connection = self._get_db()
        connection.next_result()
        self._do_get_result()
        return True

    def execute(self, query, args=None):
        ''' Execute a query '''
        from sys import exc_info

        conn = self._get_db()
        if hasattr(conn, '_last_execute_cursor') and not conn._last_execute_cursor() is None:
            conn._last_execute_cursor()._flush()

        charset = conn.charset
        del self.messages[:]

        if PYTHON3 and (not isinstance(query, str)):
            query = query.decode(charset)
        if (not PYTHON3) and isinstance(query, unicode):
            query = query.encode(charset)

        if args is not None:
            if isinstance(args, (tuple, list)):
                escaped_args = tuple(conn.escape(arg) for arg in args)
            elif isinstance(args, dict):
                escaped_args = dict((key, conn.escape(val)) for (key, val) in args.items())
            else:
                #If it's not a dictionary let's try escaping it anyways.
                #Worst case it will throw a Value error
                escaped_args = conn.escape(args)

            query = query % escaped_args

        try:
            self._query(query)
        except:
            exc, value, tb = exc_info()
            del tb
            self.messages.append((exc,value))
            self.errorhandler(exc, value)

        self._executed = query
        conn._last_execute_cursor = weakref.ref(self)

    def executemany(self, query, args):
        ''' Run several data against one query '''
        del self.messages[:]

        rowcount = 0
        for params in args:
            self.execute(query, params)
            if self.rowcount != -1:
                rowcount += self.rowcount
        self._result = None
        return rowcount

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
            if isinstance(q, unicode):
                q = q.encode(conn.charset)
            self._query(q)
            self.nextset()

        q = "CALL %s(%s)" % (procname,
                             ','.join(['@_%s_%d' % (procname, i)
                                       for i in range(len(args))]))
        if isinstance(q, unicode):
            q = q.encode(conn.charset)
        self._query(q)
        self._executed = q

        return args

    def fetchone(self):
        ''' Fetch the next row '''
        self._check_executed()
        if self._result is None:
            return None
        return self._result.fetchone()

    def fetchmany(self, size=None):
        ''' Fetch several rows '''
        self._check_executed()
        size = size or self.arraysize
        if self._result is None:
            return None
        result = []
        for i in range(size):
            r = self._result.fetchone()
            if not r:
                break
            result.append(r)
        return result

    def fetchall(self):
        ''' Fetch all the rows '''
        self._check_executed()
        if self._result is None:
            return None
        result = []

        r = self._result.fetchone()
        while r:
            result.append(r)
            r = self._result.fetchone()

        return result

    def _query(self, q):
        conn = self._get_db()
        self._last_executed = q
        conn.query(q)
        self._do_get_result()

    def _do_get_result(self):
        conn = self._get_db()
        self._result = conn._result

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

class DictCursor(Cursor):
    """A cursor which returns results as a dictionary"""

    def execute(self, query, args=None):
        result = super(DictCursor, self).execute(query, args)
        if self.description:
            self._fields = [ field[0] for field in self.description ]
        return result

    def fetchone(self):
        ''' Fetch the next row '''
        self._check_executed()
        if self._result is None:
            return None
        r = super(DictCursor, self).fetchone()
        if not r:
            return None
        return  dict(zip(self._fields, r))

    def fetchmany(self, size=None):
        ''' Fetch several rows '''
        self._check_executed()
        if self._result is None:
            return None
        result = [ dict(zip(self._fields, r)) for r in super(DictCursor, self).fetchmany(size)]
        return tuple(result)

    def fetchall(self):
        ''' Fetch all the rows '''
        self._check_executed()
        if self._result is None:
            return None
        result = [ dict(zip(self._fields, r)) for r in super(DictCursor, self).fetchall() ]
        return tuple(result)

