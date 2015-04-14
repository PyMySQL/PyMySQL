"""Connection pool"""
from __future__ import absolute_import, division, print_function

from collections import deque
import warnings

from tornado.ioloop import IOLoop
from tornado.gen import coroutine, Return
from tornado.concurrent import Future

from tornado_mysql import connect
from tornado_mysql.connections import Connection


DEBUG = False


def _debug(*msg):
    if DEBUG:
        print(*msg)


class Pool(object):
    """Connection pool like Golang's database/sql.DB.

    This connection pool is based on autocommit mode.
    You can execute query without knowing connection.

    When transaction is necessary, you can checkout transaction object.
    """

    def __init__(self,
                 connect_kwargs,
                 max_idle_connections=1,
                 max_recycle_sec=3600,
                 max_open_connections=0,
                 io_loop=None,
                 ):
        """
        :param dict connect_kwargs: kwargs for tornado_mysql.connect()
        :param int max_idle_connections: Max number of keeping connections.
        :param int max_recycle_sec: How long connections are recycled.
        :param int max_open_connections:
            Max number of opened connections. 0 means no limit.
        """
        connect_kwargs['autocommit'] = True
        self.io_loop = io_loop or IOLoop.current()
        self.connect_kwargs = connect_kwargs
        self.max_idle = max_idle_connections
        self.max_open = max_open_connections
        self.max_recycle_sec = max_recycle_sec

        self._opened_conns = 0
        self._free_conn = deque()
        self._waitings = deque()

    def stat(self):
        """Returns (opened connections, free connections, waiters)"""
        return (self._opened_conns, len(self._free_conn), len(self._waitings))

    def _get_conn(self):
        now = self.io_loop.time()

        # Try to reuse in free pool
        while self._free_conn:
            conn = self._free_conn.popleft()
            if now - conn.connected_time > self.max_recycle_sec:
                self._close_async(conn)
                continue
            _debug("Reusing connection from pool:", self.stat())
            fut = Future()
            fut.set_result(conn)
            return fut

        # Open new connection
        if self.max_open and self._opened_conns < self.max_open:
            self._opened_conns += 1
            _debug("Creating new connection:", self.stat())
            return connect(**self.connect_kwargs)

        # Wait to other connection is released.
        fut = Future()
        self._waitings.append(fut)
        return fut

    def _put_conn(self, conn):
        if (len(self._free_conn) < self.max_idle and
                self.io_loop.time() - conn.connected_time < self.max_recycle_sec):
            if self._waitings:
                fut = self._waitings.popleft()
                fut.set_result(conn)
                _debug("Passing returned connection to waiter:", self.stat())
            else:
                self._free_conn.append(conn)
                _debug("Add conn to free pool:", self.stat())
        else:
            self._close_async(conn)

    def _close_async(self, conn):
        self.io_loop.add_future(conn.close_async(), callback=self._after_close)

    def _close_conn(self, conn):
        conn.close()
        self._after_close()

    def _after_close(self, fut=None):
        if self._waitings:
            fut = self._waitings.popleft()
            conn = Connection(**self.connect_kwargs)
            cf = conn.connect()
            self.io_loop.add_future(cf, callback=lambda f: fut.set_result(conn))
        else:
            self._opened_conns -= 1
        _debug("Connection closed:", self.stat())

    @coroutine
    def execute(self, query, params=None):
        """Execute query in pool.

        Returns future yielding closed cursor.
        You can get rows, lastrowid, etc from the cursor.

        :return: Future of cursor
        :rtype: Future
        """
        conn = yield self._get_conn()
        try:
            cur = conn.cursor()
            yield cur.execute(query, params)
            yield cur.close()
        except:
            self._close_conn(conn)
            raise
        else:
            self._put_conn(conn)
        raise Return(cur)

    @coroutine
    def begin(self):
        """Start transaction

        Wait to get connection and returns `Transaction` object.

        :return: Future[Transaction]
        :rtype: Future
        """
        conn = yield self._get_conn()
        try:
            yield conn.begin()
        except:
            self._close_conn(conn)
            raise
        trx = Transaction(self, conn)
        raise Return(trx)


class Transaction(object):
    """Represents transaction in pool"""
    def __init__(self, pool, conn):
        self._pool = pool
        self._conn = conn

    def _ensure_conn(self):
        if self._conn is None:
            raise Exception("Transaction is closed already")

    def _close(self):
        self._pool._put_conn(self._conn)
        self._pool = self._conn = None

    @coroutine
    def execute(self, query, args):
        """
        :return: Future[Cursor]
        :rtype: Future
        """
        self._ensure_conn()
        cur = self._conn.cursor()
        yield cur.execute(query, args)
        raise Return(cur)

    @coroutine
    def commit(self):
        self._ensure_conn()
        yield self._conn.commit()
        self._close()

    @coroutine
    def rollback(self):
        self._ensure_conn()
        yield self._conn.rollback()
        self._close()

    def __del__(self):
        if self._pool is not None:
            warnings.warn("Transaction has not committed or rollbacked.")
            self._pool._close_conn(self._conn)
