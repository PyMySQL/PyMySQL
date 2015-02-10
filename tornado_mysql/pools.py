"""Connection pool"""
from __future__ import absolute_import, division, print_function

from collections import deque
import sys
import warnings

from tornado.ioloop import IOLoop
from tornado.gen import coroutine, Return
from tornado.concurrent import Future
from tornado_mysql import connect


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
                 io_loop=None,
                 ):
        """
        :param dict connect_kwargs: kwargs for tornado_mysql.connect()
        :param int max_idle_connections: Max number of keeping connections.
        :param int max_recycle_sec: How long connections are recycled.
        """
        connect_kwargs['autocommit'] = True
        self.io_loop = io_loop or IOLoop.current()
        self.connect_kwargs = connect_kwargs
        self.max_idle_connections = max_idle_connections
        self.max_recycle_sec = max_recycle_sec

        self._opened_conns = 0
        self._free_conn = deque()

    def _get_conn(self):
        now = self.io_loop.time()
        while self._free_conn:
            conn = self._free_conn.popleft()
            if now - conn.connected_time > self.max_recycle_sec:
                self._close_async(conn)
                continue
            _debug("Reusing connection from pool (opened=%d)" % (self._opened_conns,))
            fut = Future()
            fut.set_result(conn)
            return fut

        self._opened_conns += 1
        _debug("Creating new connection (opened=%d)" % (self._opened_conns,))
        return connect(**self.connect_kwargs)

    def _put_conn(self, conn):
        if (len(self._free_conn) < self.max_idle_connections and
                self.io_loop.time() - conn.connected_time < self.max_recycle_sec):
            self._free_conn.append(conn)
        else:
            self._close_async(conn)

    def _close_async(self, conn):
        self.io_loop.add_future(conn.close_async(), callback=lambda f: None)
        self._opened_conns -= 1

    def _close_conn(self, conn):
        conn.close()
        self._opened_conns -= 1

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
            self._put_conn(conn)
        except:
            self._opened_conns -= 1
            conn.close()
            raise
        raise Return(cur)

    @coroutine
    def begin(self):
        """Start transaction

        Wait to get connection and returns `Transaction` object.

        :return: Future[Transaction]
        :rtype: Future
        """
        conn = yield self._get_conn()
        yield conn.begin()
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
