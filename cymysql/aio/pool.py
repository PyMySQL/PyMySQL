# based on aiomysql
# https://github.com/aio-libs/aiomysql/blob/master/aiomysql/pool.py

import asyncio
import collections
import warnings

from .connections import connect
from .context import (_PoolContextManager, _PoolConnectionContextManager,
                    _PoolAcquireContextManager)


def create_pool(minsize=1, maxsize=10, pool_recycle=-1,
                loop=None, **kwargs):
    coro = _create_pool(minsize=minsize, maxsize=maxsize,
                        pool_recycle=pool_recycle, loop=loop, **kwargs)
    return _PoolContextManager(coro)


async def _create_pool(minsize=1, maxsize=10, pool_recycle=-1,
                       loop=None, **kwargs):
    if loop is None:
        loop = asyncio.get_event_loop()

    pool = Pool(minsize=minsize, maxsize=maxsize,
                pool_recycle=pool_recycle, loop=loop, **kwargs)
    if minsize > 0:
        async with pool._cond:
            await pool._fill_free_pool(False)
    return pool


class Pool(asyncio.AbstractServer):
    """Connection pool"""

    def __init__(self, minsize, maxsize, pool_recycle, loop, **kwargs):
        if minsize < 0:
            raise ValueError("minsize should be zero or greater")
        if maxsize < minsize and maxsize != 0:
            raise ValueError("maxsize should be not less than minsize")
        self._minsize = minsize
        self._loop = loop
        self._conn_kwargs = kwargs
        self._acquiring = 0
        self._free = collections.deque(maxlen=maxsize or None)
        self._cond = asyncio.Condition()
        self._used = set()
        self._terminated = set()
        self._closing = False
        self._closed = False
        self._recycle = pool_recycle

    @property
    def minsize(self):
        return self._minsize

    @property
    def maxsize(self):
        return self._free.maxlen

    @property
    def size(self):
        return self.freesize + len(self._used) + self._acquiring

    @property
    def freesize(self):
        return len(self._free)

    async def clear(self):
        """Close all free connections in pool."""
        async with self._cond:
            while self._free:
                conn = self._free.popleft()
                await conn.ensure_closed()
            self._cond.notify()

    @property
    def closed(self):
        """
        The readonly property that returns ``True`` if connections is closed.
        """
        return self._closed

    def close(self):
        """Close pool.

        Mark all pool connections to be closed on getting back to pool.
        Closed pool doesn't allow to acquire new connections.
        """
        if self._closed:
            return
        self._closing = True

    def terminate(self):
        """Terminate pool.

        Close pool with instantly closing all acquired connections also.
        """

        self.close()

        for conn in list(self._used):
            conn.close()
            self._terminated.add(conn)

        self._used.clear()

    async def wait_closed(self):
        """Wait for closing all pool's connections."""

        if self._closed:
            return
        if not self._closing:
            raise RuntimeError(".wait_closed() should be called "
                               "after .close()")

        while self._free:
            conn = self._free.popleft()
            await conn.close()

        async with self._cond:
            while self.size > self.freesize:
                await self._cond.wait()

        self._closed = True

    def acquire(self):
        """Acquire free connection from the pool."""
        coro = self._acquire()
        return _PoolAcquireContextManager(coro, self)

    async def _acquire(self):
        if self._closing:
            raise RuntimeError("Cannot acquire connection after closing pool")
        async with self._cond:
            while True:
                await self._fill_free_pool(True)
                if self._free:
                    conn = self._free.popleft()
                    assert not conn.closed, conn
                    assert conn not in self._used, (conn, self._used)
                    self._used.add(conn)
                    return conn
                else:
                    await self._cond.wait()

    async def _fill_free_pool(self, override_min):
        # iterate over free connections and remove timed out ones
        free_size = len(self._free)
        n = 0
        while n < free_size:
            conn = self._free[-1]
            if (self._recycle > -1 and
                  self._loop.time() - conn.last_usage > self._recycle):
                self._free.pop()
                conn.close()
            else:
                self._free.rotate()
            n += 1

        while self.size < self.minsize:
            self._acquiring += 1
            try:
                conn = await connect(loop=self._loop,
                                     **self._conn_kwargs)
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1
        if self._free:
            return

        if override_min and (not self.maxsize or self.size < self.maxsize):
            self._acquiring += 1
            try:
                conn = await connect(loop=self._loop,
                                     **self._conn_kwargs)
                # raise exception if pool is closing
                self._free.append(conn)
                self._cond.notify()
            finally:
                self._acquiring -= 1

    async def _wakeup(self):
        async with self._cond:
            self._cond.notify()

    def release(self, conn):
        """Release free connection back to the connection pool.

        This is **NOT** a coroutine.
        """
        fut = self._loop.create_future()
        fut.set_result(None)

        if conn in self._terminated:
            assert conn.closed, conn
            self._terminated.remove(conn)
            return fut
        assert conn in self._used, (conn, self._used)
        self._used.remove(conn)
        if not conn.closed:
            in_trans = conn.get_transaction_status()
            if in_trans:
                conn.close()
                return fut
            if self._closing:
                conn.close()
            else:
                self._free.append(conn)
            fut = self._loop.create_task(self._wakeup())
        return fut

    def __enter__(self):
        raise RuntimeError(
            '"yield from" should be used as context manager expression')

    def __exit__(self, *args):
        # This must exist because __enter__ exists, even though that
        # always raises; that's how the with-statement works.
        pass  # pragma: nocover

    def __iter__(self):
        # This is not a coroutine.  It is meant to enable the idiom:
        #
        #     with (yield from pool) as conn:
        #         <block>
        #
        # as an alternative to:
        #
        #     conn = yield from pool.acquire()
        #     try:
        #         <block>
        #     finally:
        #         conn.release()
        conn = yield from self.acquire()
        return _PoolConnectionContextManager(self, conn)

    def __await__(self):
        msg = "with await pool as conn deprecated, use" \
              "async with pool.acquire() as conn instead"
        warnings.warn(msg, DeprecationWarning, stacklevel=2)
        conn = yield from self.acquire()
        return _PoolConnectionContextManager(self, conn)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self.close()
        await self.wait_closed()
