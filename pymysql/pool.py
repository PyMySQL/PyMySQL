import threading
import time
from collections import deque

from . import connections, err
from .constants import COMMAND


class PoolError(Exception):
    """Pool Error"""


class CreateConnectionError(PoolError):
    """Create New Connection Error"""


class GetConnectionError(PoolError):
    """Get connection exception from connection pool"""


class ReturnConnectionError(PoolError):
    """Connection cannot return connection pool exception"""


class Connection(connections.Connection):
    """
    OverWrite the Connection class of `pymysql.connections.Connection`

    Create a connection object with or without connection pool properties,
    which is the same as the instance created by `pymysql.connections.Connection`,
    but adds some properties and methods related to connection pool.

    OverWrite __exit__() and close() methods.
    When the connection pool attribute is attached, it does not exit and close the connection,
    but puts the connection back into the connection pool to reduce frequent connection creation.
    """

    def __init__(self, *args, **kwargs):
        super(Connection, self).__init__(*args, **kwargs)
        self._pool = None
        self.last_connected = time.time()

    def set_pool(self, pool: 'ConnectionPool') -> None:
        """Set connection pool properties for a connection"""
        self._pool = pool

    def close(self):
        """
        Overwrite the close() method of `pymysql.connections.Connection`

        With connection pooling (self._pool != None):
            return the connection to the connection pool instead of closing the connection
        Without connection pool (self._pool == None):
            call the close() method of `pymysql.connections.Connection` to close the connection
        """
        if not self._pool:
            super(Connection, self).close()
        else:
            self._pool.return_connection(self)

    def ping(self, reconnect=True):
        """
        Overwrite the ping() method of `pymysql.connections.Connection`

        Check if the server is available and if the communication between the client and the server are normal
        When the check throws an exception:
            1> close the old/broken connection
            2> try to re-establish a new connection with the server

        :param reconnect: If the connection is closed, reconnect.
        :type reconnect: boolean

        :raise Error: If the connection is closed and reconnect=False.
        """
        if self._sock is None:
            if reconnect:
                self.connect()
                reconnect = False
            else:
                raise err.Error("Already closed")
        try:
            self._execute_command(COMMAND.COM_PING, "")
            self._read_ok_packet()
            # When the test passes, update the last connected field value
            self.last_connected = time.time()
        except Exception as exc:
            if reconnect:
                # When an exception occurs, close old/broken connection
                if self._pool:
                    self._force_close()

                # try to establish a new connection
                self.connect()
                self.ping(False)
            else:
                raise exc

    def __enter__(self) -> 'Connection':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Overwrite the ping() method of `pymysql.connections.Connection`
        When `self._pool != None`, and no exception occurs, the connection is returned to the connection pool,
        otherwise the connection is closed.
        """
        if self._pool is not None:
            if not exc_type:
                # Reusable Connection, Return the connection to the connection pool
                self._pool.return_connection(self)
            else:
                # No Reusable Connection, close connection
                self._pool = None
                try:
                    super(Connection, self).close()
                except Exception as exc:
                    print(f'Warning: {self} __exit__ exception: {exc}')
                    self._force_close()
        else:
            super(Connection, self).__exit__(self, exc_type, exc_val, exc_tb)


class ConnectionPool(object):
    """
    Create a connection pool management object to store reusable connection objects
    and reduce the number and overhead of frequent connection creation
    """

    def __init__(self, min_idle, max_idle, max_lifetime=3600, **kwargs):
        """
        :param min_idle: (Int) minimum number of idle connections
        :param max_idle: (Int) maximum number of idle connections
        :param max_lifetime: (Int | Float) connection maximum lifetime
        :param kwargs: Dict, Connection object initialization parameters.
                       refer to the `pymysql.connections.Connection`__init__() method parameters for details
        """
        # Verify connection pool configuration
        if min_idle < 0:
            raise PoolError('min_idle should be greater than 0')
        if max_idle < min_idle:
            raise PoolError('max_idle should be granter than min_idle')

        self._min_idle = min_idle
        self._max_idle = max_idle
        self._lifetime = max_lifetime
        self._lock = threading.Lock()
        self._closed = False
        self._pool = deque(maxlen=self._max_idle)
        self._kwargs = kwargs

        # Record the total number of connections currently created
        self._connect_num = deque()

        # create min idle connection
        for _ in range(self._min_idle):
            conn = self._create_connection()
            self._pool.appendleft(conn)
            conn._returned = True  # Marks that the connection has been returned to the pool

    def _create_connection(self):
        """Create new connection"""
        try:
            conn = Connection(**self._kwargs)
            conn.set_pool(self)
            conn._returned = False
            self._connect_num.append(1)
            return conn
        except Exception as exc:
            raise CreateConnectionError(exc)

    def _get_connection(self) -> Connection:
        try:
            return self._pool.pop()
        except IndexError:
            if self.connect_num < self.maxsize:
                with self._lock:
                    if self.connect_num < self.maxsize:
                        return self._create_connection()
                    else:
                        raise CreateConnectionError("Error: the connection pool has reached the threshold.")
            else:
                raise GetConnectionError(f'There are no idle connections in the connection pool.')

    def get_connection(self, tries=3, delay=0.1, backoff=1, max_delay=30, ping=False) -> Connection:
        """
        Get an available connection object from the connection pool.

        :param tries: the maximum number of attempts. default: 2.
        :param delay: initial delay between attempts. default: 0.1.
        :param backoff: multiplier applied to delay between attempts. default: 1.
        :param max_delay: the maximum value of delay. default: 30.
        :param ping: check connection availability. default: False.
        """
        if self.is_closed:
            raise PoolError('ConnectionPool already closed')

        assert tries >= 0, 'tries should be greater or equal than 0'
        assert max_delay >= 0, 'max_delay should be greater or equal than 0'

        # Limit the number of retry operations configuration
        if tries > 10:
            tries = 10
        if max_delay > 60:
            max_delay = 60

        conn = None
        while tries:
            try:
                conn = self._get_connection()
                break
            except Exception as exc:
                tries -= 1
                if not tries:
                    raise GetConnectionError(exc)

                time.sleep(delay)

                delay *= backoff
                if max_delay is not None:
                    delay = min(delay, max_delay)
        else:
            if conn is None:
                conn = self._get_connection()

        # Check whether the connection has reached the maximum lifetime
        if (time.time() - conn.last_connected + 0.1) > self._lifetime:
            conn.set_pool = None
            conn.close()
            self._connect_num.pop()

            # Connection expired, create a new connection object
            return self._create_connection()
        else:
            if ping:
                conn.ping(reconnect=True)
            conn._returned = False
            return conn

    def return_connection(self, conn: Connection):
        """Return the connection to the connection pool"""
        if not hasattr(conn, '_pool') or getattr(conn, '_pool') is None or self.is_closed:
            # When the connection does not have the pool attribute, the connection is dropped
            conn.set_pool = None
            conn.close()
            conn._returned = True
            self._connect_num.pop()
            return

        # close cursor object
        conn.cursor().close()

        if conn._returned:
            raise ReturnConnectionError(f"This connection has already returned, connection: {conn}")

        # Check whether the connection has reached the maximum lifetime
        if (time.time() - conn.last_connected + 0.1) > self._lifetime:
            conn.set_pool = None
            conn.close()
            self._connect_num.pop()

            if self.connect_num < self.maxsize:
                conn = self._create_connection()
            else:
                conn._returned = True
                return
        self._pool.appendleft(conn)
        conn._returned = True

    def close(self):
        """Close the connection pool and the connection objects in the pool"""
        self._closed = True
        try:
            while self._pool:
                conn = self._pool.pop()
                conn.set_pool = None
                conn.close()
                conn._returned = True
                self._connect_num.pop()
        except Exception as exc:
            raise PoolError(f"Close the connection pool exception: {exc}")

    @property
    def is_closed(self) -> bool:
        return self._closed

    @property
    def size(self):
        """Returns the number of available connections in the current pool"""
        return len(self._pool)

    @property
    def maxsize(self):
        """The maximum number of connections that the connection pool can hold"""
        return self._max_idle

    @property
    def connect_num(self):
        """Returns the total number of connections currently in the pool"""
        return len(self._connect_num)

    def __enter__(self) -> 'ConnectionPool':
        return self

    __exit__ = close
