import sys
import socket
import asyncio
import struct
from ..connections import (
    Connection,
    byte2int,
    int2byte,
    _caching_sha2_password_scramble,
    _mysql_native_password_scramble,
    pack_int24,
)
from .cursors import AsyncCursor
from ..charset import  charset_by_name
from ..packet import MysqlPacket
from .result import AsyncMySQLResult
from ..constants import CLIENT, COMMAND
from ..err import Warning, Error, \
     InterfaceError, DataError, DatabaseError, OperationalError, \
     IntegrityError, InternalError, NotSupportedError, ProgrammingError
from .context import _ConnectionContextManager, _ContextManager
from .recv import recv_packet


class AsyncConnection(Connection):
    def __init__(self, *args, **kwargs):
        if kwargs.get("loop"):
            self.loop = kwargs.get("loop")
            del kwargs["loop"]
        else:
            self.loop = asyncio.get_event_loop()
        super().__init__(*args, **kwargs)

    async def _initialize(self):
        self.socket.setblocking(False)
        await self._get_server_information()
        await self._request_authentication()
        await self.set_charset(self.charset)

        await self.autocommit(False)

        if self.sql_mode is not None:
            c = self.cursor()
            c.execute("SET sql_mode=%s", (self.sql_mode,))

        if self.init_command is not None:
            c = self.cursor()
            c.execute(self.init_command)

            self.commit()

    def close(self):
        ''' Send the quit message and close the socket '''
        if self.socket is None:
            raise Error("Already closed")
        send_data = b'\x01\x00\x00\x00' + int2byte(COMMAND.COM_QUIT)
        self.socket.sendall(send_data)
        self.socket.close()
        self.socket = None

    async def autocommit(self, value):
        ''' Set whether or not to commit after every execute() '''
        if value:
            q = "SET AUTOCOMMIT = 1"
        else:
            q = "SET AUTOCOMMIT = 0"
        try:
            self._execute_command(COMMAND.COM_QUERY, q)
            await self.read_packet()
        except:
            exc, value, tb = sys.exc_info()
            self.errorhandler(None, exc, value)

    async def commit(self):
        ''' Commit changes to stable storage '''
        try:
            self._execute_command(COMMAND.COM_QUERY, "COMMIT")
            await self.read_packet()
        except:
            exc, value, tb = sys.exc_info()
            self.errorhandler(None, exc, value)

    async def rollback(self):
        ''' Roll back the current transaction '''
        try:
            self._execute_command(COMMAND.COM_QUERY, "ROLLBACK")
            await self.read_packet()
        except:
            exc, value, tb = sys.exc_info()
            self.errorhandler(None, exc, value)

    def cursor(self, cursor=None):
        if cursor is None:
            cursor = self.cursorclass
        if cursor is None:
            cursor = AsyncCursor
        cur = cursor(self)
        self._last_usage = self.loop.time()
        fut = self.loop.create_future()
        fut.set_result(cur)
        return _ContextManager(fut)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.socket is not None:
            self.close()

    def __del__(self):
        if hasattr(self, 'socket') and self.socket:
            self.socket.close()
            self.socket = None

    def _is_connect(self):
        return bool(self.socket)

    # The following methods are INTERNAL USE ONLY (called from Cursor)
    async def query(self, sql):
        self._execute_command(COMMAND.COM_QUERY, sql)
        self._result = AsyncMySQLResult(self)
        await self._result.read_result()

    async def next_result(self):
        self._result = AsyncMySQLResult(self)
        await self._result.read_result()

    def affected_rows(self):
        if self._result:
            self._result._affected_rows
        else:
            return 0

    async def kill(self, thread_id):
        arg = struct.pack('<I', thread_id)
        try:
            self._execute_command(COMMAND.COM_PROCESS_KILL, arg)
            pkt = await self.read_packet()
            return pkt.is_ok_packet()
        except:
            exc, value, tb = sys.exc_info()
            self.errorhandler(None, exc, value)
        return False

    async def ping(self, reconnect=True):
        ''' Check if the server is alive '''
        try:
            self._execute_command(COMMAND.COM_PING, "")
        except:
            if reconnect:
                self._connect()
                return self.ping(False)
            else:
                exc, value, tb = sys.exc_info()
                self.errorhandler(None, exc, value)
                return

        pkt = await self.read_packet()
        return pkt.is_ok_packet()

    async def set_charset(self, charset):
        try:
            if charset:
                self._execute_command(COMMAND.COM_QUERY, "SET NAMES %s" %
                                      self.escape(charset))
                await self.read_packet()
                self.charset = charset
        except:
            exc, value, tb = sys.exc_info()
            self.errorhandler(None, exc, value)

    def _connect(self):
        sock = None
        try:
            if self.unix_socket and (self.host == 'localhost' or self.host == '127.0.0.1'):
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                t = sock.gettimeout()
                sock.settimeout(self.connect_timeout)
                sock.connect(self.unix_socket)
                sock.settimeout(t)
                self.host_info = "Localhost via UNIX socket"
            else:
                sock = socket.create_connection((self.host, self.port), self.connect_timeout)
                self.host_info = "socket %s:%d" % (self.host, self.port)
        except socket.error as e:
            if sock:
                sock.close()
            raise OperationalError(
                2003, "Can't connect to MySQL server on %r (%s)" % (self.host, e.args[0])
            )
        self.socket = sock

    async def read_packet(self):
        """Read an entire "mysql packet" in its entirety from the network
        and return a MysqlPacket type that represents the results."""
        return MysqlPacket(await recv_packet(self.socket, self.loop), self.charset, self.encoding, self.use_unicode)

    def _execute_command(self, command, sql):
        if not self.socket:
            self.errorhandler(None, InterfaceError, (-1, 'socket not found'))
            sql = sql.encode(self.encoding)
        sql = sql.encode(self.encoding)
        if len(sql) + 1 > 0xffffff:
            raise ValueError('Sending query packet is too large')
        prelude = struct.pack('<i', len(sql)+1) + int2byte(command)
        self.socket.sendall(prelude + sql)

    def _scramble(self):
        if self.auth_plugin_name in ('', 'mysql_native_password'):
            data = _mysql_native_password_scramble(
                self.password.encode(self.encoding), self.salt
            )
        elif self.auth_plugin_name == 'caching_sha2_password':
            data = _caching_sha2_password_scramble(
                self.password.encode(self.encoding), self.salt
            )
        elif self.auth_plugin_name == 'mysql_clear_password':
            data = self.password.encode(self.encoding) + b'\x00'
        else:
            raise NotImplementedError(
                "%s authentication plugin is not implemented" % (self.auth_plugin_name, )
            )
        return data

    async def _request_authentication(self):
        if self.user is None:
            raise ValueError("Did not specify a username")

        next_packet = 1

        charset_id = charset_by_name(self.charset).id
        user = self.user.encode(self.encoding)

        data_init = (
            struct.pack('<i', self.client_flag) +
            struct.pack("<I", 1) +
            int2byte(charset_id) + int2byte(0)*23
        )

        if self.ssl and self.server_capabilities & CLIENT.SSL:
            data = pack_int24(len(data_init)) + int2byte(next_packet) + data_init
            self.socket.sendall(data)
            next_packet += 1
            self.socket = ssl.wrap_socket(self.socket, keyfile=self.key,
                                          certfile=self.cert,
                                          ca_certs=self.ca)

        data = data_init + user + int2byte(0)
        authresp = self._scramble()

        if self.server_capabilities & CLIENT.SECURE_CONNECTION:
            data += int2byte(len(authresp)) + authresp
        else:
            data += authresp + int2byte(0)

        if self.db and self.server_capabilities & CLIENT.CONNECT_WITH_DB:
            data += self.db.encode(self.encoding) + int2byte(0)

        if self.server_capabilities & CLIENT.PLUGIN_AUTH:
            data += self.auth_plugin_name.encode(self.encoding) + int2byte(0)

        data = pack_int24(len(data)) + int2byte(next_packet) + data
        next_packet += 2

        self.socket.sendall(data)
        auth_packet = await self.read_packet()

        if auth_packet.is_eof_packet():
            # AuthSwitchRequest
            # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
            self.auth_plugin_name, self.salt = auth_packet.read_auth_switch_request()
            data = self._scramble()
            data = pack_int24(len(data)) + int2byte(next_packet) + data
            next_packet += 2
            self.socket.sendall(data)
            auth_packet = await self.read_packet()

        if self.auth_plugin_name == 'caching_sha2_password':
            await self._caching_sha2_authentication2(auth_packet, next_packet)

    async def _caching_sha2_authentication2(self, auth_packet, next_packet):
        # https://dev.mysql.com/doc/dev/mysql-server/latest/page_caching_sha2_authentication_exchanges.html
        if auth_packet.get_all_data() == b'\x01\x03':   # fast_auth_success
            await self.read_packet()
            return

        # perform_full_authentication
        assert auth_packet.get_all_data() == b'\x01\x04'

        if self.ssl or self.unix_socket:
            data = self.password.encode(self.encoding) + b'\x00'
        else:
            # request_public_key
            data = b'\x02'
            data = pack_int24(len(data)) + int2byte(next_packet) + data
            next_packet += 2
            self.socket.sendall(data)
            response = await self.read_packet()
            public_pem = response.get_all_data()[1:]

            from Crypto.PublicKey import RSA
            from Crypto.Cipher import PKCS1_OAEP
            key = RSA.importKey(public_pem)
            cipher = PKCS1_OAEP.new(key)
            password = self.password.encode(self.encoding) + b'\x00'
            data = cipher.encrypt(_xor(password, self.salt))

        data = pack_int24(len(data)) + int2byte(next_packet) + data
        next_packet += 2
        self.socket.sendall(data)

        await self.read_packet()

    # _mysql support
    def thread_id(self):
        return self.server_thread_id[0]

    def character_set_name(self):
        return self.charset

    def get_host_info(self):
        return self.host_info

    def get_proto_info(self):
        return self.protocol_version

    async def _get_server_information(self):
        # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake
        i = 0
        packet = await self.read_packet()
        data = packet.get_all_data()

        self.protocol_version = byte2int(data[i:i+1])
        i += 1
        str_end = data.find(int2byte(0), i)
        self.server_version = data[i:str_end].decode('utf-8')
        i = str_end + 1
        self.server_thread_id = struct.unpack('<I', data[i:i+4])
        i += 4
        self.salt = data[i:i+8]
        i += 9
        self.server_capabilities = struct.unpack('<H', data[i:i+2])[0]
        i += 2

        self.server_status = None
        self.auth_plugin_name = ''
        if len(data) > i:
            # Drop server_language and server_charset now.
            # character_set(1) only the lower 8 bits
            # self.server_language = byte2int(data[i:i+1])
            # self.server_charset = charset_by_id(self.server_language).name
            i += 1
            self.server_status = struct.unpack('<H', data[i:i+2])[0]
            i += 2
            self.server_capabilities |= (struct.unpack('<H', data[i:i+2])[0]) << 16
            i += 2

            salt_len = byte2int(data[i:i+1])
            i += 1

            i += 10     # reserverd
            if salt_len:
                rest_salt_len = max(13, salt_len-8)
                self.salt += data[i:i+rest_salt_len-1]
                i += rest_salt_len
            self.auth_plugin_name = data[i:data.find(int2byte(0), i)].decode('utf-8')


async def connect(*args, **kwargs):
    conn = AsyncConnection(*args, **kwargs)
    conn._connect()
    await conn._initialize()
    return conn
