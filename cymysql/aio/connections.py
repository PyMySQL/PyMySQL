import sys
import ssl
import asyncio
import struct
from ..connections import (
    Connection,
    byte2int,
    int2byte,
    pack_int24,
    _xor,
)
from .cursors import AsyncCursor
from ..charset import  charset_by_name
from ..packet import MysqlPacket
from .result import AsyncMySQLResult
from .socketwrapper import AsyncSocketWrapper
from ..constants import CLIENT, COMMAND
from ..err import InterfaceError


class AsyncConnection(Connection):
    def __init__(self, *args, **kwargs):
        if kwargs.get("loop"):
            self.loop = kwargs.get("loop")
            del kwargs["loop"]
        else:
            self.loop = asyncio.get_event_loop()
        super().__init__(*args, **kwargs)

    def _connect(self):
        self.socket = AsyncSocketWrapper(self._get_socket(), self.compress)

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

    async def close(self):
        ''' Send the quit message and close the socket '''
        if self.socket is None:
            return
        send_data = b'\x01\x00\x00\x00' + int2byte(COMMAND.COM_QUIT)
        await self.socket.send_packet(send_data, self.loop)
        self.socket.close()
        self.socket = None

    async def autocommit(self, value):
        ''' Set whether or not to commit after every execute() '''
        if value:
            q = "SET AUTOCOMMIT = 1"
        else:
            q = "SET AUTOCOMMIT = 0"
        try:
            await self._execute_command(COMMAND.COM_QUERY, q)
            await self.read_packet()
        except:
            exc, value, tb = sys.exc_info()
            self.errorhandler(None, exc, value)

    async def commit(self):
        ''' Commit changes to stable storage '''
        try:
            await self._execute_command(COMMAND.COM_QUERY, "COMMIT")
            await self.read_packet()
        except:
            exc, value, tb = sys.exc_info()
            self.errorhandler(None, exc, value)

    async def rollback(self):
        ''' Roll back the current transaction '''
        try:
            await self._execute_command(COMMAND.COM_QUERY, "ROLLBACK")
            await self.read_packet()
        except:
            exc, value, tb = sys.exc_info()
            self.errorhandler(None, exc, value)

    def cursor(self, cursor=None):
        self._last_usage = self.loop.time()
        if cursor is None:
            cursor = self.cursorclass
        if cursor is None:
            cursor = AsyncCursor
        return cursor(self)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.socket is not None:
            await self.close()

    # The following methods are INTERNAL USE ONLY (called from Cursor)
    async def query(self, sql):
        await self._execute_command(COMMAND.COM_QUERY, sql)
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
            await self._execute_command(COMMAND.COM_PROCESS_KILL, arg)
            pkt = await self.read_packet()
            return pkt.is_ok_packet()
        except:
            exc, value, tb = sys.exc_info()
            self.errorhandler(None, exc, value)
        return False

    async def ping(self, reconnect=True):
        ''' Check if the server is alive '''
        try:
            await self._execute_command(COMMAND.COM_PING, "")
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
                await self._execute_command(COMMAND.COM_QUERY, "SET NAMES %s" %
                                      self.escape(charset))
                await self.read_packet()
                self.charset = charset
        except:
            exc, value, tb = sys.exc_info()
            self.errorhandler(None, exc, value)

    async def read_packet(self):
        """Read an entire "mysql packet" in its entirety from the network
        and return a MysqlPacket type that represents the results."""
        return MysqlPacket(await self.socket.recv_packet(self.loop), self.charset, self.encoding, self.use_unicode)

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
            await self.socket.send_uncompress_packet(data, self.loop)
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

        await self.socket.send_uncompress_packet(data, self.loop)
        auth_packet = await self.socket.recv_uncompress_packet(self.loop)

        if auth_packet[0] == 0xfe:  # EOF packet
            # AuthSwitchRequest
            # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
            i = auth_packet.find(b'\0', 1)
            self.auth_plugin_name = auth_packet[1:i].decode('utf-8')
            j = auth_packet.find(b'\0', i + 1)
            self.salt = auth_packet[i + 1:j]
            data = self._scramble()
            data = pack_int24(len(data)) + int2byte(next_packet) + data
            next_packet += 2
            await self.socket.send_uncompress_packet(data, self.loop)
            auth_packet = await self.socket.recv_uncompress_packet(self.loop)

        if self.auth_plugin_name == 'caching_sha2_password':
            await self._caching_sha2_authentication2(auth_packet, next_packet)

    async def _execute_command(self, command, sql):
        if not self.socket:
            self.errorhandler(None, InterfaceError, (-1, 'socket not found'))

        sql = sql.encode(self.encoding)

        if len(sql) + 1 > 0xffffff:
            raise ValueError('Sending query packet is too large')
        prelude = struct.pack('<i', len(sql)+1) + int2byte(command)
        await self.socket.send_packet(prelude + sql, self.loop)

    async def _caching_sha2_authentication2(self, auth_packet, next_packet):
        # https://dev.mysql.com/doc/dev/mysql-server/latest/page_caching_sha2_authentication_exchanges.html
        if auth_packet == b'\x01\x03':   # fast_auth_success
            await self.read_packet()
            return

        # perform_full_authentication
        assert auth_packet == b'\x01\x04'

        if self.ssl or self.unix_socket:
            data = self.password.encode(self.encoding) + b'\x00'
        else:
            # request_public_key
            data = b'\x02'
            data = pack_int24(len(data)) + int2byte(next_packet) + data
            next_packet += 2
            await self.socket.send_uncompress_packet(data, self.loop)
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
        await self.socket.send_packet(data, self.loop)

        await self.read_packet()

    async def _get_server_information(self):
        # https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake
        i = 0
        data = await self.socket.recv_uncompress_packet(self.loop)

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
