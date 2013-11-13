# Python implementation of the MySQL client-server protocol
# http://dev.mysql.com/doc/internals/en/client-server-protocol.html

from __future__ import print_function
from ._compat import PY2, range_type, text_type, str_type, JYTHON, IRONPYTHON

from functools import partial
import os
import hashlib
import socket

try:
    import ssl
    SSL_ENABLED = True
except ImportError:
    SSL_ENABLED = False

import struct
import sys
if PY2:
    import ConfigParser as configparser
else:
    import configparser

import io

try:
    import getpass
    DEFAULT_USER = getpass.getuser()
except ImportError:
    DEFAULT_USER = None


from .charset import MBLENGTH, charset_by_name, charset_by_id
from .cursors import Cursor
from .constants import FIELD_TYPE
from .constants import SERVER_STATUS
from .constants.CLIENT import *
from .constants.COMMAND import *
from .util import byte2int, int2byte
from .converters import escape_item, encoders, decoders, escape_string
from .err import (
    raise_mysql_exception, Warning, Error,
    InterfaceError, DataError, DatabaseError, OperationalError,
    IntegrityError, InternalError, NotSupportedError, ProgrammingError)

_py_version = sys.version_info[:2]


# socket.makefile() in Python 2 is not usable because very inefficient and
# bad behavior about timeout.
# XXX: ._socketio doesn't work under IronPython.
if _py_version == (2, 7) and not IRONPYTHON:
    # read method of file-like returned by sock.makefile() is very slow.
    # So we copy io-based one from Python 3.
    from ._socketio import SocketIO
    def _makefile(sock, mode):
        return io.BufferedReader(SocketIO(sock, mode))
elif _py_version == (2, 6):
    # Python 2.6 doesn't have fast io module.
    # So we make original one.
    class SockFile(object):
        def __init__(self, sock):
            self._sock = sock
        def read(self, n):
            read = self._sock.recv(n)
            if len(read) == n:
                return read
            while True:
                data = self._sock.recv(n-len(read))
                if not data:
                    return read
                read += data
                if len(read) == n:
                    return read

    def _makefile(sock, mode):
        assert mode == 'rb'
        return SockFile(sock)
else:
    # socket.makefile in Python 3 is nice.
    def _makefile(sock, mode):
        return sock.makefile(mode)


TEXT_TYPES = set([
    FIELD_TYPE.BIT,
    FIELD_TYPE.BLOB,
    FIELD_TYPE.LONG_BLOB,
    FIELD_TYPE.MEDIUM_BLOB,
    FIELD_TYPE.STRING,
    FIELD_TYPE.TINY_BLOB,
    FIELD_TYPE.VAR_STRING,
    FIELD_TYPE.VARCHAR])

sha_new = partial(hashlib.new, 'sha1')

DEBUG = False

NULL_COLUMN = 251
UNSIGNED_CHAR_COLUMN = 251
UNSIGNED_SHORT_COLUMN = 252
UNSIGNED_INT24_COLUMN = 253
UNSIGNED_INT64_COLUMN = 254
UNSIGNED_CHAR_LENGTH = 1
UNSIGNED_SHORT_LENGTH = 2
UNSIGNED_INT24_LENGTH = 3
UNSIGNED_INT64_LENGTH = 8

DEFAULT_CHARSET = 'latin1'

MAX_PACKET_LEN = 2**24-1


def dump_packet(data):

    def is_ascii(data):
        if 65 <= byte2int(data) <= 122:  #data.isalnum():
            if isinstance(data, int):
                return chr(data)
            return data
        return '.'

    try:
        print("packet length:", len(data))
        print("method call[1]:", sys._getframe(1).f_code.co_name)
        print("method call[2]:", sys._getframe(2).f_code.co_name)
        print("method call[3]:", sys._getframe(3).f_code.co_name)
        print("method call[4]:", sys._getframe(4).f_code.co_name)
        print("method call[5]:", sys._getframe(5).f_code.co_name)
        print("-" * 88)
    except ValueError:
        pass
    dump_data = [data[i:i+16] for i in range_type(0, min(len(data), 256), 16)]
    for d in dump_data:
        print(' '.join(map(lambda x:"{:02X}".format(byte2int(x)), d)) +
              '   ' * (16 - len(d)) + ' ' * 2 +
              ' '.join(map(lambda x:"{}".format(is_ascii(x)), d)))
    print("-" * 88)
    print()


def _scramble(password, message):
    if not password:
        return b'\0'
    if DEBUG: print('password=' + password)
    stage1 = sha_new(password).digest()
    stage2 = sha_new(stage1).digest()
    s = sha_new()
    s.update(message)
    s.update(stage2)
    result = s.digest()
    return _my_crypt(result, stage1)


def _my_crypt(message1, message2):
    length = len(message1)
    result = struct.pack('B', length)
    for i in range_type(length):
        x = (struct.unpack('B', message1[i:i+1])[0] ^
             struct.unpack('B', message2[i:i+1])[0])
        result += struct.pack('B', x)
    return result

# old_passwords support ported from libmysql/password.c
SCRAMBLE_LENGTH_323 = 8


class RandStruct_323(object):
    def __init__(self, seed1, seed2):
        self.max_value = 0x3FFFFFFF
        self.seed1 = seed1 % self.max_value
        self.seed2 = seed2 % self.max_value

    def my_rnd(self):
        self.seed1 = (self.seed1 * 3 + self.seed2) % self.max_value
        self.seed2 = (self.seed1 + self.seed2 + 33) % self.max_value
        return float(self.seed1) / float(self.max_value)


def _scramble_323(password, message):
    hash_pass = _hash_password_323(password)
    hash_message = _hash_password_323(message[:SCRAMBLE_LENGTH_323])
    hash_pass_n = struct.unpack(">LL", hash_pass)
    hash_message_n = struct.unpack(">LL", hash_message)

    rand_st = RandStruct_323(hash_pass_n[0] ^ hash_message_n[0],
                             hash_pass_n[1] ^ hash_message_n[1])
    outbuf = io.BytesIO()
    for _ in range_type(min(SCRAMBLE_LENGTH_323, len(message))):
        outbuf.write(int2byte(int(rand_st.my_rnd() * 31) + 64))
    extra = int2byte(int(rand_st.my_rnd() * 31))
    out = outbuf.getvalue()
    outbuf = io.BytesIO()
    for c in out:
        outbuf.write(int2byte(byte2int(c) ^ byte2int(extra)))
    return outbuf.getvalue()


def _hash_password_323(password):
    nr = 1345345333
    add = 7
    nr2 = 0x12345671

    for c in [byte2int(x) for x in password if x not in (' ', '\t')]:
        nr^= (((nr & 63)+add)*c)+ (nr << 8) & 0xFFFFFFFF
        nr2= (nr2 + ((nr2 << 8) ^ nr)) & 0xFFFFFFFF
        add= (add + c) & 0xFFFFFFFF

    r1 = nr & ((1 << 31) - 1) # kill sign bits
    r2 = nr2 & ((1 << 31) - 1)

    # pack
    return struct.pack(">LL", r1, r2)


def pack_int24(n):
    return struct.pack('<I', n)[:3]

def unpack_uint16(n):
    return struct.unpack('<H', n[0:2])[0]

def unpack_int24(n):
    return struct.unpack('<I', n + b'\0')[0]

def unpack_int32(n):
    return struct.unpack('<I', n)[0]

def unpack_int64(n):
    return struct.unpack('<Q', n)[0]


class MysqlPacket(object):
    """Representation of a MySQL response packet.  Reads in the packet
    from the network socket, removes packet header and provides an interface
    for reading/parsing the packet results."""

    def __init__(self, connection):
        self._position = 0
        self._recv_packet(connection)

    def _recv_packet(self, connection):
        """Parse the packet header and read entire packet payload into buffer."""
        buff = b''
        while True:
            packet_header = connection._read_bytes(4)
            if len(packet_header) < 4:
                raise OperationalError(2013, "Lost connection to MySQL server during query")

            if DEBUG: dump_packet(packet_header)
            packet_length_bin = packet_header[:3]

            #TODO: check sequence id
            self.__packet_number = byte2int(packet_header[3])

            bin_length = packet_length_bin + b'\0'  # pad little-endian number
            bytes_to_read = struct.unpack('<I', bin_length)[0]
            recv_data = connection._read_bytes(bytes_to_read)
            if len(recv_data) < bytes_to_read:
                raise OperationalError(2013, "Lost connection to MySQL server during query")
            if DEBUG: dump_packet(recv_data)
            buff += recv_data
            if bytes_to_read < MAX_PACKET_LEN:
                break
        self.__data = buff

    def packet_number(self):
        return self.__packet_number

    def get_all_data(self):
        return self.__data

    def read(self, size):
        """Read the first 'size' bytes in packet and advance cursor past them."""
        result = self.__data[self._position:(self._position+size)]
        if len(result) != size:
            error = ('Result length not requested length:\n'
                     'Expected=%s.  Actual=%s.  Position: %s.  Data Length: %s'
                     % (size, len(result), self._position, len(self.__data)))
            if DEBUG:
                print(error)
                self.dump()
            raise AssertionError(error)
        self._position += size
        return result

    def read_all(self):
        """Read all remaining data in the packet.

        (Subsequent read() will return errors.)
        """
        result = self.__data[self._position:]
        self._position = None  # ensure no subsequent read()
        return result

    def advance(self, length):
        """Advance the cursor in data buffer 'length' bytes."""
        new_position = self._position + length
        if new_position < 0 or new_position > len(self.__data):
            raise Exception('Invalid advance amount (%s) for cursor.  '
                            'Position=%s' % (length, new_position))
        self._position = new_position

    def rewind(self, position=0):
        """Set the position of the data buffer cursor to 'position'."""
        if position < 0 or position > len(self.__data):
            raise Exception("Invalid position to rewind cursor to: %s." % position)
        self._position = position

    def get_bytes(self, position, length=1):
        """Get 'length' bytes starting at 'position'.

        Position is start of payload (first four packet header bytes are not
        included) starting at index '0'.

        No error checking is done.  If requesting outside end of buffer
        an empty string (or string shorter than 'length') may be returned!
        """
        return self.__data[position:(position+length)]

    def read_length_encoded_integer(self):
        """Read a 'Length Coded Binary' number from the data buffer.

        Length coded numbers can be anywhere from 1 to 9 bytes depending
        on the value of the first byte.
        """
        c = ord(self.read(1))
        if c == NULL_COLUMN:
            return None
        if c < UNSIGNED_CHAR_COLUMN:
            return c
        elif c == UNSIGNED_SHORT_COLUMN:
            return unpack_uint16(self.read(UNSIGNED_SHORT_LENGTH))
        elif c == UNSIGNED_INT24_COLUMN:
            return unpack_int24(self.read(UNSIGNED_INT24_LENGTH))
        elif c == UNSIGNED_INT64_COLUMN:
            return unpack_int64(self.read(UNSIGNED_INT64_LENGTH))

    def read_length_coded_string(self):
        """Read a 'Length Coded String' from the data buffer.

        A 'Length Coded String' consists first of a length coded
        (unsigned, positive) integer represented in 1-9 bytes followed by
        that many bytes of binary data.  (For example "cat" would be "3cat".)
        """
        length = self.read_length_encoded_integer()
        if length is None:
            return None
        return self.read(length)

    def is_ok_packet(self):
        return self.__data[0:1] == b'\0'

    def is_eof_packet(self):
        # http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-EOF_Packet
        # Caution: \xFE may be LengthEncodedInteger.
        # If \xFE is LengthEncodedInteger header, 8bytes followed.
        return len(self.__data) < 9 and self.__data[0:1] == b'\xfe'

    def is_resultset_packet(self):
        field_count = ord(self.__data[0:1])
        return 1 <= field_count <= 250

    def is_error_packet(self):
        return self.__data[0:1] == b'\xff'

    def check_error(self):
        if self.is_error_packet():
            self.rewind()
            self.advance(1)  # field_count == error (we already know that)
            errno = unpack_uint16(self.read(2))
            if DEBUG: print("errno = {}".format(errno))
            raise_mysql_exception(self.__data)

    def dump(self):
        dump_packet(self.__data)


class FieldDescriptorPacket(MysqlPacket):
    """A MysqlPacket that represents a specific column's metadata in the result.

    Parsing is automatically done and the results are exported via public
    attributes on the class such as: db, table_name, name, length, type_code.
    """

    def __init__(self, connection):
        MysqlPacket.__init__(self, connection)
        self.__parse_field_descriptor(connection.encoding)

    def __parse_field_descriptor(self, encoding):
        """Parse the 'Field Descriptor' (Metadata) packet.

        This is compatible with MySQL 4.1+ (not compatible with MySQL 4.0).
        """
        self.catalog = self.read_length_coded_string()
        self.db = self.read_length_coded_string()
        self.table_name = self.read_length_coded_string().decode(encoding)
        self.org_table = self.read_length_coded_string().decode(encoding)
        self.name = self.read_length_coded_string().decode(encoding)
        self.org_name = self.read_length_coded_string().decode(encoding)
        self.advance(1)  # non-null filler
        self.charsetnr = struct.unpack('<H', self.read(2))[0]
        self.length = struct.unpack('<I', self.read(4))[0]
        self.type_code = byte2int(self.read(1))
        self.flags = struct.unpack('<H', self.read(2))[0]
        self.scale = byte2int(self.read(1))  # "decimals"
        self.advance(2)  # filler (always 0x00)

        # 'default' is a length coded binary and is still in the buffer?
        # not used for normal result sets...

    def description(self):
        """Provides a 7-item tuple compatible with the Python PEP249 DB Spec."""
        desc = []
        desc.append(self.name)
        desc.append(self.type_code)
        desc.append(None) # TODO: display_length; should this be self.length?
        desc.append(self.get_column_length()) # 'internal_size'
        desc.append(self.get_column_length()) # 'precision'  # TODO: why!?!?
        desc.append(self.scale)

        # 'null_ok' -- can this be True/False rather than 1/0?
        #              if so just do:  desc.append(bool(self.flags % 2 == 0))
        if self.flags % 2 == 0:
            desc.append(1)
        else:
            desc.append(0)
        return tuple(desc)

    def get_column_length(self):
        if self.type_code == FIELD_TYPE.VAR_STRING:
            mblen = MBLENGTH.get(self.charsetnr, 1)
            return self.length // mblen
        return self.length

    def __str__(self):
        return ('%s %r.%r.%r, type=%s, flags=%x'
                % (self.__class__, self.db, self.table_name, self.name,
                   self.type_code, self.flags))


class OKPacketWrapper(object):
    """
    OK Packet Wrapper. It uses an existing packet object, and wraps
    around it, exposing useful variables while still providing access
    to the original packet objects variables and methods.
    """

    def __init__(self, from_packet):
        if not from_packet.is_ok_packet():
            raise ValueError('Cannot create ' + str(self.__class__.__name__) +
                             ' object from invalid packet type')

        self.packet = from_packet
        self.packet.advance(1)

        self.affected_rows = self.packet.read_length_encoded_integer()
        self.insert_id = self.packet.read_length_encoded_integer()
        self.server_status = struct.unpack('<H', self.packet.read(2))[0]
        self.warning_count = struct.unpack('<H', self.packet.read(2))[0]
        self.message = self.packet.read_all()

    def __getattr__(self, key):
        if hasattr(self.packet, key):
            return getattr(self.packet, key)
        raise AttributeError(
            "{0} instance has no attribute '{1}'".format(self.__class__, key))


class EOFPacketWrapper(object):
    """
    EOF Packet Wrapper. It uses an existing packet object, and wraps
    around it, exposing useful variables while still providing access
    to the original packet objects variables and methods.
    """

    def __init__(self, from_packet):
        if not from_packet.is_eof_packet():
            raise ValueError(
                "Cannot create '{0}' object from invalid packet type".format(
                    self.__class__))

        self.packet = from_packet
        from_packet.advance(1)
        self.warning_count = struct.unpack('<h', from_packet.read(2))[0]
        self.server_status = struct.unpack('<h', self.packet.read(2))[0]
        if DEBUG: print("server_status=", self.server_status)
        self.has_next = self.server_status & SERVER_STATUS.SERVER_MORE_RESULTS_EXISTS

    def __getattr__(self, key):
        if hasattr(self.packet, key):
            return getattr(self.packet, key)
        raise AttributeError(
            "{0} instance has no attribute '{1}'".format(self.__class__, key))


class Connection(object):
    """
    Representation of a socket with a mysql server.

    The proper way to get an instance of this class is to call
    connect().

    """

    socket = None

    def __init__(self, host="localhost", user=None, passwd="",
                 database=None, port=3306, unix_socket=None,
                 charset='', sql_mode=None,
                 read_default_file=None, conv=decoders, use_unicode=None,
                 client_flag=0, cursorclass=Cursor, init_command=None,
                 connect_timeout=None, ssl=None, read_default_group=None,
                 compress=None, named_pipe=None, no_delay=False,
                 autocommit=False, db=None):
        """
        Establish a connection to the MySQL database. Accepts several
        arguments:

        host: Host where the database server is located
        user: Username to log in as
        passwd: Password to use.
        database: Database to use, None to not use a particular one.
        port: MySQL port to use, default is usually OK.
        unix_socket: Optionally, you can use a unix socket rather than TCP/IP.
        charset: Charset you want to use.
        sql_mode: Default SQL_MODE to use.
        read_default_file: Specifies  my.cnf file to read these parameters from under the [client] section.
        conv: Decoders dictionary to use instead of the default one. This is used to provide custom marshalling of types. See converters.
        use_unicode: Whether or not to default to unicode strings. This option defaults to true for Py3k.
        client_flag: Custom flags to send to MySQL. Find potential values in constants.CLIENT.
        cursorclass: Custom cursor class to use.
        init_command: Initial SQL statement to run when connection is established.
        connect_timeout: Timeout before throwing an exception when connecting.
        ssl: A dict of arguments similar to mysql_ssl_set()'s parameters. For now the capath and cipher arguments are not supported.
        read_default_group: Group to read from in the configuration file.
        compress; Not supported
        named_pipe: Not supported
        no_delay: Disable Nagle's algorithm on the socket
        autocommit: Autocommit mode. None means use server default. (default: False)
        db: Alias for database. (for compatibility to MySQLdb)
        """

        if use_unicode is None and sys.version_info[0] > 2:
            use_unicode = True

        if db is not None and database is None:
            database = db

        if compress or named_pipe:
            raise NotImplementedError("compress and named_pipe arguments are not supported")

        if ssl and ('capath' in ssl or 'cipher' in ssl):
            raise NotImplementedError('ssl options capath and cipher are not supported')

        self.ssl = False
        if ssl:
            if not SSL_ENABLED:
                raise NotImplementedError("ssl module not found")
            self.ssl = True
            client_flag |= SSL
            for k in ('key', 'cert', 'ca'):
                v = None
                if k in ssl:
                    v = ssl[k]
                setattr(self, k, v)

        if read_default_group and not read_default_file:
            if sys.platform.startswith("win"):
                read_default_file = "c:\\my.ini"
            else:
                read_default_file = "/etc/my.cnf"

        if read_default_file:
            if not read_default_group:
                read_default_group = "client"

            cfg = configparser.RawConfigParser()
            cfg.read(os.path.expanduser(read_default_file))

            def _config(key, default):
                try:
                    return cfg.get(read_default_group, key)
                except Exception:
                    return default

            user = _config("user", user)
            passwd = _config("password", passwd)
            host = _config("host", host)
            database = _config("database", database)
            unix_socket = _config("socket", unix_socket)
            port = int(_config("port", port))
            charset = _config("default-character-set", charset)

        self.host = host
        self.port = port
        self.user = user or DEFAULT_USER
        self.password = passwd
        self.db = database
        self.no_delay = no_delay
        self.unix_socket = unix_socket
        if charset:
            self.charset = charset
            self.use_unicode = True
        else:
            self.charset = DEFAULT_CHARSET
            self.use_unicode = False

        if use_unicode is not None:
            self.use_unicode = use_unicode

        self.encoding = charset_by_name(self.charset).encoding

        client_flag |= CAPABILITIES
        client_flag |= MULTI_STATEMENTS
        if self.db:
            client_flag |= CONNECT_WITH_DB
        self.client_flag = client_flag

        self.cursorclass = cursorclass
        self.connect_timeout = connect_timeout

        self._result = None
        self._affected_rows = 0
        self.host_info = "Not connected"

        #: specified autocommit mode. None means use server default.
        self.autocommit_mode = autocommit

        self.encoders = encoders  # Need for MySQLdb compatibility.
        self.decoders = conv
        self.sql_mode = sql_mode
        self.init_command = init_command
        self._connect()

    def close(self):
        ''' Send the quit message and close the socket '''
        if self.socket is None:
            raise Error("Already closed")
        send_data = struct.pack('<i', 1) + int2byte(COM_QUIT)
        try:
            self._write_bytes(send_data)
        except IOError:
            pass
        finally:
            sock = self.socket
            self.socket = None
            self._rfile = None
            sock.close()

    def __del__(self):
        if self.socket:
            self.close()

    def autocommit(self, value):
        self.autocommit_mode = bool(value)
        current = self.get_autocommit()
        if value != current:
            self._send_autocommit_mode()

    def get_autocommit(self):
        return bool(self.server_status &
                    SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT)

    def _read_ok_packet(self):
        pkt = self._read_packet()
        if not pkt.is_ok_packet():
            raise OperationalError(2014, "Command Out of Sync")
        ok = OKPacketWrapper(pkt)
        self.server_status = ok.server_status
        return True

    def _send_autocommit_mode(self):
        ''' Set whether or not to commit after every execute() '''
        self._execute_command(COM_QUERY, "SET AUTOCOMMIT = %s" %
                              self.escape(self.autocommit_mode))
        self._read_ok_packet()

    def commit(self):
        ''' Commit changes to stable storage '''
        self._execute_command(COM_QUERY, "COMMIT")
        self._read_ok_packet()

    def rollback(self):
        ''' Roll back the current transaction '''
        self._execute_command(COM_QUERY, "ROLLBACK")
        self._read_ok_packet()

    def select_db(self, db):
        '''Set current db'''
        self._execute_command(COM_INIT_DB, db)
        self._read_ok_packet()

    def escape(self, obj):
        ''' Escape whatever value you pass to it  '''
        if isinstance(obj, str_type):
            return "'" + self.escape_string(obj) + "'"
        return escape_item(obj, self.charset)

    def literal(self, obj):
        '''Alias for escape()'''
        return self.escape(obj)

    def escape_string(self, s):
        if (self.server_status &
                SERVER_STATUS.SERVER_STATUS_NO_BACKSLASH_ESCAPES):
            return s.replace("'", "''")
        return escape_string(s)

    def cursor(self, cursor=None):
        ''' Create a new cursor to execute queries with '''
        if cursor:
            return cursor(self)
        return self.cursorclass(self)

    def __enter__(self):
        ''' Context manager that returns a Cursor '''
        return self.cursor()

    def __exit__(self, exc, value, traceback):
        ''' On successful exit, commit. On exception, rollback. '''
        if exc:
            self.rollback()
        else:
            self.commit()

    # The following methods are INTERNAL USE ONLY (called from Cursor)
    def query(self, sql, unbuffered=False):
        #if DEBUG:
        #    print("DEBUG: sending query:", sql)
        if isinstance(sql, text_type) and not (JYTHON or IRONPYTHON):
            sql = sql.encode(self.encoding)
        self._execute_command(COM_QUERY, sql)
        self._affected_rows = self._read_query_result(unbuffered=unbuffered)
        return self._affected_rows

    def next_result(self):
        self._affected_rows = self._read_query_result()
        return self._affected_rows

    def affected_rows(self):
        return self._affected_rows

    def kill(self, thread_id):
        arg = struct.pack('<I', thread_id)
        self._execute_command(COM_PROCESS_KILL, arg)
        return self._read_ok_packet()

    def ping(self, reconnect=True):
        ''' Check if the server is alive '''
        if self.socket is None:
            if reconnect:
                self._connect()
                reconnect = False
            else:
                raise Error("Already closed")
        try:
            self._execute_command(COM_PING, "")
            return self._read_ok_packet()
        except Exception:
            if reconnect:
                self._connect()
                return self.ping(False)
            else:
                raise

    def set_charset(self, charset):
        # Make sure charset is supported.
        encoding = charset_by_name(charset).encoding

        self._execute_command(COM_QUERY, "SET NAMES %s" % self.escape(charset))
        self._read_packet()
        self.charset = charset
        self.encoding = encoding

    def _connect(self):
        sock = None
        try:
            if self.unix_socket and self.host in ('localhost', '127.0.0.1'):
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                t = sock.gettimeout()
                sock.settimeout(self.connect_timeout)
                sock.connect(self.unix_socket)
                sock.settimeout(t)
                self.host_info = "Localhost via UNIX socket"
                if DEBUG: print('connected using unix_socket')
            else:
                sock = socket.create_connection((self.host, self.port), self.connect_timeout)
                self.host_info = "socket %s:%d" % (self.host, self.port)
                if DEBUG: print('connected using socket')
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if self.no_delay:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.socket = sock
            self._rfile = _makefile(sock, 'rb')
            self._get_server_information()
            self._request_authentication()

            if self.sql_mode is not None:
                c = self.cursor()
                c.execute("SET sql_mode=%s", (self.sql_mode,))

            if self.init_command is not None:
                c = self.cursor()
                c.execute(self.init_command)
                self.commit()

            if self.autocommit_mode is not None:
                self.autocommit(self.autocommit_mode)
        except Exception as e:
            self._rfile = None
            if sock is not None:
                try:
                    sock.close()
                except socket.error:
                    pass
            raise OperationalError(
                2003, "Can't connect to MySQL server on %r (%s)" % (self.host, e))


    def _read_packet(self, packet_type=MysqlPacket):
        """Read an entire "mysql packet" in its entirety from the network
        and return a MysqlPacket type that represents the results."""

        packet = packet_type(self)
        packet.check_error()
        return packet

    def _read_bytes(self, num_bytes):
        return self._rfile.read(num_bytes)

    def _write_bytes(self, data):
        self.socket.sendall(data)

    def _read_query_result(self, unbuffered=False):
        if unbuffered:
            try:
                result = MySQLResult(self)
                result.init_unbuffered_query()
            except:
                result.unbuffered_active = False
                raise
        else:
            result = MySQLResult(self)
            result.read()
        self._result = result
        if result.server_status is not None:
            self.server_status = result.server_status
        return result.affected_rows

    def insert_id(self):
        if self._result:
            return self._result.insert_id
        else:
            return 0

    def _execute_command(self, command, sql):
        if not self.socket:
            raise InterfaceError("(0, '')")

        # If the last query was unbuffered, make sure it finishes before
        # sending new commands
        if self._result is not None and self._result.unbuffered_active:
            self._result._finish_unbuffered_query()

        if isinstance(sql, text_type):
            sql = sql.encode(self.encoding)

        chunk_size = min(MAX_PACKET_LEN, len(sql) + 1)  # +1 is for command

        prelude = struct.pack('<i', chunk_size) + int2byte(command)
        self._write_bytes(prelude + sql[:chunk_size-1])
        if DEBUG: dump_packet(prelude + sql)

        if chunk_size < MAX_PACKET_LEN:
            return

        seq_id = 1
        sql = sql[chunk_size-1:]
        while True:
            chunk_size = min(MAX_PACKET_LEN, len(sql))
            prelude = struct.pack('<i', chunk_size)[:3]
            data = prelude + int2byte(seq_id%256) + sql[:chunk_size]
            self._write_bytes(data)
            if DEBUG: dump_packet(data)
            sql = sql[chunk_size:]
            if not sql and chunk_size < MAX_PACKET_LEN:
                break
            seq_id += 1

    def _request_authentication(self):
        self.client_flag |= CAPABILITIES
        if self.server_version.startswith('5'):
            self.client_flag |= MULTI_RESULTS

        if self.user is None:
            raise ValueError("Did not specify a username")

        charset_id = charset_by_name(self.charset).id
        if isinstance(self.user, text_type):
            self.user = self.user.encode(self.encoding)

        data_init = struct.pack('<i', self.client_flag) + struct.pack("<I", 1) + \
                     int2byte(charset_id) + int2byte(0)*23

        next_packet = 1

        if self.ssl:
            data = pack_int24(len(data_init)) + int2byte(next_packet) + data_init
            next_packet += 1

            if DEBUG: dump_packet(data)

            self._write_bytes(data)
            self.socket = ssl.wrap_socket(self.socket, keyfile=self.key,
                                          certfile=self.cert,
                                          ssl_version=ssl.PROTOCOL_TLSv1,
                                          cert_reqs=ssl.CERT_REQUIRED,
                                          ca_certs=self.ca)
            self._rfile = _makefile(self.socket, 'rb')

        data = data_init + self.user + b'\0' + \
            _scramble(self.password.encode('latin1'), self.salt)

        if self.db:
            if isinstance(self.db, text_type):
                self.db = self.db.encode(self.encoding)
            data += self.db + int2byte(0)

        data = pack_int24(len(data)) + int2byte(next_packet) + data
        next_packet += 2

        if DEBUG: dump_packet(data)

        self._write_bytes(data)

        auth_packet = MysqlPacket(self)
        auth_packet.check_error()
        if DEBUG: auth_packet.dump()

        # if old_passwords is enabled the packet will be 1 byte long and
        # have the octet 254

        if auth_packet.is_eof_packet():
            # send legacy handshake
            data = _scramble_323(self.password.encode('latin1'), self.salt) + b'\0'
            data = pack_int24(len(data)) + int2byte(next_packet) + data

            self._write_bytes(data)
            auth_packet = MysqlPacket(self)
            auth_packet.check_error()
            if DEBUG: auth_packet.dump()

    # _mysql support
    def thread_id(self):
        return self.server_thread_id[0]

    def character_set_name(self):
        return self.charset

    def get_host_info(self):
        return self.host_info

    def get_proto_info(self):
        return self.protocol_version

    def _get_server_information(self):
        i = 0
        packet = MysqlPacket(self)
        packet.check_error()
        data = packet.get_all_data()

        if DEBUG: dump_packet(data)
        self.protocol_version = byte2int(data[i:i+1])
        i += 1

        server_end = data.find(int2byte(0), i)
        self.server_version = data[i:server_end].decode('latin1')
        i = server_end + 1

        self.server_thread_id = struct.unpack('<I', data[i:i+4])
        i += 4

        self.salt = data[i:i+8]
        i += 9  # 8 + 1(filler)

        self.server_capabilities = struct.unpack('<H', data[i:i+2])[0]
        i += 2

        if len(data) >= i + 6:
            lang, stat, cap_h, salt_len = struct.unpack('<BHHB', data[i:i+6])
            i += 6
            self.server_language = lang
            self.server_charset = charset_by_id(lang).name

            self.server_status = stat
            if DEBUG: print("server_status: %x" % stat)

            self.server_capabilities |= cap_h << 16
            if DEBUG: print("salt_len:", salt_len)
            salt_len = max(12, salt_len - 9)

        # reserved
        i += 10

        if len(data) >= i + salt_len:
            self.salt += data[i:i+salt_len] # salt_len includes auth_plugin_data_part_1 and filler
        #TODO: AUTH PLUGIN NAME may appeare here.

    def get_server_info(self):
        return self.server_version

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


# TODO: move OK and EOF packet parsing/logic into a proper subclass
#       of MysqlPacket like has been done with FieldDescriptorPacket.
class MySQLResult(object):

    def __init__(self, connection):
        from weakref import proxy
        self.connection = proxy(connection)
        self.affected_rows = None
        self.insert_id = None
        self.server_status = None
        self.warning_count = 0
        self.message = None
        self.field_count = 0
        self.description = None
        self.rows = None
        self.has_next = None
        self.unbuffered_active = False

    def __del__(self):
        if self.unbuffered_active:
            self._finish_unbuffered_query()

    def read(self):
        first_packet = self.connection._read_packet()

        # TODO: use classes for different packet types?
        if first_packet.is_ok_packet():
            self._read_ok_packet(first_packet)
        else:
            self._read_result_packet(first_packet)

    def init_unbuffered_query(self):
        self.unbuffered_active = True
        first_packet = self.connection._read_packet()

        if first_packet.is_ok_packet():
            self._read_ok_packet(first_packet)
            self.unbuffered_active = False
        else:
            self.field_count = first_packet.read_length_encoded_integer()
            self._get_descriptions()

            # Apparently, MySQLdb picks this number because it's the maximum
            # value of a 64bit unsigned integer. Since we're emulating MySQLdb,
            # we set it to this instead of None, which would be preferred.
            self.affected_rows = 18446744073709551615

    def _read_ok_packet(self, first_packet):
        ok_packet = OKPacketWrapper(first_packet)
        self.affected_rows = ok_packet.affected_rows
        self.insert_id = ok_packet.insert_id
        self.server_status = ok_packet.server_status
        self.warning_count = ok_packet.warning_count
        self.message = ok_packet.message

    def _check_packet_is_eof(self, packet):
        if packet.is_eof_packet():
            eof_packet = EOFPacketWrapper(packet)
            self.warning_count = eof_packet.warning_count
            self.has_next = eof_packet.has_next
            return True
        return False

    def _read_result_packet(self, first_packet):
        self.field_count = first_packet.read_length_encoded_integer()
        self._get_descriptions()
        self._read_rowdata_packet()

    def _read_rowdata_packet_unbuffered(self):
        # Check if in an active query
        if not self.unbuffered_active:
            return

        # EOF
        packet = self.connection._read_packet()
        if self._check_packet_is_eof(packet):
            self.unbuffered_active = False
            self.rows = None
            return

        row = self._read_row_from_packet(packet)
        self.affected_rows = 1
        self.rows = (row,)  # rows should tuple of row for MySQL-python compatibility.
        return row

    def _finish_unbuffered_query(self):
        # After much reading on the MySQL protocol, it appears that there is,
        # in fact, no way to stop MySQL from sending all the data after
        # executing a query, so we just spin, and wait for an EOF packet.
        while self.unbuffered_active:
            packet = self.connection._read_packet()
            if self._check_packet_is_eof(packet):
                self.unbuffered_active = False

    def _read_rowdata_packet(self):
        """Read a rowdata packet for each data row in the result set."""
        rows = []
        while True:
            packet = self.connection._read_packet()
            if self._check_packet_is_eof(packet):
                break
            rows.append(self._read_row_from_packet(packet))

        self.affected_rows = len(rows)
        self.rows = tuple(rows)

    def _read_row_from_packet(self, packet):
        use_unicode = self.connection.use_unicode
        row = []
        for field in self.fields:
            data = packet.read_length_coded_string()
            if data is not None:
                field_type = field.type_code
                if use_unicode:
                    if field_type in TEXT_TYPES:
                        charset = charset_by_id(field.charsetnr)
                        if use_unicode and not charset.is_binary:
                            # TEXTs with charset=binary means BINARY types.
                            data = data.decode(charset.encoding)
                    else:
                        data = data.decode()

                converter = self.connection.decoders.get(field_type)
                if DEBUG: print("DEBUG: field={}, converter={}".format(field, converter))
                if DEBUG: print("DEBUG: DATA = ", data)
                if converter is not None:
                    data = converter(data)
            row.append(data)
        return tuple(row)

    def _get_descriptions(self):
        """Read a column descriptor packet for each column in the result."""
        self.fields = []
        description = []
        for i in range_type(self.field_count):
            field = self.connection._read_packet(FieldDescriptorPacket)
            self.fields.append(field)
            description.append(field.description())

        eof_packet = self.connection._read_packet()
        assert eof_packet.is_eof_packet(), 'Protocol error, expecting EOF'
        self.description = tuple(description)
