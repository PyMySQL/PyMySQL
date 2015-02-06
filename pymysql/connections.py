# Python implementation of the MySQL client-server protocol
# http://dev.mysql.com/doc/internals/en/client-server-protocol.html
# Error codes:
# http://dev.mysql.com/doc/refman/5.5/en/error-messages-client.html
from __future__ import print_function
from ._compat import PY2, range_type, text_type, str_type, JYTHON, IRONPYTHON
DEBUG = False

import errno
from functools import partial
import hashlib
import io
import os
import socket
import struct
import sys
import warnings

try:
    import ssl
    SSL_ENABLED = True
except ImportError:
    ssl = None
    SSL_ENABLED = False

if PY2:
    import ConfigParser as configparser
else:
    import configparser

try:
    import getpass
    DEFAULT_USER = getpass.getuser()
    del getpass
except ImportError:
    DEFAULT_USER = None



from .charset import MBLENGTH, charset_by_name, charset_by_id
from .cursors import Cursor
from .constants import CLIENT, COMMAND, FIELD_TYPE, SERVER_STATUS
from .util import byte2int, int2byte
from .converters import (
    escape_item, encoders, decoders, escape_string, through)
from .err import (
    raise_mysql_exception, Warning, Error,
    InterfaceError, DataError, DatabaseError, OperationalError,
    IntegrityError, InternalError, NotSupportedError, ProgrammingError)
from . import err
from . import _connection_protocol

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

NULL_COLUMN = 251
UNSIGNED_CHAR_COLUMN = 251
UNSIGNED_SHORT_COLUMN = 252
UNSIGNED_INT24_COLUMN = 253
UNSIGNED_INT64_COLUMN = 254

DEFAULT_CHARSET = 'latin1'

MAX_PACKET_LEN = 2**24-1


def dump_packet(data):
    def is_ascii(data):
        if 65 <= byte2int(data) <= 122:
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
        print(' '.join(map(lambda x: "{:02X}".format(byte2int(x)), d)) +
              '   ' * (16 - len(d)) + ' ' * 2 +
              ' '.join(map(lambda x: "{}".format(is_ascii(x)), d)))
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


class MysqlPacket(object):
    """Representation of a MySQL response packet.

    Provides an interface for reading/parsing the packet results.
    """
    __slots__ = ('_position', '_data')

    def __init__(self, data, encoding):
        self._position = 0
        self._data = data

    def get_all_data(self):
        return self._data

    def read(self, size):
        """Read the first 'size' bytes in packet and advance cursor past them."""
        result = self._data[self._position:(self._position+size)]
        if len(result) != size:
            error = ('Result length not requested length:\n'
                     'Expected=%s.  Actual=%s.  Position: %s.  Data Length: %s'
                     % (size, len(result), self._position, len(self._data)))
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
        result = self._data[self._position:]
        self._position = None  # ensure no subsequent read()
        return result

    def advance(self, length):
        """Advance the cursor in data buffer 'length' bytes."""
        new_position = self._position + length
        if new_position < 0 or new_position > len(self._data):
            raise Exception('Invalid advance amount (%s) for cursor.  '
                            'Position=%s' % (length, new_position))
        self._position = new_position

    def rewind(self, position=0):
        """Set the position of the data buffer cursor to 'position'."""
        if position < 0 or position > len(self._data):
            raise Exception("Invalid position to rewind cursor to: %s." % position)
        self._position = position

    def get_bytes(self, position, length=1):
        """Get 'length' bytes starting at 'position'.

        Position is start of payload (first four packet header bytes are not
        included) starting at index '0'.

        No error checking is done.  If requesting outside end of buffer
        an empty string (or string shorter than 'length') may be returned!
        """
        return self._data[position:(position+length)]

    if PY2:
        def read_uint8(self):
            result = ord(self._data[self._position])
            self._position += 1
            return result
    else:
        def read_uint8(self):
            result = self._data[self._position]
            self._position += 1
            return result

    def read_uint16(self):
        result = struct.unpack_from('<H', self._data, self._position)[0]
        self._position += 2
        return result

    def read_uint24(self):
        low, high = struct.unpack_from('<HB', self._data, self._position)
        self._position += 3
        return low + (high << 16)

    def read_uint32(self):
        result = struct.unpack_from('<I', self._data, self._position)[0]
        self._position += 4
        return result

    def read_uint64(self):
        result = struct.unpack_from('<Q', self._data, self._position)[0]
        self._position += 8
        return result

    def read_length_encoded_integer(self):
        """Read a 'Length Coded Binary' number from the data buffer.

        Length coded numbers can be anywhere from 1 to 9 bytes depending
        on the value of the first byte.
        """
        c = self.read_uint8()
        if c == NULL_COLUMN:
            return None
        if c < UNSIGNED_CHAR_COLUMN:
            return c
        elif c == UNSIGNED_SHORT_COLUMN:
            return self.read_uint16()
        elif c == UNSIGNED_INT24_COLUMN:
            return self.read_uint24()
        elif c == UNSIGNED_INT64_COLUMN:
            return self.read_uint64()

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

    def read_struct(self, fmt):
        s = struct.Struct(fmt)
        result = s.unpack_from(self._data, self._position)
        self._position += s.size
        return result

    def is_ok_packet(self):
        return self._data[0:1] == b'\0'

    def is_eof_packet(self):
        # http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-EOF_Packet
        # Caution: \xFE may be LengthEncodedInteger.
        # If \xFE is LengthEncodedInteger header, 8bytes followed.
        return len(self._data) < 9 and self._data[0:1] == b'\xfe'

    def is_resultset_packet(self):
        field_count = ord(self._data[0:1])
        return 1 <= field_count <= 250

    def is_load_local_packet(self):
        return self._data[0:1] == b'\xfb'

    def is_error_packet(self):
        return self._data[0:1] == b'\xff'

    def check_error(self):
        if self.is_error_packet():
            self.rewind()
            self.advance(1)  # field_count == error (we already know that)
            errno = self.read_uint16()
            if DEBUG: print("errno =", errno)
            raise_mysql_exception(self._data)

    def dump(self):
        dump_packet(self._data)


class FieldDescriptorPacket(MysqlPacket):
    """A MysqlPacket that represents a specific column's metadata in the result.

    Parsing is automatically done and the results are exported via public
    attributes on the class such as: db, table_name, name, length, type_code.
    """

    def __init__(self, data, encoding):
        MysqlPacket.__init__(self, data, encoding)
        self.__parse_field_descriptor(encoding)

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
        self.charsetnr, self.length, self.type_code, self.flags, self.scale = (
            self.read_struct('<xHIBHBxx'))
        # 'default' is a length coded binary and is still in the buffer?
        # not used for normal result sets...

    def description(self):
        """Provides a 7-item tuple compatible with the Python PEP249 DB Spec."""
        return (
            self.name,
            self.type_code,
            None,  # TODO: display_length; should this be self.length?
            self.get_column_length(),  # 'internal_size'
            self.get_column_length(),  # 'precision'  # TODO: why!?!?
            self.scale,
            self.flags % 2 == 0)

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
        self.server_status, self.warning_count = self.read_struct('<HH')
        self.message = self.packet.read_all()
        self.has_next = self.server_status & SERVER_STATUS.SERVER_MORE_RESULTS_EXISTS

    def __getattr__(self, key):
        return getattr(self.packet, key)


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
        self.warning_count, self.server_status = self.packet.read_struct('<xhh')
        if DEBUG: print("server_status=", self.server_status)
        self.has_next = self.server_status & SERVER_STATUS.SERVER_MORE_RESULTS_EXISTS

    def __getattr__(self, key):
        return getattr(self.packet, key)


class LoadLocalPacketWrapper(object):
    """
    Load Local Packet Wrapper. It uses an existing packet object, and wraps
    around it, exposing useful variables while still providing access
    to the original packet objects variables and methods.
    """

    def __init__(self, from_packet):
        if not from_packet.is_load_local_packet():
            raise ValueError(
                "Cannot create '{0}' object from invalid packet type".format(
                    self.__class__))

        self.packet = from_packet
        self.filename = self.packet.get_all_data()[1:]
        if DEBUG: print("filename=", self.filename)

    def __getattr__(self, key):
        return getattr(self.packet, key)


class Connection(object):
    """
    Representation of a socket with a mysql server.

    The proper way to get an instance of this class is to call
    connect().

    """
    socket = None

    def __init__(self, host="localhost", user=None, password="",
                 database=None, port=3306, unix_socket=None,
                 charset='', sql_mode=None,
                 read_default_file=None, conv=decoders, use_unicode=None,
                 client_flag=0, cursorclass=Cursor, init_command=None,
                 connect_timeout=None, ssl=None, read_default_group=None,
                 compress=None, named_pipe=None, no_delay=False,
                 autocommit=False, db=None, passwd=None, local_infile=False,
                 defer_mode=False):
        """
        Establish a connection to the MySQL database. Accepts several
        arguments:

        host: Host where the database server is located
        user: Username to log in as
        password: Password to use.
        database: Database to use, None to not use a particular one.
        port: MySQL port to use, default is usually OK.
        unix_socket: Optionally, you can use a unix socket rather than TCP/IP.
        charset: Charset you want to use.
        sql_mode: Default SQL_MODE to use.
        read_default_file:
            Specifies  my.cnf file to read these parameters from under the [client] section.
        conv:
            Decoders dictionary to use instead of the default one.
            This is used to provide custom marshalling of types. See converters.
        use_unicode:
            Whether or not to default to unicode strings.
            This option defaults to true for Py3k.
        client_flag: Custom flags to send to MySQL. Find potential values in constants.CLIENT.
        cursorclass: Custom cursor class to use.
        init_command: Initial SQL statement to run when connection is established.
        connect_timeout: Timeout before throwing an exception when connecting.
        ssl:
            A dict of arguments similar to mysql_ssl_set()'s parameters.
            For now the capath and cipher arguments are not supported.
        read_default_group: Group to read from in the configuration file.
        compress; Not supported
        named_pipe: Not supported
        no_delay: Disable Nagle's algorithm on the socket
        autocommit: Autocommit mode. None means use server default. (default: False)
        local_infile: Boolean to enable the use of LOAD DATA LOCAL command. (default: False)

        db: Alias for database. (for compatibility to MySQLdb)
        passwd: Alias for password. (for compatibility to MySQLdb)
        """
        if db is not None and database is None:
            database = db
        if passwd is not None and not password:
            password = passwd

        self.host = host
        self.port = port
        self.user = user or DEFAULT_USER
        self.password = password or ""
        self.db = database
        self.no_delay = no_delay
        self.unix_socket = unix_socket
        self.charset = charset
        self.client_flag = client_flag
        self._local_infile = local_infile

        self.key = None
        self.cert = None
        self.ca = None

        self.__defer_mode = defer_mode
        self.__deferred = []

        if compress or named_pipe:
            raise NotImplementedError("compress and named_pipe arguments are not supported")

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

        # Initialise aspects of the connection
        self.ssl = self._init_ssl(ssl)
        self._read_config(read_default_group, read_default_file)
        self._init_char_encoding(use_unicode)

        self._connect()

    def _init_ssl(self, ssl):
        if not ssl:
            return False

        if 'capath' in ssl or 'cipher' in ssl:
            raise NotImplementedError('ssl options capath and cipher are not supported')

        if not SSL_ENABLED:
            raise NotImplementedError("ssl module not found")

        self.key = ssl.get('key')
        self.cert = ssl.get('cert')
        self.ca = ssl.get('ca')

    def _read_config(self, read_default_group, read_default_file):
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

            self.host = _config("host", self.host)
            self.port = int(_config("port", self.port))
            self.user = _config("user", self.user)
            self.password = _config("password", self.password)
            self.db = _config("database", self.db)
            self.unix_socket = _config("socket", self.unix_socket)
            self.charset = _config("default-character-set", self.charset)

    def _init_char_encoding(self, force_unicode=None):
        if force_unicode is not None:
            self.use_unicode = force_unicode
        elif sys.version_info[0] > 2:
            self.use_unicode = True
        else:
            self.use_unicode = bool(self.charset)

        if not self.charset:
            self.charset = DEFAULT_CHARSET

        self.encoding = charset_by_name(self.charset).encoding

    def close(self):
        ''' Send the quit message and close the socket '''
        if self.socket is None:
            raise Error("Already closed")
        send_data = struct.pack('<iB', 1, COMMAND.COM_QUIT)
        try:
            self._write_bytes(send_data)
        except Exception:
            pass
        finally:
            sock = self.socket
            self.socket = None
            self._rfile = None
            sock.close()

    @property
    def open(self):
        return self.socket is not None

    def __del__(self):
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
        self.socket = None
        self._rfile = None

    def autocommit(self, value):
        ac_vars = self._autocommit_vars(value)
        self._set_variables(ac_vars)

    def _autocommit_vars(self, value):
        self.autocommit_mode = bool(value)
        current = self.get_autocommit()

        if value == current:
            return []
        return [("AUTOCOMMIT", self.autocommit_mode)]

    def get_autocommit(self):

        # TODO This code should be more descriptive
        if self.__deferred:
            self._execute_command(COMMAND.COM_QUERY, "")

        return bool(self.server_status &
                    SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT)

    def _read_ok_packet(self):
        pkt = self._read_packet()
        if not pkt.is_ok_packet():
            raise OperationalError(2014, "Command Out of Sync")
        ok = OKPacketWrapper(pkt)
        self.server_status = ok.server_status
        return ok

    def _set_variables(self, pairs):
        if not pairs:
            return
        sql = self._set_variables_sql(pairs)
        self._execute_query(sql)

    def _set_variables_sql(self, pairs):
        '''Form sql to set a collection of variables.'''
        if not pairs:
            return ''
        parts = []
        for variable, value in pairs:
            parts.append("%s = %s" % (variable, self.escape(value)))
        query = "SET " + ", ".join(parts)

        return query

    def begin(self):
        """Begin transaction."""
        if self.__defer_mode:
            self.__deferred.append("BEGIN")
        else:
            self._execute_query("BEGIN")

    def commit(self):
        ''' Commit changes to stable storage '''
        self._execute_query("COMMIT")

    def rollback(self):
        ''' Roll back the current transaction '''
        self._execute_query("ROLLBACK")

    def show_warnings(self):
        """SHOW WARNINGS"""
        self._execute_command(COMMAND.COM_QUERY, "SHOW WARNINGS")
        result = MySQLResult(self)
        result.read()
        return result.rows

    def select_db(self, db):
        '''Set current db'''
        self._execute_command(COMMAND.COM_INIT_DB, db)
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
        self._execute_command(COMMAND.COM_QUERY, sql)
        self._affected_rows = self._read_query_result(unbuffered=unbuffered)
        return self._affected_rows

    def next_result(self, unbuffered=False):
        self._affected_rows = self._read_query_result(unbuffered=unbuffered)
        return self._affected_rows

    def affected_rows(self):
        return self._affected_rows

    def kill(self, thread_id):
        arg = struct.pack('<I', thread_id)
        self._execute_command(COMMAND.COM_PROCESS_KILL, arg)
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
            self._execute_command(COMMAND.COM_PING, "")
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

        self._set_variables([("NAMES", charset)])

        self.charset = charset
        self.encoding = encoding

    def _get_socket(self):

        sock = None
        if self.unix_socket and self.host in ('localhost', '127.0.0.1'):
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(self.connect_timeout)
            sock.connect(self.unix_socket)
            self.host_info = "Localhost via UNIX socket"
            if DEBUG: print('connected using unix_socket')
        else:
            while True:
                try:
                    sock = socket.create_connection(
                        (self.host, self.port), self.connect_timeout)
                    break
                except (OSError, IOError) as e:
                    if e.errno == errno.EINTR:
                        continue
                    raise
            self.host_info = "socket %s:%d" % (self.host, self.port)
            if DEBUG: print('connected using socket')

        sock.settimeout(None)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if self.no_delay:
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        return sock

    def _connect(self):
        sock = None
        try:
            self.socket = sock = self._get_socket()
            self._rfile = _makefile(sock, 'rb')

            # Server --> Client
            self._get_server_information()

            # Client --> Server
            self._request_authentication()

            # Send initialisation query
            self._send_init_query()

        except BaseException as e:
            self._rfile = None
            if sock is not None:
                try:
                    sock.close()
                except socket.error:
                    pass
            if isinstance(e, err.MySQLError):
                raise
            raise OperationalError(
                2003,
                "Can't connect to MySQL server on %r (%s)" % (self.host, e))

    def _read_packet(self, packet_type=MysqlPacket):
        """Read an entire "mysql packet" in its entirety from the network
        and return a MysqlPacket type that represents the results.
        """
        buff = b''
        while True:
            packet_header = self._read_bytes(4)
            if DEBUG: dump_packet(packet_header)
            btrl, btrh, packet_number = struct.unpack('<HBB', packet_header)
            bytes_to_read = btrl + (btrh << 16)
            #TODO: check sequence id
            recv_data = self._read_bytes(bytes_to_read)
            if DEBUG: dump_packet(recv_data)
            buff += recv_data
            if bytes_to_read < MAX_PACKET_LEN:
                break
        packet = packet_type(buff, self.encoding)
        packet.check_error()
        return packet

    def _read_bytes(self, num_bytes):
        while True:
            try:
                data = self._rfile.read(num_bytes)
                break
            except (IOError, OSError) as e:
                if e.errno == errno.EINTR:
                    continue
                raise OperationalError(
                    2013,
                    "Lost connection to MySQL server during query (%s)" % (e,))
        if len(data) < num_bytes:
            raise OperationalError(
                2013, "Lost connection to MySQL server during query")
        return data

    def _write_bytes(self, data):
        try:
            self.socket.sendall(data)
        except IOError as e:
            raise OperationalError(2006, "MySQL server has gone away (%r)" % (e,))

    def _read_query_result(self, unbuffered=False):
        if unbuffered:
            try:
                result = MySQLResult(self)
                result.init_unbuffered_query()
            except:
                result.unbuffered_active = False
                result.connection = None
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

        deferred_parts = self.__deferred[:]

        if self.__deferred:
            deferred_sql = ";".join(deferred_parts)
            self.__deferred = []

            if command == COMMAND.COM_QUERY:
                sql = deferred_sql + "; " + sql
            else:
                self._execute_query(deferred_sql)

        if not self.socket:
            raise InterfaceError("(0, '')")

        # If the last query was unbuffered, make sure it finishes before
        # sending new commands
        if self._result is not None and self._result.unbuffered_active:
            warnings.warn("Previous unbuffered result was left incomplete")
            self._result._finish_unbuffered_query()

        if isinstance(sql, text_type):
            sql = sql.encode(self.encoding)

        chunk_size = min(MAX_PACKET_LEN, len(sql) + 1)  # +1 is for command

        prelude = struct.pack('<iB', chunk_size, command)
        self._write_bytes(prelude + sql[:chunk_size-1])
        if DEBUG: dump_packet(prelude + sql)

        if chunk_size < MAX_PACKET_LEN:
            pass
        else:
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

        if command == COMMAND.COM_QUERY:
            for _ in deferred_parts:
                self._read_query_result()

    def _execute_query(self, query):
        """ Execute a query """
        self._execute_command(COMMAND.COM_QUERY, query)
        self._read_ok_packet()

    def _execute_query(self, query):
        """ Execute a query """
        self._execute_command(COMMAND.COM_QUERY, query)
        self._read_ok_packet()

    def _client_flag_for_features(self):

        client_flag = CLIENT.CAPABILITIES | CLIENT.MULTI_STATEMENTS

        if self._local_infile:
            client_flag |= CLIENT.LOCAL_FILES

        if self.ssl:
            client_flag |= CLIENT.SSL

        if self.db:
            client_flag |= CLIENT.CONNECT_WITH_DB

        if self.server_version.startswith('5'):
            client_flag |= CLIENT.MULTI_RESULTS

        return client_flag

    def _request_authentication(self):

        self.client_flag |= self._client_flag_for_features()

        if self.user is None:
            raise ValueError("Did not specify a username")

        charset_id = charset_by_name(self.charset).id
        if isinstance(self.user, text_type):
            self.user = self.user.encode(self.encoding)

        data_init = struct.pack('<iIB23s', self.client_flag, 1, charset_id, b'')

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

        auth_packet = self._read_packet()

        # if old_passwords is enabled the packet will be 1 byte long and
        # have the octet 254

        if auth_packet.is_eof_packet():
            # send legacy handshake
            data = _scramble_323(self.password.encode('latin1'), self.salt) + b'\0'
            data = pack_int24(len(data)) + int2byte(next_packet) + data
            self._write_bytes(data)
            auth_packet = self._read_packet()

    def _send_init_query(self):
        sql_var = []
        if self.sql_mode is not None:
            sql_var.append(('sql_mode', self.sql_mode))

        if self.autocommit_mode is not None:
            sql_var.extend(self._autocommit_vars(self.autocommit_mode))

        init_sql = [self._set_variables_sql(sql_var)]

        if self.__defer_mode and self.init_command is None:
            self.__deferred.extend(init_sql)
        else:
            sql = ";".join(init_sql)
            self._execute_query(sql)

        if self.init_command is not None:
            c = self.cursor()
            c.execute(self.init_command)
            self.commit()

    # _mysql support
    def thread_id(self):
        return self.server_thread_id

    def character_set_name(self):
        return self.charset

    def get_host_info(self):
        return self.host_info

    def get_proto_info(self):
        return self.protocol_version

    def _get_server_information(self):
        packet = self._read_packet()
        data = packet.get_all_data()
        if DEBUG: dump_packet(data)

        # Parse basic
        basics_size, parsed_basics = _connection_protocol.parse_basics(data)
        pos = basics_size

        # Bail if all data has been consumed
        if len(data) > basics_size:
            # Parse aditional info
            size, parsed_additional = _connection_protocol.parse_additional(data[pos:])
            pos += size

            if DEBUG: print("server_status: %x" % parsed_additional.status_flags)

            # Some data needs interpretation
            capabilities = (parsed_basics.capabilities_lower
                            | parsed_additional.capabilities_upper << 16)

            if capabilities & CLIENT.SECURE_CONNECTION:

                if DEBUG: print("salt_len:", parsed_additional.auth_plugin_data_length)

                # Null teminated auth_plugin_data
                data_length = max(13, parsed_additional.auth_plugin_data_length - 8)
                auth_plugin_data_part2 = data[pos:pos+data_length - 1]

                salt = parsed_basics.auth_plugin_data_part1 + auth_plugin_data_part2
                pos += data_length

            # Currently not used... but part of the protocol spec.
            # if capabilities & CLIENT_PLUGIN_AUTH:
            #   plugin_auth_expr = _expand_expression("<z", data[pos:])

        else:
            raise NotImplementedError()

        self.protocol_version = parsed_basics.protocol_version
        self.server_version = parsed_basics.server_version
        self.server_thread_id = parsed_basics.server_thread_id
        self.server_capabilities = capabilities
        self.server_language = parsed_additional.character_set
        self.server_charset = charset_by_id(self.server_language).name
        self.server_status = parsed_additional.status_flags
        self.salt = salt

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


class MySQLResult(object):

    def __init__(self, connection):
        self.connection = connection
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
        try:
            first_packet = self.connection._read_packet()

            if first_packet.is_ok_packet():
                self._read_ok_packet(first_packet)
            elif first_packet.is_load_local_packet():
                self._read_load_local_packet(first_packet)
            else:
                self._read_result_packet(first_packet)
        finally:
            self.connection = False

    def init_unbuffered_query(self):
        self.unbuffered_active = True
        first_packet = self.connection._read_packet()

        if first_packet.is_ok_packet():
            self._read_ok_packet(first_packet)
            self.unbuffered_active = False
            self.connection = None
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
        self.has_next = ok_packet.has_next

    def _read_load_local_packet(self, first_packet):
        load_packet = LoadLocalPacketWrapper(first_packet)
        sender = LoadLocalFile(load_packet.filename, self.connection)
        sender.send_data()

        ok_packet = self.connection._read_packet()
        if not ok_packet.is_ok_packet():
            raise OperationalError(2014, "Commands Out of Sync")
        self._read_ok_packet(ok_packet)

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
            self.connection = None
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
                self.connection = None  # release reference to kill cyclic reference.

    def _read_rowdata_packet(self):
        """Read a rowdata packet for each data row in the result set."""
        rows = []
        while True:
            packet = self.connection._read_packet()
            if self._check_packet_is_eof(packet):
                self.connection = None  # release reference to kill cyclic reference.
                break
            rows.append(self._read_row_from_packet(packet))

        self.affected_rows = len(rows)
        self.rows = tuple(rows)

    def _read_row_from_packet(self, packet):
        row = []
        for encoding, converter in self.converters:
            data = packet.read_length_coded_string()
            if data is not None:
                if encoding is not None:
                    data = data.decode(encoding)
                if DEBUG: print("DEBUG: DATA = ", data)
                if converter is not None:
                    data = converter(data)
            row.append(data)
        return tuple(row)

    def _get_descriptions(self):
        """Read a column descriptor packet for each column in the result."""
        self.fields = []
        self.converters = []
        use_unicode = self.connection.use_unicode
        description = []
        for i in range_type(self.field_count):
            field = self.connection._read_packet(FieldDescriptorPacket)
            self.fields.append(field)
            description.append(field.description())
            field_type = field.type_code
            if use_unicode:
                if field_type in TEXT_TYPES:
                    charset = charset_by_id(field.charsetnr)
                    if charset.is_binary:
                        # TEXTs with charset=binary means BINARY types.
                        encoding = None
                    else:
                        encoding = charset.encoding
                else:
                    encoding = 'ascii'
            else:
                encoding = None
            converter = self.connection.decoders.get(field_type)
            if converter is through:
                converter = None
            if DEBUG: print("DEBUG: field={}, converter={}".format(field, converter))
            self.converters.append((encoding, converter))

        eof_packet = self.connection._read_packet()
        assert eof_packet.is_eof_packet(), 'Protocol error, expecting EOF'
        self.description = tuple(description)


class LoadLocalFile(object):
    def __init__(self, filename, connection):
        self.filename = filename
        self.connection = connection

    def send_data(self):
        """Send data packets from the local file to the server"""
        if not self.connection.socket:
            raise InterfaceError("(0, '')")

        # sequence id is 2 as we already sent a query packet
        seq_id = 2
        try:
            with open(self.filename, 'rb') as open_file:
                chunk_size = MAX_PACKET_LEN
                prelude = b""
                packet = b""
                packet_size = 0

                while True:
                    chunk = open_file.read(chunk_size)
                    if not chunk:
                        break
                    packet = struct.pack('<i', len(chunk))[:3] + int2byte(seq_id)
                    format_str = '!{0}s'.format(len(chunk))
                    packet += struct.pack(format_str, chunk)
                    self.connection._write_bytes(packet)
                    seq_id += 1
        except IOError:
            raise OperationalError(1017, "Can't find file '{0}'".format(self.filename))
        finally:
            # send the empty packet to signify we are done sending data
            packet = struct.pack('<i', 0)[:3] + int2byte(seq_id)
            self.connection._write_bytes(packet)

# g:khuno_ignore='E226,E301,E701'
