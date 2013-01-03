# Python implementation of the MySQL client-server protocol
#   http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol

import hashlib
sha_new = lambda *args, **kwargs: hashlib.new("sha1", *args, **kwargs)

import socket
import ssl

import struct
import sys
import os
try:
    import ConfigParser
except ImportError:
    import configparser

import getpass
DEFAULT_USER = getpass.getuser()

from cymysql.charset import charset_by_name, charset_by_id
try:
    from cymysql.cursorsx import Cursor
except ImportError:
    from cymysql.cursors import Cursor
from cymysql.constants.CLIENT import *
from cymysql.constants.COMMAND import *
from cymysql.converters import escape_item, encoders, decoders
from cymysql.err import raise_mysql_exception, Warning, Error, \
     InterfaceError, DataError, DatabaseError, OperationalError, \
     IntegrityError, InternalError, NotSupportedError, ProgrammingError
try:
    from cymysql.packetx import MysqlPacket, FieldDescriptorPacket, MySQLResult
except ImportError:
    from cymysql.packet import MysqlPacket, FieldDescriptorPacket, MySQLResult

PYTHON3 = sys.version_info[0] > 2

DEBUG = False

DEFAULT_CHARSET = 'latin1'

def byte2int(b):
    if isinstance(b, int):
        return b
    else:
        return ord(b)

def int2byte(i):
    if PYTHON3:
        return bytes([i])
    else:
        return chr(i)

def pack_int24(n):
    if PYTHON3:
        return bytes([n&0xFF, (n>>8)&0xFF,(n>>16)&0xFF])
    else:
        return chr(n&0xFF) + chr((n>>8)&0xFF) + chr((n>>16)&0xFF)

def dump_packet(data):
    
    def is_ascii(data):
        if byte2int(data) >= 65 and byte2int(data) <= 122: #data.isalnum():
            return data
        return '.'
    print("packet length %d" % len(data))
    print("method call[1]: %s" % sys._getframe(1).f_code.co_name)
    print("method call[2]: %s" % sys._getframe(2).f_code.co_name)
    print("method call[3]: %s" % sys._getframe(3).f_code.co_name)
    print("method call[4]: %s" % sys._getframe(4).f_code.co_name)
    print("method call[5]: %s" % sys._getframe(5).f_code.co_name)
    print("-" * 88)
    dump_data = [data[i:i+16] for i in range(len(data)) if i%16 == 0]
    for d in dump_data:
        print(' '.join(map(lambda x:"%02X" % byte2int(x), d)) + \
                '   ' * (16 - len(d)) + ' ' * 2 + \
                ' '.join(map(lambda x:"%s" % is_ascii(x), d)))
    print("-" * 88)
    print("")

def _scramble(password, message):
    if password == None or len(password) == 0:
        return int2byte(0)
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
    for i in range(length):
        x = (struct.unpack('B', message1[i:i+1])[0] ^ \
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
    outbuf = StringIO()
    for _ in range(min(SCRAMBLE_LENGTH_323, len(message))):
        outbuf.write(int2byte(int(rand_st.my_rnd() * 31) + 64))
    extra = int2byte(int(rand_st.my_rnd() * 31))
    out = outbuf.getvalue()
    outbuf = StringIO()
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



class Connection(object):
    """
    Representation of a socket with a mysql server.

    The proper way to get an instance of this class is to call
    connect()."""

    def errorhandler(connection, cursor, errorclass, errorvalue):
        err = errorclass, errorvalue
        if DEBUG:
            raise

        if cursor:
            cursor.messages.append(err)
        else:
            connection.messages.append(err)
        del cursor
        del connection

        if not issubclass(errorclass, Error):
            raise Error(errorclass, errorvalue)
        else:
            raise errorclass(errorvalue[0], errorvalue[1])


    def __init__(self, host="localhost", user=None, passwd="",
                 db=None, port=3306, unix_socket=None,
                 charset='', sql_mode=None,
                 read_default_file=None, conv=decoders, use_unicode=None,
                 client_flag=0, cursorclass=Cursor, init_command=None,
                 connect_timeout=None, ssl=None, read_default_group=None,
                 compress=None, named_pipe=None):
        """
        Establish a connection to the MySQL database. Accepts several
        arguments:

        host: Host where the database server is located
        user: Username to log in as
        passwd: Password to use.
        db: Database to use, None to not use a particular one.
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
        """

        if use_unicode is None and sys.version_info[0] > 2:
            use_unicode = True

        if compress or named_pipe:
            raise NotImplementedError("compress and named_pipe arguments are not supported")

        if ssl and (ssl.has_key('capath') or ssl.has_key('cipher')):
            raise NotImplementedError('ssl options capath and cipher are not supported')

        self.ssl = False
        if ssl:
            self.ssl = True
            client_flag |= SSL
            for k in ('key', 'cert', 'ca'):
                v = None
                if ssl.has_key(k):
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

            cfg = ConfigParser.RawConfigParser()
            cfg.read(os.path.expanduser(read_default_file))

            def _config(key, default):
                try:
                    return cfg.get(read_default_group,key)
                except:
                    return default

            user = _config("user",user)
            passwd = _config("password",passwd)
            host = _config("host", host)
            db = _config("db",db)
            unix_socket = _config("socket",unix_socket)
            port = _config("port", port)
            charset = _config("default-character-set", charset)

        self.host = host
        self.port = port
        self.user = user or DEFAULT_USER
        self.password = passwd
        self.db = db
        self.unix_socket = unix_socket
        if charset:
            self.charset = charset
            self.use_unicode = True
        else:
            self.charset = DEFAULT_CHARSET
            self.use_unicode = False

        if use_unicode is not None:
            self.use_unicode = use_unicode

        client_flag |= CAPABILITIES
        client_flag |= MULTI_STATEMENTS
        if self.db:
            client_flag |= CONNECT_WITH_DB
        self.client_flag = client_flag

        self.cursorclass = cursorclass
        self.connect_timeout = connect_timeout

        self._connect()

        self.messages = []
        self.set_charset(charset)
        self.encoders = encoders
        self.decoders = conv

        self._result = None
        self._affected_rows = 0
        self.host_info = "Not connected"

        self.autocommit(False)

        if sql_mode is not None:
            c = self.cursor()
            c.execute("SET sql_mode=%s", (sql_mode,))

        self.commit()

        if init_command is not None:
            c = self.cursor()
            c.execute(init_command)

            self.commit()


    def close(self):
        ''' Send the quit message and close the socket '''
        if self.socket is None:
            raise Error("Already closed")
        send_data = struct.pack('<i',1) + int2byte(COM_QUIT)
        self.socket.sendall(send_data)
        self.socket.close()
        self.socket = None

    def autocommit(self, value):
        ''' Set whether or not to commit after every execute() '''
        try:
            self._execute_command(COM_QUERY, "SET AUTOCOMMIT = %s" % \
                                      self.escape(value))
            self.read_packet()
        except:
            exc,value,tb = sys.exc_info()
            self.errorhandler(None, exc, value)

    def commit(self):
        ''' Commit changes to stable storage '''
        try:
            self._execute_command(COM_QUERY, "COMMIT")
            self.read_packet()
        except:
            exc,value,tb = sys.exc_info()
            self.errorhandler(None, exc, value)

    def rollback(self):
        ''' Roll back the current transaction '''
        try:
            self._execute_command(COM_QUERY, "ROLLBACK")
            self.read_packet()
        except:
            exc,value,tb = sys.exc_info()
            self.errorhandler(None, exc, value)

    def escape(self, obj):
        ''' Escape whatever value you pass to it  '''
        return escape_item(obj, self.charset)

    def literal(self, obj):
        ''' Alias for escape() '''
        return escape_item(obj, self.charset)

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
    def query(self, sql):
        if DEBUG:
            print("sending query: %s" % sql)
        self._execute_command(COM_QUERY, sql)
        self._affected_rows = self._read_query_result()
        return self._affected_rows

    def next_result(self):
        self._affected_rows = self._read_query_result()
        return self._affected_rows

    def affected_rows(self):
        return self._affected_rows

    def kill(self, thread_id):
        arg = struct.pack('<I', thread_id)
        try:
            self._execute_command(COM_PROCESS_KILL, arg)
        except:
            exc,value,tb = sys.exc_info()
            self.errorhandler(None, exc, value)
            return
        pkt = self.read_packet()
        return pkt.is_ok_packet()

    def ping(self, reconnect=True):
        ''' Check if the server is alive '''
        try:
            self._execute_command(COM_PING, "")
        except:
            if reconnect:
                self._connect()
                return self.ping(False)
            else:
                exc,value,tb = sys.exc_info()
                self.errorhandler(None, exc, value)
                return

        pkt = self.read_packet()
        return pkt.is_ok_packet()

    def set_charset(self, charset):
        try:
            if charset:
                self._execute_command(COM_QUERY, "SET NAMES %s" %
                                      self.escape(charset))
                self.read_packet()
                self.charset = charset
        except:
            exc,value,tb = sys.exc_info()
            self.errorhandler(None, exc, value)

    def _connect(self):
        try:
            if self.unix_socket and (self.host == 'localhost' or self.host == '127.0.0.1'):
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                t = sock.gettimeout()
                sock.settimeout(self.connect_timeout)
                sock.connect(self.unix_socket)
                sock.settimeout(t)
                self.host_info = "Localhost via UNIX socket"
                if DEBUG: print('connected using unix_socket')
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                t = sock.gettimeout()
                sock.settimeout(self.connect_timeout)
                sock.connect((self.host, self.port))
                sock.settimeout(t)
                self.host_info = "socket %s:%d" % (self.host, self.port)
                if DEBUG: print('connected using socket')
            self.socket = sock
            self.sock_fd = sock.fileno()
            self._get_server_information()
            self._request_authentication()
        except socket.error as e:
            raise OperationalError(2003, "Can't connect to MySQL server on %r (%s)" % (self.host, e.args[0]))

    def read_packet(self, packet_type=MysqlPacket):
      """Read an entire "mysql packet" in its entirety from the network
      and return a MysqlPacket type that represents the results."""

      packet = packet_type(self)
      _errno, _data = packet.check_error()
      if _errno:
        raise_mysql_exception(_data)
      return packet

    def _read_query_result(self):
        result = MySQLResult(self)
        result.read()
        self._result = result
        return result.affected_rows

    def insert_id(self):
        if self._result:
            return self._result.insert_id
        else:
            return 0

    def _send_command(self, command, sql):
        #send_data = struct.pack('<i', len(sql) + 1) + command + sql
        # could probably be more efficient, at least it's correct
        if not self.socket:
            self.errorhandler(None, InterfaceError, "(0, '')")

        if ((PYTHON3 and isinstance(sql, str)) or 
            (not PYTHON3 and  isinstance(sql, unicode))):
            sql = sql.encode(self.charset)

        prelude = struct.pack('<i', len(sql)+1) + int2byte(command)
        self.socket.sendall(prelude + sql)
        if DEBUG: dump_packet(prelude + sql)

    def _execute_command(self, command, sql):
        self._send_command(command, sql)
        
    def _request_authentication(self):
        self._send_authentication()

    def _send_authentication(self):
        self.client_flag |= CAPABILITIES
        if self.server_version.startswith('5'):
            self.client_flag |= MULTI_RESULTS

        if self.user is None:
            raise ValueError("Did not specify a username")

        charset_id = charset_by_name(self.charset).id
        self.user = self.user.encode(self.charset)

        data_init = struct.pack('<i', self.client_flag) + struct.pack("<I", 1) + \
                     int2byte(charset_id) + int2byte(0)*23

        next_packet = 1

        if self.ssl:
            data = pack_int24(len(data_init)) + int2byte(next_packet) + data_init
            next_packet += 1

            if DEBUG: dump_packet(data)

            self.socket.sendall(data)
            self.socket = ssl.wrap_self.socketet(self.socket, keyfile=self.key,
                                                 certfile=self.cert,
                                                 ssl_version=ssl.PROTOCOL_TLSv1,
                                                 cert_reqs=ssl.CERT_REQUIRED,
                                                 ca_certs=self.ca)
            self.sock_fd = -1

        data = data_init + self.user+int2byte(0) + _scramble(self.password.encode(self.charset), self.salt)

        if self.db:
            self.db = self.db.encode(self.charset)
            data += self.db + int2byte(0)

        data = pack_int24(len(data)) + int2byte(next_packet) + data
        next_packet += 2

        if DEBUG: dump_packet(data)

        self.socket.sendall(data)

        auth_packet = MysqlPacket(self)
        _errno, _data = auth_packet.check_error()
        if _errno:
            raise_mysql_exception(_data)
        if DEBUG: dump_packet(auth_packet.get_all_data())

        # if old_passwords is enabled the packet will be 1 byte long and
        # have the octet 254

        if auth_packet.is_eof_packet():
            # send legacy handshake
            #raise NotImplementedError, "old_passwords are not supported. Check to see if mysqld was started with --old-passwords, if old-passwords=1 in a my.cnf file, or if there are some short hashes in your mysql.user table."
            # TODO: is this the correct charset?
            data = _scramble_323(self.password.encode(self.charset), self.salt.encode(self.charset)) + int2byte(0)
            data = pack_int24(len(data)) + int2byte(next_packet) + data

            self.socket.sendall(data)
            auth_packet = MysqlPacket(self)
            _errno, _data = auth_packet.check_error()
            if _errno:
                raise_mysql_exception(_data)
            if DEBUG: dump_packet(auth_packet.get_all_data())

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
        data = packet.get_all_data()

        if DEBUG: dump_packet(data)
        #packet_len = byte2int(data[i:i+1])
        #i += 4
        self.protocol_version = byte2int(data[i:i+1])

        i += 1
        server_end = data.find(int2byte(0), i)
        # TODO: is this the correct charset? should it be default_charset?
        self.server_version = data[i:server_end].decode(self.charset)

        i = server_end + 1
        self.server_thread_id = struct.unpack('<h', data[i:i+2])

        i += 4
        self.salt = data[i:i+8]

        i += 9
        if len(data) >= i + 1:
            i += 1

        self.server_capabilities = struct.unpack('<h', data[i:i+2])[0]

        i += 1
        self.server_language = byte2int(data[i:i+1])
        self.server_charset = charset_by_id(self.server_language).name

        i += 16
        if len(data) >= i+12-1:
            rest_salt = data[i:i+12]
            self.salt += rest_salt

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
