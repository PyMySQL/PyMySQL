import sys
import struct
import socket
import sha
import re

from pymysql.cursor import Cursor
from pymysql.charset import MBLENGTH
from pymysql.converters import escape_item, encoders, decoders
from pymysql.constants import FIELD_TYPE
from pymysql.constants import SERVER_STATUS
from pymysql.constants.CLIENT_FLAG import *
from pymysql.constants.COMMAND import *
from pymysql.exceptions import raise_mysql_exception, Warning, Error, InterfaceError, DataError, \
             DatabaseError, OperationalError, IntegrityError, InternalError, \
            NotSupportedError, ProgrammingError

DEBUG = False

NULL_COLUMN = 251
UNSIGNED_CHAR_COLUMN = 251
UNSIGNED_SHORT_COLUMN = 252
UNSIGNED_INT24_COLUMN = 253
UNSIGNED_INT32_COLUMN = 254
UNSIGNED_CHAR_LENGTH = 1
UNSIGNED_SHORT_LENGTH = 2
UNSIGNED_INT24_LENGTH = 3
UNSIGNED_INT32_LENGTH = 4
UNSIGNED_INT32_PAD_LENGTH = 4

DEFAULT_CHARSET = 'latin1'
BUFFER_SIZE = 256*256*256-1

def dump_packet(data):
    
    def is_ascii(data):
        if data.isalnum():
            return data
        return '.'
    print "packet length %d" % len(data)
    print "method call: %s \npacket dump" % sys._getframe(2).f_code.co_name
    print "-" * 88
    dump_data = [data[i:i+16] for i in xrange(len(data)) if i%16 == 0]
    for d in dump_data:
        print ' '.join(map(lambda x:"%02X" % ord(x), d)) + \
                '   ' * (16 - len(d)) + ' ' * 2 + ' '.join(map(lambda x:"%s" % is_ascii(x), d))
    print "-" * 88
    print ""

def _scramble(password, message):
    if password == None or len(password) == 0:
        return '\0'
    if DEBUG: print 'password=' + password
    stage1 = sha.new(password).digest()
    stage2 = sha.new(stage1).digest()
    s = sha.new()
    s.update(message)
    s.update(stage2)
    result = s.digest()
    return _my_crypt(result, stage1)

def _my_crypt(message1, message2):
    length = len(message1)
    result = struct.pack('B', length)
    for i in xrange(length):
        x = (struct.unpack('B', message1[i:i+1])[0] ^ struct.unpack('B', message2[i:i+1])[0])
        result += struct.pack('B', x)
    return result

def pack_int24(n):
    return struct.pack('BBB', n&0xFF, (n>>8)&0xFF, (n>>16)&0xFF)

def unpack_int24(n):
    return struct.unpack('B',n[0])[0] + (struct.unpack('B', n[1])[0] << 8) +\
        (struct.unpack('B',n[2])[0] << 16)

def unpack_int32(n):
    return struct.unpack('B',n[0])[0] + (struct.unpack('B', n[1])[0] << 8) +\
        (struct.unpack('B',n[2])[0] << 16) + (struct.unpack('B', n[3])[0] << 24)

def unpack_int64(n):
    return struct.unpack('B',n[0])[0] + (struct.unpack('B', n[1])[0]<<8) +\
    (struct.unpack('B',n[2])[0] << 16) + (struct.unpack('B',n[3])[0]<<24)+\
    (struct.unpack('B',n[4])[0] << 32) + (struct.unpack('B',n[5])[0]<<40)+\
    (struct.unpack('B',n[6])[0] << 48) + (struct.unpack('B',n[7])[0]<<56)

def defaulterrorhandler(connection, cursor, errorclass, errorvalue):
    err = errorclass, errorvalue
    
    if cursor:
        cursor.messages.append(err)
    else:
        connection.messages.append(err)
    del cursor
    del connection
    raise errorclass, errorvalue

class Connection(object):
    
    errorhandler = defaulterrorhandler

    def __init__(self, *args, **kwargs):
        self.host = kwargs['host']
        self.port = kwargs.get('port', 3306)
        self.user = kwargs['user']
        self.password = kwargs['passwd']
        self.db = kwargs.get('db', None)
        self.unix_socket = kwargs.get('unix_socket', None)
        self.charset = DEFAULT_CHARSET
        
        client_flag = CLIENT_CAPABILITIES
        #client_flag = kwargs.get('client_flag', None)
        client_flag |= CLIENT_MULTI_STATEMENTS
        if self.db:
            client_flag |= CLIENT_CONNECT_WITH_DB
        self.client_flag = client_flag
        
        self._connect()
        
        charset = kwargs.get('charset', None)
        self.set_chatset_set(charset)
        self.messages = []
        self.encoders = encoders
        self.decoders = decoders
        
        self.autocommit(False)
        

    def close(self):
        send_data = struct.pack('<i',1) + COM_QUIT
        sock = self.socket
        sock.send(send_data)
        sock.close()
    
    def autocommit(self, value):
        self._execute_command(COM_QUERY, "SET AUTOCOMMIT = %s" % \
                self.escape(value))
        self._read_and_check_packet()

    def commit(self):
        self._execute_command(COM_QUERY, "COMMIT")
        self._read_and_check_packet()

    def rollback(self):
        self._execute_command(COM_QUERY, "ROLLBACK")
        self._read_and_check_packet()

    def escape(self, obj):
        return escape_item(obj)

    def cursor(self):
        return Cursor(self)
    
    def __enter__(self):
        return self.cursor()

    def __exit__(self, exc, value, traceback):
        if exc:
            self.rollback()
        else:
            self.commit()
    
    def _query(self, sql):
        self._execute_command(COM_QUERY, sql)
        return self._read_query_result()
    
    def next_result(self):
        return self._read_query_result()

    def set_chatset_set(self, charset):
        sock = self.socket
        if charset and self.charset != charset:
            self._execute_command(COM_QUERY, "SET NAMES %s" % charset)
            self._read_and_check_packet()
            self.charset = charset     

    def _connect(self):
        if self.unix_socket and (self.host == 'localhost' or self.host == '127.0.0.1'):
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(self.unix_socket)
            if DEBUG: print 'connected using unix_socket'
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((self.host, self.port))
            if DEBUG: print 'connected using socket'
        self.socket = sock
        self._get_server_information()
        self._request_authentication()
    
    def _read_and_check_packet(self):    
        recv_data = self.socket.recv(BUFFER_SIZE)
        if DEBUG: dump_packet(recv_data)
        self._check_error(recv_data)
        return recv_data

    def _read_query_result(self):
        recv_data = self._read_and_check_packet()
        result = MySQLResult(self, recv_data)
        result.read()
        self._result = result

        affected_rows = result.affected_rows
        if not result.ok_packet:
            affected_rows = len(result.rows)
            self._result.affected_rows = affected_rows

        return affected_rows

    def _send_command(self, command, sql):
        send_data = struct.pack('<i', len(sql) + 1) + command + sql
        sock = self.socket
        sock.send(send_data)
        if DEBUG: dump_packet(send_data)

    def _execute_command(self, command, sql):
        self._send_command(command, sql)
        
    def _check_error(self, recv_data):
        field_count = ord(recv_data[4:5])
        if field_count == 255:
            errno = ord(recv_data[5:6]) + ord(recv_data[6:7]) * 256
            if DEBUG: print "errno = %d" % errno
            raise_mysql_exception(recv_data)
    
    def _is_ok_packet(self, recv_data):
        field_count = ord(recv_data[4:5])
        if field_count == 0:
            return True
        return False

    def _is_resultset_packet(self, recv_data):
        field_count = ord(recv_data[4:5])
        if field_count > 1:
            return True
        return False
    
    def _request_authentication(self):
        sock = self.socket
        self._send_authentication()

    def _send_authentication(self):
        sock = self.socket
        self.client_flag |= CLIENT_CAPABILITIES
        if self.server_version.startswith('5'):
            self.client_flag |= CLIENT_MULTI_RESULTS
    
        data = (struct.pack('i', self.client_flag)) + "\0\0\0\x01" + \
                '\x08' + '\0'*23 + \
                self.user+"\0" + _scramble(self.password, self.salt)
        
        if self.db:
            data += self.db + "\0"
        
        data = pack_int24(len(data)) + "\x01" + data
        
        if DEBUG: dump_packet(data)
        
        sock.send(data)
        
        auth_msg = sock.recv(BUFFER_SIZE)
        self._check_auth_packet(auth_msg)
        
    def _check_auth_packet(self, recv_data):
        if DEBUG: dump_packet(recv_data)
        self._check_error(recv_data)

    def _get_server_information(self):
        sock = self.socket
        i = 0
        data = sock.recv(BUFFER_SIZE)
        if DEBUG: dump_packet(data)
        packet_len = ord(data[i:i+1])
        i += 4
        self.protocol_version = ord(data[i:i+1])
        
        i += 1
        server_end = data.find("\0", i)
        self.server_version = data[i:server_end]
        
        i = server_end + 1
        self.server_thread_id = struct.unpack('h', data[i:i+2])

        i += 4
        self.salt = data[i:i+8]
        
        i += 9
        if len(data) >= i + 1:
            i += 1
       
        self.sever_capabilities = struct.unpack('h', data[i:i+2])
        
        i += 1
        self.sever_language = ord(data[i:i+1])
        
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

class MySQLResult(object):

    def __init__(self, connection, data):
        from weakref import proxy
        self.connection = proxy(connection)
        self.data = data
        self.position = 0        
        self.affected_rows = None
        self.insert_id = None
        self.server_status = 0
        self.warning_count = 0
        self.message = None
        self.field_count = 0
        self.ok_packet = connection._is_ok_packet(data)
        self.description = None
        self.rows = None
        self.has_next = None
        if not self.ok_packet:
            self._check_has_more_packet()

    def read(self):
        if self.ok_packet:
            self._read_ok_packet()
        else:
            self._read_result_packet()
        self.data = None

    def _read_ok_packet(self):
        self.position += 5
        self.affected_rows = self._get_field_length()
        self.insert_id = self._get_field_length()
        self.server_status = struct.unpack('H',self.data[self.position:self.position+2])[0]
        self.position += 2
        self.warning_count = struct.unpack('H',self.data[self.position:self.position+2])[0]
        self.position += 2
        self.message = self.data[self.position:]
    
    def _check_has_more_packet(self):
        packet_len = unpack_int24(self.data[:3])
        length = len(self.data) - 4
        while length < packet_len:
            d = self.connection.socket.recv(BUFFER_SIZE)
            length += len(d)
            self.data += d

    def _read_result_packet(self):
        self._get_field_count()
        self._get_description()
        self._read_rowdata_packet()

    def _read_rowdata_packet(self):
        rows = []
        not_eof = True
        while(not_eof):
            row = []
            next = ord(self.data[self.position:self.position+1])
            if next == 254:
                self.position += 3
                server_status = struct.unpack('h', self.data[-2:])[0]
                self.has_next = server_status & SERVER_STATUS.SERVER_MORE_RESULTS_EXISTS
                not_eof = False
            else:
	            for field in self.description:
	                type_code = field[1]
	                converter = self.connection.decoders[type_code]
	                if DEBUG: print "DEBUG: field=" + str(field[0]) + ", type_code=" + str(type_code) + ", converter=" + str(converter)
	                data = self._seek_and_get_string()
	                converted = None
	                if data != None:
	                    converted = converter(data)
	                row.append(converted)
	            rows.append(tuple(row))
	            self.position += 4

        self.rows = tuple(rows)
        if DEBUG: self.rows

    def _get_field_count(self):
        self.position += 4
        pos = self.position
        count = ord(self.data[pos:pos+1])
        self.field_count = count
        self.position += 5

    def _get_description(self):
        data = self.data
        pos = self.position
        description = []
        for i in xrange(self.field_count):
            desc = []
            catalog = self._seek_and_get_string()   
            db = self._seek_and_get_string()
            table_name = self._seek_and_get_string()
            org_table = self._seek_and_get_string()
            name = self._seek_and_get_string()
            desc.append(name)
            org_name = self._seek_and_get_string()
            #filler
            self.position += 1
            #charsetnr
            charsetnr = struct.unpack('<h',
                    data[self.position:self.position+2])[0]
            self.position += 2
            #length
            length = struct.unpack('<i', data[self.position:self.position+4])
            self.position += 4
            
            type = ord(data[self.position:self.position+1])
            desc.append(type)
            desc.append(None)
            self.position += 1
            
            desc.append(self._get_column_length(type, charsetnr, length[0]))
            desc.append(self._get_column_length(type, charsetnr, length[0]))

            #flags
            flags = struct.unpack('<h', data[self.position:self.position+2])
            flags = int(("%02X" % flags)[1:])
             
            self.position += 2
            scale = ord(data[self.position:self.position+1])
            desc.append(scale)
            
            self.position += 1
            if flags % 2 == 0:
                desc.append(1) 
            else:
                desc.append(0)
            #filler
            self.position += 2
            self.position += 4
            description.append(tuple(desc))
        
        self.position += 9
        self.description = tuple(description)
    
    def _get_column_length(self, type, charsetnr, length):
        if type == FIELD_TYPE.VAR_STRING:
            mblen = MBLENGTH.get(charsetnr, 1)
            return length / mblen
        return length

    def _seek_and_get_string(self):
        length = self._get_field_length()
        if length : 
            str = self.data[self.position:self.position+length]
            self.position += length
            return str
        return None
    
    def _get_field_length(self, longlong=False):
        data = self.data
        pos = self.position
        c = ord(data[pos:pos + 1])
        self.position += UNSIGNED_CHAR_LENGTH
        if c == NULL_COLUMN:
            return None
        if c < UNSIGNED_CHAR_COLUMN:
            return c
        elif c == UNSIGNED_SHORT_COLUMN:
            length = struct.unpack('<H', data[pos:pos+UNSIGNED_SHORT_LENGTH])
            self.position += UNSIGNED_SHORT_LENGTH
            return length
        elif c == UNSIGNED_INT24_COLUMN:
            length = unpack_int24(data[pos:pos+UNSIGNED_INT24_COLUMN])
            self.position += UNSIGNED_INT24_LENGTH
            return length
        else:
            length = 0
            if longlong:
                length = unpack_int64(data[pos:pos+UNSIGNED_INT32_LENGTH*2])
            else:
                length = unpack_int32(data[pos:pos+UNSIGNED_INT32_LENGTH])
            
            self.position += UNSIGNED_INT32_LENGTH
            self.position += UNSIGNED_INT32_PAD_LENGTH
            return length


