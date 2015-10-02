# Python implementation of the MySQL client-server protocol
#   http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol

import sys
import struct
from cymysql.err import raise_mysql_exception, OperationalError
from cymysql.constants import SERVER_STATUS, FLAG
from cymysql.converters import convert_characters
from cymysql.charset import charset_by_id

from libc.stdint cimport uint16_t, uint32_t
cdef int PYTHON3 = sys.version_info[0] > 2

MBLENGTH = {
        8:1,
        33:3,
        88:2,
        91:2
        }

cdef int FIELD_TYPE_VAR_STRING=253

cdef int UNSIGNED_CHAR_COLUMN = 251
cdef int UNSIGNED_SHORT_COLUMN = 252
cdef int UNSIGNED_INT24_COLUMN = 253
cdef int UNSIGNED_INT64_COLUMN = 254

cdef int SERVER_MORE_RESULTS_EXISTS = SERVER_STATUS.SERVER_MORE_RESULTS_EXISTS

cdef uint16_t unpack_uint16(bytes s):
    cdef unsigned char* n = s
    return n[0] + (n[1] << 8)

cdef uint32_t unpack_uint24(bytes s):
    cdef unsigned char* n = s
    return n[0] + (n[1] << 8) + (n[2] << 16)

cdef uint32_t unpack_uint32(bytes s):
    cdef unsigned char* n = s
    return n[0] + (n[1] << 8) + (n[2] << 16) + (n[3] << 24)

cdef long long unpack_uint64(bytes n):
    return struct.unpack('<Q', n)[0]

cdef class MysqlPacket(object):
    """Representation of a MySQL response packet.  Reads in the packet
    from the network socket, removes packet header and provides an interface
    for reading/parsing the packet results."""
    cdef object connection, _socket, _charset, _use_unicode
    cdef bytes __data
    cdef int __position

    def __init__(self, connection):
        cdef int is_error
        cdef int errno
        self.connection = connection
        self._socket = connection.socket
        self._charset = connection.charset
        self._use_unicode = connection.use_unicode
        self.__position = 0
        self.__recv_packet()
        is_error = (<unsigned char>(self.__data[0])) == 0xff
        if is_error:
            self.__position += 1  # field_count == error (we already know that)
            errno = unpack_uint16(self._read(2))
            raise_mysql_exception(self.__data)

    cdef bytes __recv_from_socket(self, int size):
        cdef bytes r
        cdef int recieved

        r = b''
        while size:
            recv_data = self._socket.recv(size)
            if not recv_data:
                break
            size -= len(recv_data)
            r += recv_data
        return r

    cdef __recv_packet(self):
        """Parse the packet header and read entire packet payload into buffer."""
        cdef bytes packet_header, recv_data
        cdef int bytes_to_read

        packet_header = self.__recv_from_socket(4)
        if len(packet_header) < 4:
            raise OperationalError(2013, "Lost connection to MySQL server during query")

        bytes_to_read = unpack_uint24(packet_header)
        # TODO: check packet_num is correct (+1 from last packet)
        # self.packet_number = ord(packet_header[3:])
  
        recv_data = self.__recv_from_socket(bytes_to_read)
        if len(recv_data) < bytes_to_read:
            raise OperationalError(2013, "Lost connection to MySQL server during query")

        self.__data = recv_data
  
    def get_all_data(self): return self.__data

    cpdef read(self, size):
        return self._read(size)

    cdef bytes _read(self, int size):
        """Read the first 'size' bytes in packet and advance cursor past them."""
        self.__position += size
        return self.__data[self.__position-size:self.__position]

    cdef void _skip(self, int size):
        """Read the first 'size' bytes in packet and advance cursor past them."""
        self.__position += size

    cdef _read_all(self):
        """Read all remaining data in the packet.

        (Subsequent read() or peek() will return errors.)
        """
        self.__position = -1  # ensure no subsequent read() or peek()
        return self.__data[self.__position:]
  
    cdef int read_length_coded_binary(self):
        """Read a 'Length Coded Binary' number from the data buffer.

        Length coded numbers can be anywhere from 1 to 9 bytes depending
        on the value of the first byte.
        """
        cdef unsigned char c = self._read(1)[0]
        if c == UNSIGNED_CHAR_COLUMN:
            return -1
        elif c == UNSIGNED_SHORT_COLUMN:
            return unpack_uint16(self._read(2))
        elif c == UNSIGNED_INT24_COLUMN:
            return unpack_uint24(self._read(3))
        elif c == UNSIGNED_INT64_COLUMN:
            return unpack_uint64(self._read(8))
        else:
            return c
  
    cdef bytes _read_length_coded_string(self):
        """Read a 'Length Coded String' from the data buffer.

        A 'Length Coded String' consists first of a length coded
        (unsigned, positive) integer represented in 1-9 bytes followed by
        that many bytes of binary data.  (For example "cat" would be "3cat".)
        """
        cdef int length 
        length = self.read_length_coded_binary()
        if length < 0:
            return None
        return self._read(length)

    cdef read_decode_data(self, fields, decoders):
        return tuple([
            None if value is None
            else decoder(value, self._charset, field, self._use_unicode)
                if decoder is convert_characters
                else decoder(value)
            for value, field, decoder in [(self._read_length_coded_string(), f, decoders.get(f.type_code)) for f in fields]
        ])

    cpdef is_ok_packet(self):
        return (<unsigned char>(self.__data[0])) == 0

    cpdef is_eof_packet(self):
        return (<unsigned char>(self.__data[0])) == 0xfe

    cpdef is_eof_and_status(self):
        if (<unsigned char>(self.__data[0])) != 0xfe:
           return False, 0, 0
        return True, unpack_uint16(self._read(2)), unpack_uint16(self._read(2))

    cpdef read_ok_packet(self):
        cdef int affected_rows, insert_id, server_status, warning_count
        cdef message
        self._skip(1)  # field_count (always '0')
        affected_rows = self.read_length_coded_binary()
        insert_id = self.read_length_coded_binary()
        server_status = unpack_uint16(self._read(2))
        warning_count = unpack_uint16(self._read(2))
        message = self._read_all()
        return (None if affected_rows < 0 else affected_rows,
                None if insert_id < 0 else insert_id,
                server_status, warning_count, message)


cdef class FieldDescriptorPacket(MysqlPacket):
    """A MysqlPacket that represents a specific column's metadata in the result.

    Parsing is automatically done and the results are exported via public
    attributes on the class such as: db, table_name, name, length, type_code.
    """
    cdef public object catalog, db, table_name, org_table, name, org_name
    cdef public int charsetnr, length, type_code, flags, scale, is_set, is_binary
    cdef public str charset

    def __init__(self, *args):
        MysqlPacket.__init__(self, *args)
        self.__parse_field_descriptor()

    def __parse_field_descriptor(self):
        """Parse the 'Field Descriptor' (Metadata) packet.
    
        This is compatible with MySQL 4.1+ (not compatible with MySQL 4.0).
        """
        self.catalog = self._read_length_coded_string()
        self.db = self._read_length_coded_string()
        self.table_name = self._read_length_coded_string()
        self.org_table = self._read_length_coded_string()
        self.name = self._read_length_coded_string().decode(self.connection.charset)
        self.org_name = self._read_length_coded_string()
        self._skip(1)  # non-null filler
        self.charsetnr = unpack_uint16(self._read(2))
        self.charset = charset_by_id(self.charsetnr).name
        self.length = unpack_uint32(self._read(4))
        self.type_code = ord(self._read(1))
        self.flags = unpack_uint16(self._read(2))
        self.is_set = self.flags & FLAG.SET
        self.is_binary = self.flags & FLAG.BINARY
        self.scale = ord(self._read(1))  # "decimals"
        self._skip(2)  # filler (always 0x00)
    
        # 'default' is a length coded binary and is still in the buffer?
        # not used for normal result sets...

    def description(self):
        """Provides a 7-item tuple compatible with the Python PEP249 DB Spec."""
        return (
            self.name,
            self.type_code,
            None,
            self.get_column_length(),
            self.get_column_length(),
            self.scale,
            <int>(self.flags % 2 == 0),
        )

    def get_column_length(self):
        cdef int mblen
        if self.type_code == FIELD_TYPE_VAR_STRING:
            mblen = MBLENGTH.get(self.charsetnr, 1)
            return self.length // mblen
        return self.length

    def __str__(self):
        return ('%s %s.%s.%s, type=%s'
            % (self.__class__, self.db, self.table_name, self.name,
               self.type_code))


# TODO: move OK and EOF packet parsing/logic into a proper subclass
#       of MysqlPacket like has been done with FieldDescriptorPacket.
cdef class MySQLResult(object):
    cdef public object affected_rows, insert_id, rest_rows, has_result
    cdef public object message, description
    cdef object connection, first_packet, fields
    cdef public int has_next
    cdef int rest_row_index, server_status, warning_count, field_count

    def __init__(self, connection):
        from weakref import proxy
        self.connection = proxy(connection)
        self.affected_rows = None
        self.insert_id = None
        self.server_status = 0
        self.warning_count = 0
        self.message = None
        self.field_count = 0
        self.description = None
        self.has_next = 0
        self.has_result = False
        self.rest_rows = None
        self.rest_row_index = 0
        self.first_packet = MysqlPacket(self.connection)

        if self.first_packet.is_ok_packet():
            (self.affected_rows, self.insert_id,
                self.server_status, self.warning_count,
                self.message) = self.first_packet.read_ok_packet()
            self.has_result = False
        else:
            self.field_count = ord(self.first_packet.read(1))
            self._get_descriptions()
            self.has_result = True
            self.read_rest_rowdata_packet()

    def read_rest_rowdata_packet(self):
        """Read rest rowdata packets for each data row in the result set."""
        cdef int is_eof, warning_count, server_status
        if (not self.has_result) or (self.rest_rows is not None):
            return
        rest_rows = []
        decoder = self.connection.conv
        while True:
            packet = MysqlPacket(self.connection)
            is_eof, warning_count, server_status = packet.is_eof_and_status()
            if is_eof:
                self.warning_count = warning_count
                self.server_status = server_status
                self.has_next = (server_status & SERVER_MORE_RESULTS_EXISTS)
                break
            rest_rows.append(packet.read_decode_data(self.fields, decoder))
        self.rest_rows = rest_rows
        self.rest_row_index = 0

    cdef void _get_descriptions(self):
        """Read a column descriptor packet for each column in the result."""
        cdef int i
        cdef object eof_packet
        self.fields = []
        description = []
        for i in range(self.field_count):
            field = FieldDescriptorPacket(self.connection)
            self.fields.append(field)
            description.append(field.description())

        eof_packet = MysqlPacket(self.connection)
        assert eof_packet.is_eof_packet(), 'Protocol error, expecting EOF'
        self.description = tuple(description)

    def fetchone(self):
        cdef int is_eof, warning_count, server_status
        if not self.has_result:
            return None
        if self.rest_rows is None:
            packet = MysqlPacket(self.connection)
            is_eof, warning_count, server_status = packet.is_eof_and_status()
            if is_eof:
                self.warning_count = warning_count
                self.server_status = server_status
                self.has_next = (server_status & SERVER_MORE_RESULTS_EXISTS)
                self.rest_rows = []
                return None
            return packet.read_decode_data(self.fields, self.connection.conv)
        elif len(self.rest_rows) != self.rest_row_index:
            self.rest_row_index += 1
            return self.rest_rows[self.rest_row_index-1]
        return None
