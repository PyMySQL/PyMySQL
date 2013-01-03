# Python implementation of the MySQL client-server protocol
#   http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol

import sys
from cymysql.err import OperationalError
from cymysql.constants import SERVER_STATUS

PYTHON3 = sys.version_info[0] > 2

MBLENGTH = {
        8:1,
        33:3,
        88:2,
        91:2
        }

FIELD_TYPE_VAR_STRING=253

NULL_COLUMN = 251
UNSIGNED_CHAR_COLUMN = 251
UNSIGNED_SHORT_COLUMN = 252
UNSIGNED_INT24_COLUMN = 253
UNSIGNED_INT64_COLUMN = 254
UNSIGNED_CHAR_LENGTH = 1
UNSIGNED_SHORT_LENGTH = 2
UNSIGNED_INT24_LENGTH = 3
UNSIGNED_INT64_LENGTH = 8


def unpack_uint16(n):
    if PYTHON3:
        return n[0] + (n[1] << 8)
    else:
        return ord(n[0]) + (ord(n[1]) << 8)

def unpack_uint24(n):
    if PYTHON3:
        return n[0] + (n[1] << 8) + (n[2] << 16)
    else:
        return ord(n[0]) + (ord(n[1]) << 8) + (ord(n[2]) << 16)

def unpack_uint32(n):
    if PYTHON3:
        return n[0] + (n[1] << 8) + (n[2] << 16) + (n[3] << 24)
    else:
        return ord(n[0]) + (ord(n[1]) << 8) + \
            (ord(n[2]) << 16) + (ord(n[3]) << 24)


class MysqlPacket(object):
    """Representation of a MySQL response packet.  Reads in the packet
    from the network socket, removes packet header and provides an interface
    for reading/parsing the packet results."""

    def __init__(self, connection):
        self.connection = connection
        self.sock_fd = connection.sock_fd
        self.__position = 0
        self.__recv_packet()

    def __recv_from_socket(self, size):
        r = b''
        while size:
            recv_data = self.connection.socket.recv(size)
            recieved = len(recv_data)
            if recieved == 0:
                break
            size -= len(recv_data)
            r += recv_data
        return r
  
    def __recv_packet(self):
        """Parse the packet header and read entire packet payload into buffer."""
        packet_header = self.__recv_from_socket(4)
        if len(packet_header) < 4:
            raise OperationalError(2013, "Lost connection to MySQL server during query")

        bytes_to_read = unpack_uint24(packet_header[:3])
        self.packet_number = ord(packet_header[3:])
        # TODO: check packet_num is correct (+1 from last packet)
  
        recv_data = self.__recv_from_socket(bytes_to_read)
        if len(recv_data) < bytes_to_read:
            raise OperationalError(2013, "Lost connection to MySQL server during query")

        self.__data = recv_data
  
    def get_all_data(self): return self.__data
  
    def read(self, size):
        """Read the first 'size' bytes in packet and advance cursor past them."""
        result = self.peek(size)
        self.advance(size)
        return result
  
    def read_all(self):
        """Read all remaining data in the packet.

        (Subsequent read() or peek() will return errors.)
        """
        result = self.__data[self.__position:]
        self.__position = -1  # ensure no subsequent read() or peek()
        return result
  
    def advance(self, length):
        """Advance the cursor in data buffer 'length' bytes."""
        new_position = self.__position + length
        if new_position < 0 or new_position > len(self.__data):
            raise Exception('Invalid advance amount (%s) for cursor.  '
                        'Position=%s' % (length, new_position))
        self.__position = new_position
  
    def rewind(self, position=0):
        """Set the position of the data buffer cursor to 'position'."""
        if position < 0 or position > len(self.__data):
            raise Exception(
                    "Invalid position to rewind cursor to: %s." % position)
        self.__position = position
  
    def peek(self, size):
        """Look at the first 'size' bytes in packet without moving cursor."""
        result = self.__data[self.__position:(self.__position+size)]
        if len(result) != size:
            error = ('Result length not requested length:\n'
                 'Expected=%s.  Actual=%s.  Position: %s.  Data Length: %s'
                 % (size, len(result), self.__position, len(self.__data)))
            raise AssertionError(error)
        return result
  
    def get_bytes(self, position, length=1):
        """Get 'length' bytes starting at 'position'.
  
        Position is start of payload (first four packet header bytes are not
        included) starting at index '0'.
  
        No error checking is done.  If requesting outside end of buffer
        an empty string (or string shorter than 'length') may be returned!
        """
        return self.__data[position:(position+length)]
  
    def read_length_coded_binary(self):
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
            return unpack_uint24(self.read(UNSIGNED_INT24_LENGTH))
        elif c == UNSIGNED_INT64_COLUMN:
            # TODO: what was 'longlong'?  confirm it wasn't used?
            pass
  
    def read_length_coded_string(self):
        """Read a 'Length Coded String' from the data buffer.

        A 'Length Coded String' consists first of a length coded
        (unsigned, positive) integer represented in 1-9 bytes followed by
        that many bytes of binary data.  (For example "cat" would be "3cat".)
        """
        length = self.read_length_coded_binary()
        if length is None:
            return None
        return self.read(length)
  
    def is_ok_packet(self):
        return ord(self.get_bytes(0)) == 0

    def is_eof_packet(self):
        return ord(self.get_bytes(0)) == 254  # 'fe'

    def is_resultset_packet(self):
        field_count = ord(self.get_bytes(0))
        return field_count >= 1 and field_count <= 250
  
    def check_error(self):
        if ord(self.get_bytes(0)) == 255:
            self.rewind()
            self.advance(1)  # field_count == error (we already know that)
            errno = unpack_uint16(self.read(2))
            return errno, self.__data
        return 0, None

    def read_ok_packet(self):
        self.advance(1)  # field_count (always '0')
        affected_rows = self.read_length_coded_binary()
        insert_id = self.read_length_coded_binary()
        server_status = unpack_uint16(self.read(2))
        warning_count = unpack_uint16(self.read(2))
        message = self.read_all()
        return (affected_rows, insert_id, server_status, warning_count, message)


class FieldDescriptorPacket(MysqlPacket):
    """A MysqlPacket that represents a specific column's metadata in the result.

    Parsing is automatically done and the results are exported via public
    attributes on the class such as: db, table_name, name, length, type_code.
    """

    def __init__(self, *args):
        MysqlPacket.__init__(self, *args)
        self.__parse_field_descriptor()

    def __parse_field_descriptor(self):
        """Parse the 'Field Descriptor' (Metadata) packet.
    
        This is compatible with MySQL 4.1+ (not compatible with MySQL 4.0).
        """
        self.catalog = self.read_length_coded_string()
        self.db = self.read_length_coded_string()
        self.table_name = self.read_length_coded_string()
        self.org_table = self.read_length_coded_string()
        self.name = self.read_length_coded_string().decode(self.connection.charset)
        self.org_name = self.read_length_coded_string()
        self.advance(1)  # non-null filler
        self.charsetnr = unpack_uint16(self.read(2))
        self.length = unpack_uint32(self.read(4))
        self.type_code = ord(self.read(1))
        self.flags = unpack_uint16(self.read(2))
        self.scale = ord(self.read(1))  # "decimals"
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
class MySQLResult(object):

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
        self.rows = None
        self.has_next = None

    def read(self):
        self.first_packet = self.connection.read_packet()

        # TODO: use classes for different packet types?
        if self.first_packet.is_ok_packet():
            self._read_ok_packet()
        else:
            self._read_result_packet()

    def _read_ok_packet(self):
        (self.affected_rows, self.insert_id,
            self.server_status, self.warning_count,
            self.message) = self.first_packet.read_ok_packet()

    def _read_result_packet(self):
        self.field_count = ord(self.first_packet.read(1))
        self._get_descriptions()
        self._read_rowdata_packet()

    def _field_data(self, packet, decoders, field):
        data = packet.read_length_coded_string()
        if data != None and field.type_code in decoders:
            return decoders[field.type_code](self.connection, field, data)
        return None

    # TODO: implement this as an iteratable so that it is more
    #       memory efficient and lower-latency to client...
    def _read_rowdata_packet(self):
      """Read a rowdata packet for each data row in the result set."""
      decoders = self.connection.decoders
      rows = []
      while True:
        packet = self.connection.read_packet()
        if packet.is_eof_packet():
            self.warning_count = packet.read(2)
            server_status = unpack_uint16(packet.read(2))
            self.has_next = (server_status
                             & SERVER_STATUS.SERVER_MORE_RESULTS_EXISTS)
            break
        rows.append(tuple([self._field_data(packet, decoders, self.fields[i])
                                            for i in range(len(self.fields))]))

      self.affected_rows = len(rows)
      self.rows = rows

    def _get_descriptions(self):
        """Read a column descriptor packet for each column in the result."""
        self.fields = []
        description = []
        for i in range(self.field_count):
            field = self.connection.read_packet(FieldDescriptorPacket)
            self.fields.append(field)
            description.append(field.description())

        eof_packet = self.connection.read_packet()
        assert eof_packet.is_eof_packet(), 'Protocol error, expecting EOF'
        self.description = tuple(description)
