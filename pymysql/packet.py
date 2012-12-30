# Python implementation of the MySQL client-server protocol
#   http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol

import struct
import sys
import os

from pymysql.err import OperationalError
from pymysql.charset import MBLENGTH
from pymysql.constants import FIELD_TYPE

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

def byte2int(b):
    if isinstance(b, int):
        return b
    else:
        return struct.unpack("!B", b)[0]

def int2byte(i):
    return struct.pack("!B", i)

def pack_int24(n):
    return struct.pack('BBB', n&0xFF, (n>>8)&0xFF, (n>>16)&0xFF)

def unpack_uint16(n):
  return struct.unpack('<H', n[0:2])[0]


# TODO: stop using bit-shifting in these functions...
# TODO: rename to "uint" to make it clear they're unsigned...
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

class MysqlPacket(object):
  """Representation of a MySQL response packet.  Reads in the packet
  from the network socket, removes packet header and provides an interface
  for reading/parsing the packet results."""

  def __init__(self, connection):
    self.connection = connection
    self.__position = 0
    self.__recv_packet()

  def __recv_packet(self):
    """Parse the packet header and read entire packet payload into buffer."""
    packet_header = self.connection.rfile.read(4)
    if len(packet_header) < 4:
        raise OperationalError(2013, "Lost connection to MySQL server during query")

    if DEBUG: dump_packet(packet_header)
    packet_length_bin = packet_header[:3]
    self.__packet_number = byte2int(packet_header[3])
    # TODO: check packet_num is correct (+1 from last packet)

    bin_length = packet_length_bin + int2byte(0)  # pad little-endian number
    bytes_to_read = struct.unpack('<I', bin_length)[0]
    recv_data = self.connection.rfile.read(bytes_to_read)
    if len(recv_data) < bytes_to_read:
        raise OperationalError(2013, "Lost connection to MySQL server during query")
    if DEBUG: dump_packet(recv_data)
    self.__data = recv_data

  def packet_number(self): return self.__packet_number

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
    self.__position = None  # ensure no subsequent read() or peek()
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
      raise Exception("Invalid position to rewind cursor to: %s." % position)
    self.__position = position

  def peek(self, size):
    """Look at the first 'size' bytes in packet without moving cursor."""
    result = self.__data[self.__position:(self.__position+size)]
    if len(result) != size:
      error = ('Result length not requested length:\n'
               'Expected=%s.  Actual=%s.  Position: %s.  Data Length: %s'
               % (size, len(result), self.__position, len(self.__data)))
      if DEBUG:
        print(error)
        self.dump()
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
    c = byte2int(self.read(1))
    if c == NULL_COLUMN:
      return None
    if c < UNSIGNED_CHAR_COLUMN:
      return c
    elif c == UNSIGNED_SHORT_COLUMN:
      return unpack_uint16(self.read(UNSIGNED_SHORT_LENGTH))
    elif c == UNSIGNED_INT24_COLUMN:
      return unpack_int24(self.read(UNSIGNED_INT24_LENGTH))
    elif c == UNSIGNED_INT64_COLUMN:
      # TODO: what was 'longlong'?  confirm it wasn't used?
      return unpack_int64(self.read(UNSIGNED_INT64_LENGTH))

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
    return byte2int(self.get_bytes(0)) == 0

  def is_eof_packet(self):
    return byte2int(self.get_bytes(0)) == 254  # 'fe'

  def is_resultset_packet(self):
    field_count = byte2int(self.get_bytes(0))
    return field_count >= 1 and field_count <= 250

  def is_error_packet(self):
    return byte2int(self.get_bytes(0)) == 255

  def check_error(self):
    if self.is_error_packet():
      self.rewind()
      self.advance(1)  # field_count == error (we already know that)
      errno = unpack_uint16(self.read(2))
      if DEBUG: print("errno = %d" % errno)
      return errno, self.__data
    return 0, None

  def dump(self):
    dump_packet(self.__data)


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
    return ('%s %s.%s.%s, type=%s'
            % (self.__class__, self.db, self.table_name, self.name,
               self.type_code))

