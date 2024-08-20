# Python implementation of the MySQL client-server protocol
#   https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html

import sys
import struct
from cymysql.err import raise_mysql_exception, OperationalError
from cymysql.constants import SERVER_STATUS, FLAG
from cymysql.converters import convert_characters, convert_json
from cymysql.charset import charset_by_id, encoding_by_charset

PYTHON3 = sys.version_info[0] > 2

FIELD_TYPE_VAR_STRING = 253

UNSIGNED_CHAR_COLUMN = 251
UNSIGNED_SHORT_COLUMN = 252
UNSIGNED_INT24_COLUMN = 253
UNSIGNED_INT64_COLUMN = 254

SERVER_MORE_RESULTS_EXISTS = SERVER_STATUS.SERVER_MORE_RESULTS_EXISTS


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


def unpack_uint64(n):
    return struct.unpack('<Q', n)[0]


class MysqlPacket(object):
    """Representation of a MySQL response packet.  Reads in the packet
    from the network socket, removes packet header and provides an interface
    for reading/parsing the packet results."""

    def __init__(self, data, charset, encoding, use_unicode):
        self._charset = charset
        self._encoding = encoding
        self._use_unicode = use_unicode
        self.__position = 0
        self.__data = data
        is_error = self.__data[0] == (0xff if PYTHON3 else b'\xff')
        if is_error:
            self.__position += 1  # field_count == error (we already know that)
            unpack_uint16(self._read(2))    # errno
            raise_mysql_exception(self.__data)

    def get_all_data(self):
        return self.__data

    def read(self, size):
        return self._read(size)

    def _read(self, size):
        """Read the first 'size' bytes in packet and advance cursor past them."""
        self.__position += size
        return self.__data[self.__position - size:self.__position]

    def _skip(self, size):
        """Read the first 'size' bytes in packet and advance cursor past them."""
        self.__position += size

    def _read_all(self):
        """Read all remaining data in the packet.

        (Subsequent read() or peek() will return errors.)
        """
        self.__position = -1  # ensure no subsequent read() or peek()
        return self.__data[self.__position:]

    def read_length_coded_binary(self):
        """Read a 'Length Coded Binary' number from the data buffer.

        Length coded numbers can be anywhere from 1 to 9 bytes depending
        on the value of the first byte.
        """
        c = ord(self._read(1))
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

    def _read_length_coded_string(self):
        """Read a 'Length Coded String' from the data buffer.

        A 'Length Coded String' consists first of a length coded
        (unsigned, positive) integer represented in 1-9 bytes followed by
        that many bytes of binary data.  (For example "cat" would be "3cat".)
        """
        length = self.read_length_coded_binary()
        if length < 0:
            return None
        return self._read(length)

    def read_decode_data(self, fields, decoders):
        return tuple([
            None if value is None
            else decoder(value, self._encoding, field, self._use_unicode)
            if decoder in (convert_characters, convert_json)
            else decoder(value)
            for value, field, decoder in [
                (self._read_length_coded_string(), f, decoders.get(f.type_code))
                for f in fields
            ]
        ])

    def is_ok_packet(self):
        return self.__data[0] == (0 if PYTHON3 else b'\x00')

    def is_eof_packet(self):
        return self.__data[0] == (0xfe if PYTHON3 else b'\xfe')

    def is_eof_and_status(self):
        if self.__data[0] != (0xfe if PYTHON3 else b'\xfe'):
            return False, 0, 0
        return True, unpack_uint16(self._read(2)), unpack_uint16(self._read(2))

    def read_ok_packet(self):
        self._skip(1)  # field_count (always '0')
        affected_rows = self.read_length_coded_binary()
        insert_id = self.read_length_coded_binary()
        server_status = unpack_uint16(self._read(2))
        warning_count = unpack_uint16(self._read(2))
        message = self._read_all()
        return (None if affected_rows < 0 else affected_rows,
                None if insert_id < 0 else insert_id,
                server_status, warning_count, message)


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
        self.catalog = self._read_length_coded_string()
        self.db = self._read_length_coded_string()
        self.table_name = self._read_length_coded_string()
        self.org_table = self._read_length_coded_string()
        self.name = self._read_length_coded_string().decode(self._encoding)
        self.org_name = self._read_length_coded_string()
        self._skip(1)  # non-null filler
        self.charsetnr = unpack_uint16(self._read(2))
        self.charset = charset_by_id(self.charsetnr).name
        self.encoding = encoding_by_charset(self.charset)
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
            1 if self.flags % 2 == 0 else 0,
        )

    def get_column_length(self):
        if self.type_code == FIELD_TYPE_VAR_STRING:
            mblen = {8: 1, 33: 3, 88: 2, 91: 2}.get(self.charsetnr, 1)
            return self.length // mblen
        return self.length

    def __str__(self):
        return ('%s %s.%s.%s, type=%s' % (
            self.__class__, self.db, self.table_name, self.name, self.type_code)
        )
