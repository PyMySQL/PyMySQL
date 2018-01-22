from __future__ import print_function
from ._compat import PY2, range_type, text_type, str_type

from struct import unpack_from, Struct

from .charset import MBLENGTH
from .constants import FIELD_TYPE
from . import err
from .util import byte2int

DEBUG = False

NULL_COLUMN = 251
UNSIGNED_CHAR_COLUMN = 251
UNSIGNED_SHORT_COLUMN = 252
UNSIGNED_INT24_COLUMN = 253
UNSIGNED_INT64_COLUMN = 254

DEFAULT_CHARSET = 'latin1'

MAX_PACKET_LEN = 2**24-1

def dump_packet(data): # pragma: no cover
    def is_ascii(data):
        if 65 <= byte2int(data) <= 122:
            if isinstance(data, int):
                return chr(data)
            return data
        return '.'

    try:
        print("packet length:", len(data))
        for i in range(1, 6):
            f = sys._getframe(i)
            print("call[%d]: %s (line %d)" % (i, f.f_code.co_name, f.f_lineno))
        print("-" * 66)
    except ValueError:
        pass
    dump_data = [data[i:i+16] for i in range_type(0, min(len(data), 256), 16)]
    for d in dump_data:
        print(' '.join(map(lambda x: "{:02X}".format(byte2int(x)), d)) +
              '   ' * (16 - len(d)) + ' ' * 2 +
              ''.join(map(lambda x: "{}".format(is_ascii(x)), d)))
    print("-" * 66)
    print()


class InvalidPacketError(Exception):
    pass

if PY2:
    def read_uint8(data, offset=0):
        return 1, ord(data[offset])
else:
    def read_uint8(data, offset=0):
        return 1, data[offset]

def read_uint16(data, offset=0):
    return 2, unpack_from('<H', data, offset)[0]

def read_uint24(data, offset=0):
    low, high = unpack_from('<HB', data, offset)
    return 3, low + (high << 16)

def read_uint32(data, offset=0):
    return 4, unpack_from('<I', data, offset)[0]

def read_uint64(data, offset=0):
    return 8, unpack_from('<Q', data, offset)[0]

def read(data, nbytes, offset=0):
    if nbytes is None:
        # nbytes == len(result) by definition
        sl = data[offset:]
        return len(sl), sl
    else:
        result = data[offset:offset+nbytes]

        if len(result) == nbytes:
            return nbytes, result

        error = ('Result length not requested length:\n'
                 'Expected=%s  Actual=%s  Position: %s  Data Length: %s'
                 % (nbytes, len(result), offset, len(data)))
        if DEBUG:
            print(error)
        raise AssertionError(error)


def read_length_encoded_integer(data, offset=0):
    """
    Read a length encoded integer.
    Args:
        data:
        offset:

    Returns:
        (size, int):
    """
    size, col = read_uint8(data, offset=offset)
    if col == NULL_COLUMN:
        return size, None

    if col < UNSIGNED_CHAR_COLUMN:
        return size, col
    elif col == UNSIGNED_SHORT_COLUMN:
        _s, _d = read_uint16(data, offset=offset)
    elif col == UNSIGNED_INT24_COLUMN:
        _s, _d = read_uint24(data, offset=offset)
    elif col == UNSIGNED_INT64_COLUMN:
        _s, _d = read_uint64(data, offset=offset)
    else:
        raise ValueError
    return (size + _s, _d)

def read_length_coded_string(data, offset=0):
    size, length = read_length_encoded_integer(data, offset=offset)
    if length is None:
        return size, None
    _s, _d = read(data, length, offset=offset+length)
    return size + _s, _d

def read_string(data, offset=0):
    end = data.find(b'\0', offset)
    if end >= 0:
        sl = data[offset:end]
        return len(sl), sl

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
        _s, _d = read(self._data, size, offset=self._position)
        self._position += _s
        return _d

    def read_all(self):
        """Read all remaining data in the packet.

        (Subsequent read() will return errors.)
        """
        _s, _d = read(self._data, None, offset=self._position)
        self._position = None
        return _d

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
        _s, _d = read(self._data, length, offset=position)
        return _d

    def read_uint8(self):
        _s, _d = read_uint8(self._data, offset=self._position)
        self._position += _s
        return _d

    def read_uint16(self):
        _s, _d = read_uint16(self._data, offset=self._position)
        self._position += _s
        return _d

    def read_uint24(self):
        _s, _d = read_uint24(self._data, offset=self._position)
        self._position += _s
        return _d

    def read_uint32(self):
        _s, _d = read_uint32(self._data, offset=self._position)
        self._position += _s
        return _d

    def read_uint64(self):
        _s, _d = read_uint64(self._data, offset=self._position)
        self._position += _s
        return _d

    def read_string(self):
        _s, _d = read_string(self._data, self._position)
        self._position += _s
        return _d

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
        s = Struct(fmt)
        result = s.unpack_from(self._data, self._position)
        self._position += s.size
        return result

    def is_ok_packet(self):
        # https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
        return read_uint8(self._data)[1] == 0 and len(self._data) >= 7

    def is_eof_packet(self):
        # http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-EOF_Packet
        # Caution: \xFE may be LengthEncodedInteger.
        # If \xFE is LengthEncodedInteger header, 8bytes followed.
        return read_uint8(self._data)[1] == 254 and len(self._data) < 9

    def is_auth_switch_request(self):
        # http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
        return read_uint8(self._data)[1] == 254

    def is_resultset_packet(self):
        return 1 <= read_uint8(self._data)[1] <= 250

    def is_load_local_packet(self):
        return read_uint8(self._data)[1] == 251

    def is_error_packet(self):
        return read_uint8(self._data)[1] == 255

    def check_error(self):
        _s, _d = read_uint8(self._data)
        if _d == 255:
            self._position = 1
            _s, errno = read_uint16(self._data, offset=self._position)
            if DEBUG: print("errno =", errno)
            err.raise_mysql_exception(self._data)

    def dump(self):
        dump_packet(self._data)


class FieldDescriptorPacket(MysqlPacket):
    """A MysqlPacket that represents a specific column's metadata in the result.

    Parsing is automatically done and the results are exported via public
    attributes on the class such as: db, table_name, name, length, type_code.
    """

    def __init__(self, data, encoding):
        MysqlPacket.__init__(self, data, encoding)
        self._parse_field_descriptor(encoding)

    def _parse_field_descriptor(self, encoding):
        """Parse the 'Field Descriptor' (Metadata) packet.

        This is compatible with MySQL 4.1+ (not compatible with MySQL 4.0).
        """
        self.catalog = self.read_length_coded_string()
        self.db = self.read_length_coded_string()
        self.table_name = self.read_length_coded_string().decode(encoding)
        self.org_table = self.read_length_coded_string().decode(encoding)
        self.name = self.read_length_coded_string().decode(encoding)
        self.org_name = self.read_length_coded_string().decode(encoding)
        self.charsetnr, self.length, self.type_code, self.flags, self.scale = unpack_from(
            '<xHIBHBxx', self._data, self._position)
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
