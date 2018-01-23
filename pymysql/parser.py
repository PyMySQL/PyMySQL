from __future__ import print_function
from ._compat import PY2, range_type, text_type, str_type

from collections import namedtuple
from struct import unpack_from, Struct

from .charset import MBLENGTH
from .constants import FIELD_TYPE, SERVER_STATUS
from . import err
from .util import byte2int

DEBUG = False

Packet = namedtuple('Packet', ['size', 'seq_id', 'payload'])

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
    _s, _d = read(data, length, offset=offset+size)
    return size + _s, _d

def read_string(data, offset=0):
    end = data.find(b'\0', offset)
    if end >= 0:
        sl = data[offset:end]
        return len(sl), sl

def is_ok_packet(packet):
    # https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
    return read_uint8(packet.payload)[1] == 0 and packet.size >= 7

def is_eof_packet(packet):
    # http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-EOF_Packet
    # Caution: \xFE may be LengthEncodedInteger.
    # If \xFE is LengthEncodedInteger header, 8bytes followed.
    return read_uint8(packet.payload)[1] == 254 and packet.size < 9

def is_auth_switch_request(packet):
    # http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
    return read_uint8(packet.payload)[1] == 254

def is_load_local(packet):
    return read_uint8(packet.payload)[1] == 251

def is_resultset_packet(packet):
    return 1 <= read_uint8(packet.payload)[1] <= 250

def check_error(packet):
    if read_uint8(packet.payload)[1] == 255:
        errno = read_uint16(packet.payload, offset=1)[1]
        if DEBUG: print("errno = ", errno)
        err.raise_mysql_exception(packet.payload)

class MysqlPacket(object):
    """Representation of a MySQL response packet.

    Provides an interface for reading/parsing the packet results.
    """
    __slots__ = ('_position', '_data', '_packet')

    def __init__(self, data, encoding):
        self._position = 0
        self._data = data
        self._packet = Packet(len(data), None, payload=data)

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
        return is_ok_packet(self._packet)

    def is_eof_packet(self):
        return is_eof_packet(self._packet)

    def is_auth_switch_request(self):
        return is_auth_switch_request(self._packet)

    def is_resultset_packet(self):
        return is_resultset_packet(self._packet)

    def is_load_local_packet(self):
        return is_load_local(self._packet)

    def check_error(self):
        check_error(self._packet)

    def dump(self):
        dump_packet(self._data)


def parse_field_descriptor_packet(packet, encoding=DEFAULT_CHARSET):
    pos = 0
    data = packet.payload

    size, catalog = read_length_coded_string(data, offset=pos)
    pos += size

    size, db = read_length_coded_string(data, offset=pos)
    pos += size

    size, table_name = read_length_coded_string(data, offset=pos)
    pos += size

    size, org_table = read_length_coded_string(data, offset=pos)
    pos += size

    size, name = read_length_coded_string(data, offset=pos)
    pos += size

    size, org_name = read_length_coded_string(data, offset=pos)
    pos += size

    charsetnr, length, type_code, flags, scale = unpack_from('<xHIBHBxx', data, offset=pos)

    if encoding:
        table_name = table_name.decode(encoding)
        org_table = org_table.decode(encoding)
        name = name.decode(encoding)
        org_name = org_name.decode(encoding)

    result = {'catalog': catalog,
            'db': db,
            'table_name': table_name,
            'org_table': org_table,
            'name': name,
            'org_name': org_name,
            'charsetnr': charsetnr,
            'length': length,
            'type_code': type_code,
            'flags': flags,
            'scale': scale}

    column_length = length
    if type_code == FIELD_TYPE.VAR_STRING:
        column_length //=  MBLENGTH.get(charsetnr, 1)

    result['description'] = (result['name'],
                             type_code,
                             None,           # TODO: display length. should this be length.
                             column_length,  # internal size
                             column_length,  # precision
                             scale,
                             flags % 2 == 0)
    return result

def parse_ok_packet(packet):
    if not is_ok_packet(packet):
        raise InvalidPacketError()

    pos = 1
    data = packet.payload

    size, affected_rows = read_length_encoded_integer(data, offset=pos)
    pos += size

    size, insert_id = read_length_encoded_integer(data, offset=pos)
    pos += size

    server_status, warning_count = unpack_from('<HH', data, offset=pos)
    pos += 4

    # Read the rest of the packet
    message = read(data, None, offset=pos)
    #pos += len(message)

    return {'affected_rows': affected_rows,
            'insert_id': insert_id,
            'server_status': server_status,
            'warning_count': warning_count,
            'message': message,
            'has_next': server_status & SERVER_STATUS.SERVER_MORE_RESULTS_EXISTS}

def parse_eof_packet(packet):
    if not is_eof_packet(packet):
        raise InvalidPacketError()

    pos = 1
    data = packet.payload

    warning_count, server_status = unpack_from('<hh', data, offset=pos)
    pos += 4

    if DEBUG: print("server_status=", server_status)
    return {'warning_count': warning_count,
            'server_status': server_status,
            'has_next': server_status & SERVER_STATUS.SERVER_MORE_RESULTS_EXISTS}