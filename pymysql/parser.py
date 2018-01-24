from __future__ import print_function
from ._compat import PY2, range_type, text_type, str_type

from collections import namedtuple
from struct import unpack_from, Struct
from operator import itemgetter

from .charset import MBLENGTH
from .constants import FIELD_TYPE, SERVER_STATUS
from . import converters as _converters
from . import err
from .util import byte2int

DEBUG = False

Packet = namedtuple('Packet', ['size', 'seq_id', 'payload'])
psize = itemgetter(0)
pseq = itemgetter(1)
payload = itemgetter(2)

TEXT_TYPES = set([
    FIELD_TYPE.BIT,
    FIELD_TYPE.BLOB,
    FIELD_TYPE.LONG_BLOB,
    FIELD_TYPE.MEDIUM_BLOB,
    FIELD_TYPE.STRING,
    FIELD_TYPE.TINY_BLOB,
    FIELD_TYPE.VAR_STRING,
    FIELD_TYPE.VARCHAR,
    FIELD_TYPE.GEOMETRY])

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
    if col == 251:
        # NULL_COLUMN
        return size, None

    if col < 251:
        # UNSIGNED_CHAR_COLUMN
        return size, col
    elif col == 252:
        # UNSIGNED_SHORT_COLUMN
        _s, _d = read_uint16(data, offset=offset+size)
    elif col == 253:
        # UNSIGNED_INT24_COLUMN
        _s, _d = read_uint24(data, offset=offset+size)
    elif col == 254:
        # UNSIGNED_INT64_COLUMN
        _s, _d = read_uint64(data, offset=offset+size)
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
    return read_uint8(payload(packet))[1] == 0 and psize(packet) >= 7

def is_eof_packet(packet):
    # http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-EOF_Packet
    # Caution: \xFE may be LengthEncodedInteger.
    # If \xFE is LengthEncodedInteger header, 8bytes followed.
    return read_uint8(payload(packet))[1] == 254 and psize(packet) < 9

def is_auth_switch_request(packet):
    # http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
    return read_uint8(payload(packet))[1] == 254

def is_load_local(packet):
    return read_uint8(payload(packet))[1] == 251

def is_resultset_packet(packet):
    return 1 <= read_uint8(payload(packet))[1] <= 250

def check_error(packet):
    if read_uint8(payload(packet))[1] == 255:
        errno = read_uint16(payload(packet), offset=1)[1]
        if DEBUG: print("errno = ", errno)
        err.raise_mysql_exception(payload(packet))

def parse_load_local_packet(packet):
    data = payload(packet)

    filename = read(data, None, offset=1)
    if DEBUG: print("filename=", filename)

    return {'filename': filename}

def parse_field_descriptor_packet(packet, encoding=DEFAULT_CHARSET):
    pos = 0
    data = payload(packet)

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
    pos = 1
    data = payload(packet)

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

    data = payload(packet)
    warning_count, server_status = unpack_from('<hh', data, offset=1)

    if DEBUG: print("server_status=", server_status)
    return {'warning_count': warning_count,
            'server_status': server_status,
            'has_next': server_status & SERVER_STATUS.SERVER_MORE_RESULTS_EXISTS}


def parse_result_stream(stream, encoding=DEFAULT_CHARSET, use_unicode=False,
                        converters=None):
    """
    Parse stream of packets in a result stream.
    Args:
        stream:
        encoding:

    Returns:
        (iterator):
            0: Field descriptions
            1: Groups of rows (each packet)
    """
    curr_packet = next(stream)
    _payload = payload
    _, field_count = read_length_encoded_integer(_payload(curr_packet))

    # the next field_count packets are field descriptor packets.
    if converters is None:
        converters = _converters.decoders
    fields = [None] * field_count
    f_encodings = [None] * field_count
    f_converters = [None] * field_count
    i = 0
    while i < field_count:
        curr_packet = next(stream)
        fields[i] = field = parse_field_descriptor_packet(curr_packet, encoding=encoding)
        _field_encoding = None
        if use_unicode:
            field_type = field['type_code']
            if field_type == FIELD_TYPE.JSON or field_type in TEXT_TYPES:
                if field['charsetnr'] != 63:
                    _field_encoding = encoding
            elif field['charsetnr'] == 63:
                _field_encoding = encoding
            else:
                _field_encoding = 'ascii'
        field['encoding'] = f_encodings[i] = _field_encoding

        converter = converters.get(field_type, _converters.through)
        field['converter'] = f_converters[i] = converter
        i += 1

    # Yield the descriptions
    yield tuple(fields)

    # Rest of the packets contain rows (1 packet == 1 row)
    # Parsing of packets
    row = [None] * field_count
    _length_coded_str = read_length_coded_string
    for curr_packet in stream:
        position = 0
        i = 0
        while i < field_count:
            size, data = _length_coded_str(_payload(curr_packet), offset=position)
            position += size

            if data is not None:
                if f_encodings[i]:
                    data = data.decode(f_encodings[i])
                data = f_converters[i](data)
            row[i] = data
            i += 1
        yield tuple(row)
