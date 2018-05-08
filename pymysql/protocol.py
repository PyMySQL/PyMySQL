from __future__ import print_function
from ._compat import PY2

from struct import unpack_from

from . import err

DEBUG = False

if PY2:
    def read_uint8(data, offset=0):
        """Read 1 byte of data"""
        return ord(data[offset])
else:
    def read_uint8(data, offset=0):
        """Read 1 byte of data"""
        return data[offset]


def read_uint16(data, offset=0):
    """Read 2 bytes of data beginning at offset"""
    return unpack_from('<H', data, offset=offset)[0]


def read_uint24(data, offset=0):
    """Read 3 bytes of data beginning at offset"""
    low, high = unpack_from('<HB', data, offset=offset)
    return low + (high << 16)


def read_uint32(data, offset=0):
    """Read 4 bytes of data beginning at offset"""
    return unpack_from('<I', data, offset=offset)[0]


def read_uint64(data, offset=0):
    """Read 8 bytes of data beginning at offset"""
    return unpack_from('<Q', data, offset=offset)[0]


def is_ok_packet(packet):
    # https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
    return read_uint8(packet) == 0 and len(packet) >= 7


def is_eof_packet(packet):
    # http://dev.mysql.com/doc/internals/en/generic-response-packets.html#packet-EOF_Packet
    # Caution: \xFE may be LengthEncodedInteger.
    # If \xFE is LengthEncodedInteger header, 8bytes followed.
    return read_uint8(packet) == 254 and len(packet) < 9


def is_auth_switch_request(packet):
    # http://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::AuthSwitchRequest
    return read_uint8(packet) == 254


def is_load_local(packet):
    return read_uint8(packet) == 251


def is_resultset_packet(packet):
    return 1 <= read_uint8(packet) <= 250


def check_error(packet):
    if read_uint8(packet) == 255:
        errno = read_uint16(packet, offset=1)
        if DEBUG: print("errno = ", errno)
        err.raise_mysql_exception(packet)


def read_string(data, offset=0):
    end = data.find(b'\0', offset)
    if end >= 0:
        result = data[offset:end]
        # Add one to length to account for the null byte
        return len(result) + 1, result
    else:
        raise ValueError("Invalid read on non-null terminated string")


def read_length_encoded_integer(data, offset=0):
    col = read_uint8(data, offset=offset)
    bytes_read = 1

    if col == 251:
        return bytes_read, None

    # Unsigned char column
    if col < 251:
        return bytes_read, col
    # Unsigned short column
    elif col == 252:
        return bytes_read + 2, read_uint16(data, offset=offset+bytes_read)
    # Unsigned int24 column
    elif col == 253:
        return bytes_read + 3, read_uint24(data, offset=offset+bytes_read)
    # Unsigned int64 column
    elif col == 254:
        return bytes_read + 8, read_uint64(data, offset=offset+bytes_read)
    else:
        raise ValueError


def read_bytes(data, nbytes, offset=0):
    if nbytes is None:
        result = data[offset:]
        return len(result), result
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


def read_length_coded_string(data, offset=0):
    bytes_read, length = read_length_encoded_integer(data, offset=offset)
    if length is not None:
        _br, result = read_bytes(data, length, offset=offset+bytes_read)
        return bytes_read + _br, result
    else:
        # Null column
        return bytes_read, None