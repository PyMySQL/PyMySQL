from __future__ import print_function
from ._compat import PY2

from struct import unpack_from

from . import err

DEBUG = False

if PY2:
    def read_uint8(data, offset=0):
        return ord(data[offset])
else:
    def read_uint8(data, offset=0):
        return data[offset]


def read_uint16(data, offset=0):
    return unpack_from('<H', data, offset=offset)[0]


def read_uint24(data, offset=0):
    low, high = unpack_from('<HB', data, offset=offset)
    return low + (high << 16)


def read_uint32(data, offset=0):
    return unpack_from('<I', data, offset=offset)[0]


def read_uint64(data, offset=0):
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
