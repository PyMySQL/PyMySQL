from ._compat import PY2

from struct import unpack_from

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

