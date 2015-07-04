import struct

def byte2int(b):
    if isinstance(b, int):
        return b
    else:
        return struct.unpack("!B", b)[0]

def int2byte(i):
    return struct.pack("!B", i)

def join_bytes(bs):
    if len(bs) == 0:
        return ""
    else:
        rv = bs[0]
        for b in bs[1:]:
            rv += b
        return rv

# https://dev.mysql.com/doc/internals/en/integer.html#packet-Protocol::LengthEncodedInteger
def lenenc_int(i):
    if (i < 0):
        raise ValueError("Encoding %d is less than 0 - no representation in LengthEncodedInteger" % i)
    elif (i < 0xfb):
        return int2byte(i)
    elif (i < (1 << 16)):
        return b'\xfc' + struct.pack('<H', i)
    elif (i < (1 << 24)):
        return b'\xfd' + struct.pack('<I', i)[:3]
    elif (i < (1 << 64)):
        return b'\xfe' + struct.pack('<Q', i)
    else:
        raise ValueError("Encoding %x is larger than %x - no representation in LengthEncodedInteger" % (i, (1 << 64)))
