import sys
import zlip
from cymysql.err import OperationalError
from libc.stdint cimport uint16_t, uint32_t


cdef bytes pack_int24(int n):
    return bytes([n & 0xFF, (n >> 8) & 0xFF, (n >> 16) & 0xFF])


cdef uint32_t unpack_uint24(bytes s):
    cdef unsigned char* n = s
    return n[0] + (n[1] << 8) + (n[2] << 16)


cdef class SocketWrapper():
    cdef public object _sock
    cdef public bool _compress
    cdef public bool _decompressed

    def __init__(self, sock, compress):
        self._sock = sock
        self._compress = compress
        self._decompressed = b''

    cdef recv(self, size):
        r = b''
        while size:
            recv_data = self._sock.recv(size)
            if not recv_data:
                raise OperationalError(2013, "Lost connection to MySQL server during query")
            size -= len(recv_data)
            r += recv_data
        return r

    def recv_uncompress_packet(self):
        return self.recv(unpack_uint24(self.recv(4)))

    cdef bytes _recv_from_decompressed(self, size):
        if len(self._decompressed) < size:
            compressed_length = unpack_uint24(self.recv(3))
            self.recv(1)  # compressed sequence
            uncompressed_length = unpack_uint24(self.recv(3))
            data = self.recv(compressed_length)
            if uncompressed_length != 0:
                data = zlib.decompress(data)
                assert len(data) == uncompressed_length
            self._decompressed += data
        recv_data, self._decompressed = self._decompressed[:size], self._decompressed[size:]
        return recv_data

    def recv_packet(self):
        """Read entire mysql packet."""
        recv_data = b''
        if self._compress:
            ln = unpack_uint24(self._recv_from_decompressed(4))
            recv_data = self._recv_from_decompressed(ln)
        else:
            while True:
                recv_data += self.recv(unpack_uint24(self.recv(4)))
                if recv_data[:3] != b'\xff\xff\xff':
                    break
        return recv_data

    def send_uncompress_packet(self, data):
        self._sock.sendall(data)

    def send_packet(self, data):
        if self._compress:
            data = pack_int24(len(data)) + b'\x00' + pack_int24(0) + data
        self._sock.sendall(data)

    def setblocking(self, b):
        self._sock.setblocking(b)

    def close(self):
        self._sock.close()
