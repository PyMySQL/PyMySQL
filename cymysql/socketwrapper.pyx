import sys
from cymysql.err import OperationalError


cdef  uint32_t unpack_uint24(bytes s):
    cdef unsigned char* n = s
    return n[0] + (n[1] << 8) + (n[2] << 16)


class SocketWrapper():
    cdef object _socket

    def __init__(self, sock):
        self._sock = sock

    cdef recv(self, size):
        r = b''
        while size:
            recv_data = self._sock.recv(size)
            if not recv_data:
                raise OperationalError(2013, "Lost connection to MySQL server during query")
            size -= len(recv_data)
            r += recv_data
        return r

    cdef recv_packet(self):
        """Read entire mysql packet."""
        recv_data = b''
        while True:
            recv_data += self.recv(unpack_uint24(self.recv(4)))
            if recv_data[:3] != b'\xff\xff\xff':
                break
        return recv_data

    cdef sendall(self, data):
        self._sock.sendall(data)

    cdef setblocking(self, b):
        self._sock.setblocking(b)

    cdef close(self):
        self._sock.close()
