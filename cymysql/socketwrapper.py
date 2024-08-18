import sys
from cymysql.err import OperationalError

PYTHON3 = sys.version_info[0] > 2

def unpack_uint24(n):
    if PYTHON3:
        return n[0] + (n[1] << 8) + (n[2] << 16)
    else:
        return ord(n[0]) + (ord(n[1]) << 8) + (ord(n[2]) << 16)

class SocketWrapper():
    def __init__(self, sock, compress):
        self._sock = sock
        self._compress = compress

    def recv(self, size):
        r = b''
        while size:
            recv_data = self._sock.recv(size)
            if not recv_data:
                raise OperationalError(2013, "Lost connection to MySQL server during query")
            size -= len(recv_data)
            r += recv_data
        return r

    def recv_packet(self):
        """Read entire mysql packet."""
        recv_data = b''
        while True:
            recv_data += self.recv(unpack_uint24(self.recv(4)))
            if recv_data[:3] != b'\xff\xff\xff':
                break
        return recv_data

    def send_packet(self, data):
        self._sock.sendall(data)

    def setblocking(self, b):
        self._sock.setblocking(b)

    def close(self):
        self._sock.close()
