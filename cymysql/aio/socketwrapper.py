import sys
from ..socketwrapper import SocketWrapper
from ..err import OperationalError

PYTHON3 = sys.version_info[0] > 2


def unpack_uint24(n):
    if PYTHON3:
        return n[0] + (n[1] << 8) + (n[2] << 16)
    else:
        return ord(n[0]) + (ord(n[1]) << 8) + (ord(n[2]) << 16)


class AsyncSocketWrapper(SocketWrapper):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def recv(self, size, loop):
        r = b''
        while size:
            recv_data = await loop.sock_recv(self._sock, size)
            if not recv_data:
                raise OperationalError(2013, "Lost connection to MySQL server during query")
            size -= len(recv_data)
            r += recv_data
        return r

    async def recv_packet(self, loop):
        """Read entire mysql packet."""
        recv_data = b''
        while True:
            ln = unpack_uint24(await self.recv(4, loop))
            recv_data += await self.recv(ln, loop)
            if recv_data[:3] != b'\xff\xff\xff':
                break

        return recv_data
