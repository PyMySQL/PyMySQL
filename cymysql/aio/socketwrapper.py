from ..socketwrapper import SocketWrapper
from ..err import OperationalError


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
            ln = int.from_bytes((await self.recv(4, loop))[:3], "little")
            recv_data += await self.recv(ln, loop)
            if recv_data[:3] != b'\xff\xff\xff':
                break

        return recv_data

    async def sendall(self, data, loop):
        loop.sock_sendall(self._sock, data)
