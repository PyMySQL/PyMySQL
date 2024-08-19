import zlib
from ..socketwrapper import SocketWrapper, pack_int24, unpack_uint24
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

    async def recv_uncompress_packet(self):
        return await self.recv(unpack_uint24(await self.recv(4)))

    async def _recv_from_decompressed(self, size):
        if len(self._decompressed) < size:
            compressed_length = unpack_uint24(await self.recv(3))
            await self.recv(1)  # compressed sequence
            uncompressed_length = unpack_uint24(await self.recv(3))
            data = await self.recv(compressed_length)
            if uncompressed_length != 0:
                data = zlib.decompress(data)
                assert len(data) == uncompressed_length
            self._decompressed += data
        recv_data, self._decompressed = self._decompressed[:size], self._decompressed[size:]
        return recv_data

    async def recv_packet(self, loop):
        """Read entire mysql packet."""
        recv_data = b''
        if self._compress:
            ln = unpack_uint24(await self._recv_from_decompressed(4))
            recv_data = await self._recv_from_decompressed(ln)
        else:
            while True:
                ln = int.from_bytes((await self.recv(4, loop))[:3], "little")
                recv_data += await self.recv(ln, loop)
                if recv_data[:3] != b'\xff\xff\xff':
                    break

        return recv_data

    async def send_uncompress_packet(self, data, loop):
        await loop.sock_sendall(self._sock, data)

    async def send_packet(self, data, loop):
        if self._compress:
            data = pack_int24(len(data)) + b'\x00' + pack_int24(0) + data
        await loop.sock_sendall(self._sock, data)
