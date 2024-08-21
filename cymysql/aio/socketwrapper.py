import zlib
try:
    import pyzstd
except ImportError:
    pyzstd = None
from ..socketwrapper import SocketWrapper
from ..err import OperationalError

def pack_int24(n):
    return bytes([n & 0xFF, (n >> 8) & 0xFF, (n >> 16) & 0xFF])


def unpack_uint24(n):
    return n[0] + (n[1] << 8) + (n[2] << 16)


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

    async def recv_uncompress_packet(self, loop):
        return await self.recv(unpack_uint24(await self.recv(4, loop)), loop)

    async def _recv_from_decompressed(self, size, loop):
        if len(self._decompressed) < size:
            compressed_length = unpack_uint24(await self.recv(3, loop))
            await self.recv(1)  # compressed sequence
            uncompressed_length = unpack_uint24(await self.recv(3, loop))
            data = await self.recv(compressed_length, loop)
            if uncompressed_length != 0:
                if self._compress == "zlib":
                    data = zlib.decompress(data)
                elif self._compress == "zstd":
                    data = pyzstd.decompress(data)
                assert len(data) == uncompressed_length
            self._decompressed += data
        recv_data, self._decompressed = self._decompressed[:size], self._decompressed[size:]
        return recv_data

    async def recv_packet(self, loop):
        """Read entire mysql packet."""
        recv_data = b''
        if self._compress:
            ln = unpack_uint24(await self._recv_from_decompressed(4, loop))
            recv_data = await self._recv_from_decompressed(ln, loop)
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
            uncompressed_length = len(data)
            if uncompressed_length < 50:
                compressed = data
                compressed_length = len(compressed)
                uncompressed_length = 0
            else:
                if self._compress == "zlib":
                    compressed = zlib.compress(data)
                elif self._compress == "pyzstd":
                    compressed = pyzstd.compress(data)
                compressed_length = len(compressed)
                if len(data) < compressed_length:
                    compressed = data
                    compressed_length = len(compressed)
                    uncompressed_length = 0
            data = pack_int24(compressed_length) + b'\x00' + pack_int24(uncompressed_length) + compressed
        await loop.sock_sendall(self._sock, data)
