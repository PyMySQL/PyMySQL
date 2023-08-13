# Python implementation of the MySQL client-server protocol
#   https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html

import sys
from ..err import OperationalError


def unpack_uint24(n):
    return n[0] + (n[1] << 8) + (n[2] << 16)


async def _recv_from_socket(sock, size, loop):
    r = b''
    while size:
        recv_data = await loop.sock_recv(sock, size)
        if not recv_data:
            raise OperationalError(2013, "Lost connection to MySQL server during query")
        size -= len(recv_data)
        r += recv_data
    return r


async def recv_packet(sock, loop):
    """Read entire mysql packet."""
    recv_data = b''
    while True:
        ln = unpack_uint24(await _recv_from_socket(sock, 4, loop))
        recv_data += await _recv_from_socket(sock, ln, loop)
        if recv_data[:3] != b'\xff\xff\xff':
            break

    return recv_data
