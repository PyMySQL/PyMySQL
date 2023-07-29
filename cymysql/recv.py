# Python implementation of the MySQL client-server protocol
#   https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_PROTOCOL.html

import sys
from cymysql.err import OperationalError

PYTHON3 = sys.version_info[0] > 2


def unpack_uint24(n):
    if PYTHON3:
        return n[0] + (n[1] << 8) + (n[2] << 16)
    else:
        return ord(n[0]) + (ord(n[1]) << 8) + (ord(n[2]) << 16)


def _recv_from_socket(sock, size):
    r = b''
    while size:
        recv_data = sock.recv(size)
        if not recv_data:
            raise OperationalError(2013, "Lost connection to MySQL server during query")
        size -= len(recv_data)
        r += recv_data
    return r


def recv_packet(sock):
    """Read entire mysql packet."""
    recv_data = b''
    while True:
        recv_data += _recv_from_socket(sock, unpack_uint24(_recv_from_socket(sock, 4)))
        if recv_data[:3] != b'\xff\xff\xff':
            break

    return recv_data
