"""
Implements sha256_password and caching_sha2_password auth methods.
"""
from .._compat import text_type
from ..constants import CLIENT
from ..err import OperationalError

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding

import hashlib


DEBUG = True


def _roundtrip(conn, send_data):
    conn.write_packet(send_data)
    pkt = conn._read_packet()
    pkt.check_error()
    return pkt


def _xor_password(password, salt):
    password_bytes = bytearray(password)
    salt = bytearray(salt)  # for PY2 compat.
    salt_len = len(salt)
    for i in range(len(password_bytes)):
        password_bytes[i] ^= salt[i % salt_len]
    return password_bytes


def _sha256_rsa_crypt(password, salt, public_key):
    message = _xor_password(password + b'\0', salt)
    rsa_key = serialization.load_pem_public_key(public_key, default_backend())
    return rsa_key.encrypt(
        message.decode('latin1').encode('latin1'), padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA1()),
            algorithm=hashes.SHA1(),
            label=None))


class SHA256Password(object):
    def __init__(self, conn):
        self.conn = conn

    def authenticate(self, pkt):
        conn = self.conn

        if conn.ssl and conn.server_capabilities & CLIENT.SSL:
            if DEBUG: print("sha256: Sending plain password")
            data = conn.password + b'\0'
            return _roundtrip(conn, data)

        if pkt.is_auth_switch_request():
            conn.salt = pkt.read_all()
            if not conn.server_public_key and conn.password:
                # Request server public key
                if DEBUG: print("sha256: Requesting server public key")
                pkt = _roundtrip(conn, b'\1')

        if pkt.is_extra_auth_data():
            conn.server_public_key = pkt._data[1:]
            if DEBUG:
                print("Received public key:\n", conn.server_public_key.decode('ascii'))

        if conn.password:
            if not conn.server_public_key:
                raise OperationalError("Couldn't receive server's public key")
            data = _sha256_rsa_crypt(conn.password, conn.salt, conn.server_public_key)
        else:
            data = b''

        return _roundtrip(conn, data)


# XOR(SHA256(password), SHA256(SHA256(SHA256(password)), scramble))
# Used in caching_sha2_password
def _scramble_sha256_password(password, scramble):
    if not password:
        return b''

    p1 = hashlib.sha256(password).digest()
    p2 = hashlib.sha256(p1).digest()
    p3 = hashlib.sha256(p2 + scramble).digest()

    res = bytearray(p1)
    for i in range(len(p3)):
        res[i] ^= p3[i]

    return bytes(res)


class CachingSHA2Password(object):
    def __init__(self, conn):
        self.conn = conn

    def authenticate(self, pkt):
        conn = self.conn

        # No password fast path
        if not conn.password:
            return _roundtrip(conn, b'')

        if pkt.is_auth_switch_request():
            # Try from fast auth
            if DEBUG: print("caching sha2: Trying fast path")
            conn.salt = pkt.read_all()
            scrambled = _scramble_sha256_password(conn.password, conn.salt)
            pkt = _roundtrip(conn, scrambled)
        #else: fast auth is tried in initial handshake

        if not pkt.is_extra_auth_data():
            raise OperationalError("caching sha2: Unknown packet for fast auth: %s" % pkt._data[:1])

        # magic numbers:
        # 2 - request public key
        # 3 - fast auth succeeded
        # 4 - need full auth

        pkt.advance(1)
        n = pkt.read_uint8()

        if n == 3:
            if DEBUG: print("caching sha2: succeeded by fast path.")
            pkt = conn._read_packet()
            pkt.check_error()  # pkt must be OK packet
            return pkt

        if n != 4:
            raise OperationalError("caching sha2: Unknwon result for fast auth: %s" % n)

        if DEBUG: print("caching sha2: Trying full auth...")

        if conn.ssl and conn.server_capabilities & CLIENT.SSL:
            if DEBUG: print("caching sha2: Sending plain password via SSL")
            return _roundtrip(conn, conn.password + b'\0')

        if not conn.server_public_key:
            pkt = _roundtrip(conn, b'\x02')  # Request public key
            if not pkt.is_extra_auth_data():
                raise OperationalError("caching sha2: Unknown packet for public key: %s" % pkt._data[:1])
            conn.server_public_key = pkt._data[1:]
            if DEBUG:
                print(conn.server_public_key.decode('ascii'))

        data = _sha256_rsa_crypt(conn.password, conn.salt, conn.server_public_key)
        pkt = _roundtrip(conn, data)
