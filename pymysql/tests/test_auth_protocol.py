"""
Unit tests for authentication protocol logic.
These tests verify the Connection object's auth handling by mocking the network layer.
"""

import pytest
import struct
from io import BytesIO
from unittest import mock

from pymysql import _auth
from pymysql.connections import Connection, _lenenc_int
from pymysql.constants import CLIENT


class MockSocket:
    """Mock socket for testing Connection authentication."""

    def __init__(self):
        self.sent_data = bytearray()
        self.recv_buffer = BytesIO()
        self._closed = False
        self._next_server_seq = 0

    def sendall(self, data):
        """Capture sent data."""
        if self._closed:
            raise OSError("Socket closed")
        self.sent_data.extend(data)

    def settimeout(self, timeout):
        pass

    def setsockopt(self, *args):
        pass

    def makefile(self, mode):
        """Return a file-like object for reading."""
        return self.recv_buffer

    def close(self):
        self._closed = True

    def queue_server_packet(self, data, seq_num=None):
        """Queue a packet from the server to be read."""
        # Packet format: 3 bytes length + 1 byte seq + data
        if seq_num is None:
            seq_num = self._next_server_seq
            self._next_server_seq = (self._next_server_seq + 1) % 256

        packet_len = len(data)
        header = struct.pack("<I", packet_len)[:3] + bytes([seq_num])
        self.recv_buffer.write(header + data)

    def finalize_recv_buffer(self):
        """Finalize the receive buffer and reset position to start."""
        self.recv_buffer.seek(0)

    def get_last_packet_data(self):
        """Extract the last packet's data (without header)."""
        if len(self.sent_data) < 4:
            return b""
        # Skip the 4-byte header (3 bytes length + 1 byte seq)
        return bytes(self.sent_data[4:])


def create_handshake_packet(auth_plugin_name=b"mysql_native_password", salt=None):
    """Create a mock server handshake packet."""
    if salt is None:
        salt = b"12345678901234567890"  # 20 bytes

    data = bytearray()
    data.append(10)  # protocol version
    data.extend(b"8.0.0\x00")  # server version
    data.extend(struct.pack("<I", 1))  # thread_id
    data.extend(salt[:8])  # first 8 bytes of salt
    data.append(0)  # filler

    # Server capabilities (lower 2 bytes)
    capabilities = (
        CLIENT.LONG_PASSWORD |
        CLIENT.LONG_FLAG |
        CLIENT.CONNECT_WITH_DB |
        CLIENT.PROTOCOL_41 |
        CLIENT.SECURE_CONNECTION |
        CLIENT.PLUGIN_AUTH |
        CLIENT.PLUGIN_AUTH_LENENC_CLIENT_DATA
    )
    data.extend(struct.pack("<H", capabilities & 0xFFFF))

    # charset, status, capabilities (upper 2 bytes), salt length
    data.append(33)  # utf8_general_ci
    data.extend(struct.pack("<H", 2))  # server status
    data.extend(struct.pack("<H", (capabilities >> 16) & 0xFFFF))
    data.append(21)  # salt length

    # reserved
    data.extend(b"\x00" * 10)

    # rest of salt
    data.extend(salt[8:20])
    data.append(0)  # filler

    # auth plugin name
    data.extend(auth_plugin_name + b"\x00")

    return bytes(data)


def create_ok_packet():
    """Create a mock OK packet."""
    data = bytearray()
    data.append(0x00)  # OK packet header
    data.append(0)  # affected rows
    data.append(0)  # insert_id
    data.extend(struct.pack("<H", 0))  # server status
    data.extend(struct.pack("<H", 0))  # warning count
    return bytes(data)


def create_auth_switch_packet(plugin_name, salt):
    """Create a mock auth switch request packet."""
    data = bytearray()
    data.append(0xfe)  # auth switch marker
    data.extend(plugin_name + b"\x00")
    data.extend(salt + b"\x00")
    return bytes(data)


class TestConnectionNativePasswordAuth:
    """Test mysql_native_password authentication via Connection object."""

    def test_native_password_initial_handshake(self):
        """Test native password auth in initial handshake."""
        mock_sock = MockSocket()
        salt = b"12345678901234567890"

        # Queue handshake packet (server seq 0)
        handshake = create_handshake_packet(b"mysql_native_password", salt)
        mock_sock.queue_server_packet(handshake, seq_num=0)

        # Queue OK packet for auth response (server seq 2, after client sends seq 1)
        mock_sock.queue_server_packet(create_ok_packet(), seq_num=2)
        mock_sock.finalize_recv_buffer()

        # Create connection
        with mock.patch("socket.create_connection", return_value=mock_sock):
            conn = Connection(
                host="localhost",
                user="testuser",
                password="testpass",
                defer_connect=True
            )
            conn._sock = mock_sock
            conn._rfile = mock_sock.makefile("rb")
            conn._next_seq_id = 0

            # Perform authentication
            conn._get_server_information()
            conn._request_authentication()

            # Verify the auth plugin was detected
            assert conn._auth_plugin_name == "mysql_native_password"

            # Verify authentication response was sent
            sent_data = mock_sock.get_last_packet_data()
            assert len(sent_data) > 0

            # The sent packet should contain the scrambled password
            # Structure: capability (4) + max_packet (4) + charset (1) + filler (23) + user + \x00 + auth_len + auth_data + ...
            expected_scramble = _auth.scramble_native_password(b"testpass", salt)
            assert expected_scramble in sent_data

    def test_native_password_empty_password(self):
        """Test native password with empty password."""
        mock_sock = MockSocket()
        salt = b"12345678901234567890"

        handshake = create_handshake_packet(b"mysql_native_password", salt)
        mock_sock.queue_server_packet(handshake, seq_num=0)
        mock_sock.queue_server_packet(create_ok_packet(), seq_num=2)
        mock_sock.finalize_recv_buffer()

        with mock.patch("socket.create_connection", return_value=mock_sock):
            conn = Connection(
                host="localhost",
                user="testuser",
                password="",
                defer_connect=True
            )
            conn._sock = mock_sock
            conn._rfile = mock_sock.makefile("rb")
            conn._next_seq_id = 0

            conn._get_server_information()
            conn._request_authentication()

            # With empty password, auth response should be empty
            sent_data = mock_sock.get_last_packet_data()
            # Should contain user but empty auth data
            assert b"testuser\x00" in sent_data


class TestConnectionCachingSha2Auth:
    """Test caching_sha2_password authentication via Connection object."""

    def test_caching_sha2_initial_handshake(self):
        """Test caching_sha2_password in initial handshake."""
        mock_sock = MockSocket()
        salt = b"12345678901234567890"

        handshake = create_handshake_packet(b"caching_sha2_password", salt)
        mock_sock.queue_server_packet(handshake, seq_num=0)

        # Queue auth continuation packet (fast auth success)
        auth_continue = b"\x01\x03"  # extra auth data marker + success
        mock_sock.queue_server_packet(auth_continue, seq_num=2)

        # Queue OK packet
        mock_sock.queue_server_packet(create_ok_packet(), seq_num=3)
        mock_sock.finalize_recv_buffer()

        with mock.patch("socket.create_connection", return_value=mock_sock):
            conn = Connection(
                host="localhost",
                user="testuser",
                password="testpass",
                defer_connect=True
            )
            conn._sock = mock_sock
            conn._rfile = mock_sock.makefile("rb")
            conn._next_seq_id = 0

            conn._get_server_information()
            conn._request_authentication()

            assert conn._auth_plugin_name == "caching_sha2_password"

            # Verify scrambled password was sent
            sent_data = mock_sock.get_last_packet_data()
            expected_scramble = _auth.scramble_caching_sha2(b"testpass", salt)
            assert expected_scramble in sent_data


class TestConnectionClearPasswordAuth:
    """Test mysql_clear_password authentication via Connection object."""

    def test_clear_password_format(self):
        """Test clear password sends password + null terminator."""
        mock_sock = MockSocket()
        salt = b"12345678901234567890"

        handshake = create_handshake_packet(b"mysql_clear_password", salt)
        mock_sock.queue_server_packet(handshake, seq_num=0)
        mock_sock.queue_server_packet(create_ok_packet(), seq_num=2)
        mock_sock.finalize_recv_buffer()

        with mock.patch("socket.create_connection", return_value=mock_sock):
            conn = Connection(
                host="localhost",
                user="testuser",
                password="mysecret",
                defer_connect=True
            )
            conn._sock = mock_sock
            conn._rfile = mock_sock.makefile("rb")
            conn._next_seq_id = 0

            conn._get_server_information()
            conn._request_authentication()

            assert conn._auth_plugin_name == "mysql_clear_password"

            # Clear password should be sent as password + \x00
            sent_data = mock_sock.get_last_packet_data()
            assert b"mysecret\x00" in sent_data


class TestConnectionOIDCAuth:
    """Test authentication_openid_connect_client via Connection object."""

    def test_oidc_message_format(self):
        """Test OIDC auth sends correct message format."""
        mock_sock = MockSocket()
        salt = b"12345678901234567890"
        jwt_token = b"eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ1c2VyIn0.signature"

        handshake = create_handshake_packet(b"authentication_openid_connect_client", salt)
        mock_sock.queue_server_packet(handshake, seq_num=0)
        mock_sock.queue_server_packet(create_ok_packet(), seq_num=2)
        mock_sock.finalize_recv_buffer()

        with mock.patch("socket.create_connection", return_value=mock_sock):
            conn = Connection(
                host="localhost",
                user="testuser",
                password=jwt_token,
                defer_connect=True
            )
            conn._sock = mock_sock
            conn._rfile = mock_sock.makefile("rb")
            conn._next_seq_id = 0

            conn._get_server_information()
            conn._request_authentication()

            assert conn._auth_plugin_name == "authentication_openid_connect_client"

            # Verify OIDC message format: [capability][length-encoded token length][token]
            sent_data = mock_sock.get_last_packet_data()

            # Expected format: capability (0x01) + lenenc_int(50) + token
            expected_capability = b"\x01"
            expected_len = _lenenc_int(len(jwt_token))
            expected_auth = expected_capability + expected_len + jwt_token

            assert expected_auth in sent_data

    def test_oidc_large_token(self):
        """Test OIDC with token > 250 bytes (multi-byte length encoding)."""
        mock_sock = MockSocket()
        salt = b"12345678901234567890"

        # Create a token larger than 250 bytes
        large_token = b"x" * 1393

        handshake = create_handshake_packet(b"authentication_openid_connect_client", salt)
        mock_sock.queue_server_packet(handshake, seq_num=0)
        mock_sock.queue_server_packet(create_ok_packet(), seq_num=2)
        mock_sock.finalize_recv_buffer()

        with mock.patch("socket.create_connection", return_value=mock_sock):
            conn = Connection(
                host="localhost",
                user="testuser",
                password=large_token,
                defer_connect=True
            )
            conn._sock = mock_sock
            conn._rfile = mock_sock.makefile("rb")
            conn._next_seq_id = 0

            conn._get_server_information()
            conn._request_authentication()

            sent_data = mock_sock.get_last_packet_data()

            # For 1393 bytes: should be 0xFC 0x71 0x05
            expected_len_enc = b"\xfc\x71\x05"
            expected_auth = b"\x01" + expected_len_enc + large_token

            assert expected_auth in sent_data


class TestConnectionAuthSwitch:
    """Test authentication plugin switch requests."""

    def test_auth_switch_to_native_password(self):
        """Test switching from one auth method to another."""
        mock_sock = MockSocket()
        salt = b"12345678901234567890"
        new_salt = b"98765432109876543210"

        # Initial handshake with caching_sha2
        handshake = create_handshake_packet(b"caching_sha2_password", salt)
        mock_sock.queue_server_packet(handshake, seq_num=0)

        # Server requests auth switch
        auth_switch = create_auth_switch_packet(b"mysql_native_password", new_salt)
        mock_sock.queue_server_packet(auth_switch, seq_num=2)

        # Queue OK packet
        mock_sock.queue_server_packet(create_ok_packet(), seq_num=4)
        mock_sock.finalize_recv_buffer()

        with mock.patch("socket.create_connection", return_value=mock_sock):
            conn = Connection(
                host="localhost",
                user="testuser",
                password="testpass",
                defer_connect=True
            )
            conn._sock = mock_sock
            conn._rfile = mock_sock.makefile("rb")
            conn._next_seq_id = 0

            conn._get_server_information()
            conn._request_authentication()

            # Should have sent two auth packets: initial and switched
            # The second packet should contain native password scramble with new salt
            expected_scramble = _auth.scramble_native_password(b"testpass", new_salt)

            # Check that scramble was sent (it should be in the sent data)
            # Note: MockSocket accumulates all sent data
            sent_all = bytes(mock_sock.sent_data)
            assert expected_scramble in sent_all

    def test_auth_switch_to_oidc(self):
        """Test switching to OIDC authentication."""
        mock_sock = MockSocket()
        salt = b"12345678901234567890"
        new_salt = b"98765432109876543210"
        jwt_token = b"eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ1c2VyIn0.sig"

        handshake = create_handshake_packet(b"mysql_native_password", salt)
        mock_sock.queue_server_packet(handshake, seq_num=0)

        # Server requests switch to OIDC
        auth_switch = create_auth_switch_packet(b"authentication_openid_connect_client", new_salt)
        mock_sock.queue_server_packet(auth_switch, seq_num=2)

        mock_sock.queue_server_packet(create_ok_packet(), seq_num=4)
        mock_sock.finalize_recv_buffer()

        with mock.patch("socket.create_connection", return_value=mock_sock):
            conn = Connection(
                host="localhost",
                user="testuser",
                password=jwt_token,
                defer_connect=True
            )
            conn._sock = mock_sock
            conn._rfile = mock_sock.makefile("rb")
            conn._next_seq_id = 0

            conn._get_server_information()
            conn._request_authentication()

            # Should have sent OIDC format in response to auth switch
            expected_oidc = b"\x01" + _lenenc_int(len(jwt_token)) + jwt_token
            sent_all = bytes(mock_sock.sent_data)
            assert expected_oidc in sent_all


class TestConnectionAuthErrors:
    """Test authentication error conditions."""

    def test_no_username_raises_error(self):
        """Test that missing username raises ValueError."""
        mock_sock = MockSocket()
        salt = b"12345678901234567890"

        handshake = create_handshake_packet(b"mysql_native_password", salt)
        mock_sock.queue_server_packet(handshake, seq_num=0)
        mock_sock.finalize_recv_buffer()

        # Patch DEFAULT_USER to None to simulate no username available
        with mock.patch("socket.create_connection", return_value=mock_sock), \
             mock.patch("pymysql.connections.DEFAULT_USER", None):
            conn = Connection(
                host="localhost",
                user=None,
                password="testpass",
                defer_connect=True
            )
            conn._sock = mock_sock
            conn._rfile = mock_sock.makefile("rb")
            conn._next_seq_id = 0

            conn._get_server_information()

            with pytest.raises(ValueError, match="Did not specify a username"):
                conn._request_authentication()


class TestUtilityFunctions:
    """Test utility functions used by authentication."""

    def test_lenenc_int_single_byte(self):
        """Values < 251 should be encoded as a single byte."""
        assert _lenenc_int(0) == b"\x00"
        assert _lenenc_int(1) == b"\x01"
        assert _lenenc_int(250) == b"\xfa"

    def test_lenenc_int_three_bytes(self):
        """Values 251-65535 should be encoded as 0xFC + 2 bytes."""
        assert _lenenc_int(251) == b"\xfc\xfb\x00"
        assert _lenenc_int(1393) == b"\xfc\x71\x05"
        assert _lenenc_int(65535) == b"\xfc\xff\xff"

    def test_lenenc_int_four_bytes(self):
        """Values 65536-16777215 should be encoded as 0xFD + 3 bytes."""
        assert _lenenc_int(65536) == b"\xfd\x00\x00\x01"
        assert _lenenc_int(16777215) == b"\xfd\xff\xff\xff"

    def test_lenenc_int_nine_bytes(self):
        """Values >= 16777216 should be encoded as 0xFE + 8 bytes."""
        result = _lenenc_int(16777216)
        assert result[0:1] == b"\xfe"
        assert len(result) == 9
