"""Unit tests for authentication packet transitions."""

from unittest import mock

import pytest

import pymysql
from pymysql import _auth
from pymysql.constants import CLIENT
from pymysql.protocol import MysqlPacket


class TestAuthenticationStateMachine:
    password = b"router-password"
    salt = b"01234567890123456789"
    ok_packet = b"\0\0\0\2\0\0\0"

    @staticmethod
    def packet(data):
        return MysqlPacket(data, "utf8")

    def connection(self, **kwargs):
        conn = pymysql.connect(
            user="router-user",
            password=self.password,
            ssl_disabled=True,
            defer_connect=True,
            **kwargs,
        )
        conn.server_version = "8.0.0"
        conn.server_capabilities = CLIENT.PLUGIN_AUTH | CLIENT.SECURE_CONNECTION
        conn.client_flag = CLIENT.PLUGIN_AUTH | CLIENT.SECURE_CONNECTION
        conn.salt = self.salt
        conn._auth_plugin_name = "caching_sha2_password"
        conn._secure = True
        return conn

    @pytest.mark.parametrize(
        ("responses", "expected_auth_responses"),
        [
            ([b"\x01\x04", ok_packet], [password + b"\0"]),
            (
                [b"\xfemysql_native_password\0" + salt + b"\0", ok_packet],
                [_auth.scramble_native_password(password, salt)],
            ),
            (
                [
                    b"\x01\x04",
                    b"\xfemysql_native_password\0" + salt + b"\0",
                    ok_packet,
                ],
                [password + b"\0", _auth.scramble_native_password(password, salt)],
            ),
        ],
        ids=["more-data", "auth-switch", "more-data-then-auth-switch"],
    )
    def test_authentication_packet_transitions(
        self, responses, expected_auth_responses
    ):
        """Exercise multi-step authentication without a MySQL server or Router."""
        conn = self.connection()

        packets = [self.packet(response) for response in responses]
        with (
            mock.patch.object(conn, "write_packet") as write_packet,
            mock.patch.object(conn, "_read_packet", side_effect=packets),
        ):
            conn._request_authentication()

        # The first write is the handshake response; subsequent writes are the
        # authentication state-machine responses asserted by each scenario.
        assert [call.args[0] for call in write_packet.call_args_list[1:]] == (
            expected_auth_responses
        )

    def test_multiple_auth_switch_requests_are_rejected(self):
        """Only one authentication-method switch is valid per handshake."""
        conn = self.connection()

        packets = [
            self.packet(b"\xfemysql_native_password\0" + self.salt + b"\0"),
            self.packet(b"\xfesha256_password\0" + self.salt + b"\0"),
        ]
        with (
            mock.patch.object(conn, "write_packet"),
            mock.patch.object(conn, "_read_packet", side_effect=packets),
            pytest.raises(
                pymysql.err.OperationalError,
                match="received multiple auth switch requests",
            ),
        ):
            conn._request_authentication()

    def test_custom_handler_processes_follow_up_packet(self):
        """The same configured handler processes every packet in its exchange."""

        class ChainedHandler:
            calls = []

            def __init__(self, conn):
                self.conn = conn

            def authenticate(self, packet):
                self.calls.append(packet.get_all_data())
                return self.conn._read_packet()

        conn = self.connection(
            auth_plugin_map={b"caching_sha2_password": ChainedHandler}
        )
        packets = [
            self.packet(b"\xfecaching_sha2_password\0challenge"),
            self.packet(b"\x01follow-up"),
            self.packet(self.ok_packet),
        ]

        with (
            mock.patch.object(conn, "write_packet"),
            mock.patch.object(conn, "_read_packet", side_effect=packets),
        ):
            conn._request_authentication()

        assert ChainedHandler.calls == [
            b"\xfecaching_sha2_password\0challenge",
            b"\x01follow-up",
        ]

    def test_custom_handler_overrides_initial_plugin(self):
        """An initial plugin override handles extra data without an auth switch."""

        class InitialHandler:
            calls = []

            def __init__(self, conn):
                self.conn = conn

            def authenticate(self, packet):
                self.calls.append(packet.get_all_data())
                return self.conn._read_packet()

        conn = self.connection(
            auth_plugin_map={b"caching_sha2_password": InitialHandler}
        )
        packets = [self.packet(b"\x01follow-up"), self.packet(self.ok_packet)]

        with (
            mock.patch.object(conn, "write_packet"),
            mock.patch.object(conn, "_read_packet", side_effect=packets),
        ):
            conn._request_authentication()

        assert InitialHandler.calls == [b"\x01follow-up"]

    def test_custom_handler_may_consume_exchange_without_returning_packet(self):
        class ConsumingHandler:
            def __init__(self, conn):
                self.conn = conn

            def authenticate(self, packet):
                self.conn._read_packet()

        conn = self.connection(auth_plugin_map={b"custom": ConsumingHandler})
        packets = [
            self.packet(b"\xfecustom\0challenge"),
            self.packet(self.ok_packet),
        ]

        with (
            mock.patch.object(conn, "write_packet"),
            mock.patch.object(conn, "_read_packet", side_effect=packets),
        ):
            conn._request_authentication()

    def test_non_ok_terminal_packet_is_rejected(self):
        conn = self.connection()

        with (
            mock.patch.object(conn, "write_packet"),
            mock.patch.object(
                conn, "_read_packet", return_value=self.packet(b"\x02unexpected")
            ),
            pytest.raises(
                pymysql.err.OperationalError,
                match="unexpected packet during authentication",
            ),
        ):
            conn._request_authentication()
