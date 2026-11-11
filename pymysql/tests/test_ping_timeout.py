"""Tests for Connection.ping(timeout=...) that do not need a MySQL server."""

import socket
import struct
import time

import pytest

import pymysql


def _connection_with_socket(sock, **kwargs):
    conn = pymysql.connect(defer_connect=True, **kwargs)
    conn._sock = sock
    conn._rfile = sock.makefile("rb")
    conn._current_timeout = conn._read_timeout
    return conn


def test_ping_timeout_on_silent_server():
    client, server = socket.socketpair()
    conn = _connection_with_socket(client)
    try:
        start = time.monotonic()
        with pytest.raises(pymysql.OperationalError):
            conn.ping(timeout=0.2)
        assert time.monotonic() - start < 5
        assert conn._sock is None
    finally:
        server.close()
        client.close()


def test_ping_timeout_is_restored_and_ping_works():
    client, server = socket.socketpair()
    conn = _connection_with_socket(client, read_timeout=30, write_timeout=40)
    try:
        # OK packet with sequence id 1, sent in reply to COM_PING.
        ok = b"\x00\x00\x00\x02\x00\x00\x00"
        server.sendall(struct.pack("<I", len(ok))[:3] + b"\x01" + ok)
        conn.ping(timeout=0.5)
        assert conn._read_timeout == 30
        assert conn._write_timeout == 40
        assert server.recv(16) == b"\x01\x00\x00\x00\x0e"
    finally:
        server.close()
        client.close()


def test_ping_timeout_must_be_positive():
    client, server = socket.socketpair()
    conn = _connection_with_socket(client)
    try:
        with pytest.raises(ValueError):
            conn.ping(timeout=0)
    finally:
        server.close()
        client.close()


def test_ping_timeout_is_checked_before_reconnect(monkeypatch):
    conn = pymysql.connect(defer_connect=True)
    calls = []
    monkeypatch.setattr(conn, "connect", lambda *args: calls.append(args))
    with pytest.warns(DeprecationWarning), pytest.raises(ValueError):
        conn.ping(reconnect=True, timeout=0)
    assert calls == []
