"""Compressed protocol framing, negotiation, and database round trips."""

import builtins
import importlib.util
import io
import random
import struct
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest import mock

import pytest

import pymysql
from pymysql import _compression, connections, err
from pymysql.constants import CLIENT, COMMAND
from pymysql.protocol import MysqlPacket
from pymysql.tests import base

ALGORITHMS = [
    "zlib",
    pytest.param(
        "zstd",
        marks=pytest.mark.skipif(
            _compression.zstd is None,
            reason="zstd requires compression.zstd or PyMySQL[zstd]",
        ),
    ),
]


def frame(payload, sequence=0, original_length=0):
    return (
        struct.pack("<I", len(payload))[:3]
        + bytes([sequence])
        + struct.pack("<I", original_length)[:3]
        + payload
    )


def compressed_payload(algorithm, payload):
    if algorithm == "zstd":
        return _compression.zstd.compress(payload)
    return _compression.zlib.compress(payload)


def mysql_packet(payload, sequence):
    return len(payload).to_bytes(3, "little") + bytes([sequence]) + payload


class FragmentedRaw(io.RawIOBase):
    """Return short transport reads even when a full frame is already available."""

    def __init__(self, data, fragment_size):
        self.source = io.BytesIO(data)
        self.fragment_size = fragment_size
        self.read_sizes = []

    def readable(self):
        return True

    def readinto(self, buffer):
        data = self.source.read(min(len(buffer), self.fragment_size))
        buffer[: len(data)] = data
        self.read_sizes.append(len(data))
        return len(data)


def framed_connection(algorithm, wire, fragment_size):
    conn = pymysql.connect(defer_connect=True, ssl_disabled=True)
    conn._compression = _compression.CompressedStream(algorithm)
    conn._sock = mock.Mock()
    conn._current_timeout = None
    conn._next_seq_id = 1
    raw = FragmentedRaw(wire, fragment_size)
    # socket.makefile('rb') uses a buffered reader too. Each raw read may
    # provide only part of a compressed header or compressed payload.
    conn._rfile = io.BufferedReader(raw)
    return conn, raw


@pytest.mark.parametrize("value", [0, 1, (1 << 24) - 1])
def test_pack_int24(value):
    assert _compression._pack_int24(value) == value.to_bytes(3, "little")


@pytest.mark.parametrize("value", [-1, 1 << 24])
def test_pack_int24_rejects_overflow(value):
    with pytest.raises(OverflowError):
        _compression._pack_int24(value)


@pytest.mark.parametrize("algorithm", ALGORITHMS)
@pytest.mark.parametrize("fragment_size", [1, 2, 5, 17])
def test_compressed_frame_with_fragmented_transport_reads(algorithm, fragment_size):
    payloads = [b"a" * 1000, b"b" * 2000]
    normal_wire = b"".join(
        mysql_packet(payload, sequence) for sequence, payload in enumerate(payloads, 1)
    )
    packets = []
    _compression.CompressedStream(algorithm).write(normal_wire, packets.append)
    assert len(packets) == 1
    assert int.from_bytes(packets[0][4:7], "little") == len(normal_wire)
    conn, raw = framed_connection(algorithm, packets[0], fragment_size)
    try:
        for expected in payloads:
            assert conn._read_packet().get_all_data() == expected
        assert raw.source.tell() == len(packets[0])
        assert len(raw.read_sizes) > 1
        assert all(size <= fragment_size for size in raw.read_sizes)
    finally:
        conn.close()


@pytest.mark.parametrize("algorithm", ALGORITHMS)
@pytest.mark.parametrize("fragment_size", [1, 13, 8192])
def test_mysql_header_and_body_split_across_compressed_frames(algorithm, fragment_size):
    # The second four-byte MySQL header begins at offset 399. Its first byte
    # is in one compressed frame and the remaining three are in the next.
    payloads = [b"a" * 395, b"b" * 1000]
    normal_wire = b"".join(
        mysql_packet(payload, sequence) for sequence, payload in enumerate(payloads, 1)
    )
    packets = []
    with mock.patch.object(_compression, "MAX_PAYLOAD_LENGTH", 400):
        _compression.CompressedStream(algorithm).write(normal_wire, packets.append)
    assert [packet[3] for packet in packets] == [0, 1, 2, 3]
    assert [int.from_bytes(packet[4:7], "little") for packet in packets] == [
        400,
        400,
        400,
        0,
    ]
    wire = b"".join(packets)
    conn, raw = framed_connection(algorithm, wire, fragment_size)
    try:
        for expected in payloads:
            assert conn._read_packet().get_all_data() == expected
        assert raw.source.tell() == len(wire)
    finally:
        conn.close()


@pytest.mark.parametrize("algorithm", ALGORITHMS)
@pytest.mark.parametrize("tail_size", [0, 5])
def test_mysql_continuation_packets_across_compressed_frames(algorithm, tail_size):
    limit = 400
    data = b"a" * limit + b"b" * limit + b"c" * tail_size
    # An exact multiple of the normal protocol limit needs an empty final
    # packet. Otherwise the following response could become part of this one.
    normal_wire = (
        mysql_packet(data[:limit], 1)
        + mysql_packet(data[limit : 2 * limit], 2)
        + mysql_packet(data[2 * limit :], 3)
        + mysql_packet(b"done", 4)
    )
    packets = []
    with mock.patch.object(_compression, "MAX_PAYLOAD_LENGTH", limit):
        _compression.CompressedStream(algorithm).write(normal_wire, packets.append)
    assert len(packets) == 3
    wire = b"".join(packets)
    conn, raw = framed_connection(algorithm, wire, 3)
    try:
        with mock.patch.object(connections, "MAX_PACKET_LEN", limit):
            assert conn._read_packet().get_all_data() == data
            assert conn._read_packet().get_all_data() == b"done"
        assert raw.source.tell() == len(wire)
    finally:
        conn.close()


@pytest.mark.parametrize("algorithm", ALGORITHMS)
@pytest.mark.parametrize("compressible", [True, False])
def test_real_24_bit_frame_limit(algorithm, compressible):
    limit = _compression.MAX_PAYLOAD_LENGTH
    size = limit + 400
    data = b"a" * size if compressible else random.Random(42).randbytes(size)
    packets = []
    _compression.CompressedStream(algorithm).write(data, packets.append)
    assert len(packets) == 2
    assert [packet[3] for packet in packets] == [0, 1]
    for packet, expected_length in zip(packets, (limit, 400)):
        wire_length = int.from_bytes(packet[:3], "little")
        original_length = int.from_bytes(packet[4:7], "little")
        assert wire_length == len(packet) - 7 <= limit
        assert original_length == (expected_length if compressible else 0)
        # Each frame must be independently decodable. A single compressed
        # stream cannot be split between protocol frames for later reassembly.
        reader = _compression.CompressedStream(algorithm)
        offset = 0 if packet[3] == 0 else limit
        assert (
            reader.read(expected_length, io.BytesIO(packet).read)
            == data[offset : offset + expected_length]
        )


@pytest.mark.parametrize("algorithm", ALGORITHMS)
def test_parallel_frame_decoding_keeps_streams_independent(algorithm):
    workers = 8
    barrier = threading.Barrier(workers)

    def decode(worker):
        data = bytes([worker + 1]) * 100000
        wire = frame(compressed_payload(algorithm, data), original_length=len(data))
        stream = _compression.CompressedStream(algorithm)
        barrier.wait(timeout=30)
        for _ in range(20):
            assert stream.read(len(data), io.BytesIO(wire).read) == data

    with ThreadPoolExecutor(max_workers=workers) as executor:
        # Consume every result so worker exceptions fail the test.
        list(executor.map(decode, range(workers)))


@pytest.mark.parametrize("algorithm", ALGORITHMS)
@pytest.mark.parametrize("length", [0, 1, 399, 400, 10000])
def test_threshold_and_round_trip(algorithm, length):
    data = b"a" * length
    stream = _compression.CompressedStream(algorithm)
    packets = []
    stream.write(data, packets.append)
    if length:
        original_length = int.from_bytes(packets[0][4:7], "little")
        assert original_length == (length if length >= 400 else 0)
    reader = _compression.CompressedStream(algorithm)
    assert reader.read(length, io.BytesIO(b"".join(packets)).read) == data


@pytest.mark.parametrize("algorithm", ALGORITHMS)
def test_incompressible_payload_is_sent_uncompressed(algorithm):
    # Stable random bytes avoid relying on compression of one particular string.
    rng = random.Random(42)
    data = bytes(rng.getrandbits(8) for _ in range(1000))
    packets = []
    _compression.CompressedStream(algorithm).write(data, packets.append)
    assert packets == [frame(data)]


@pytest.mark.parametrize("algorithm", ALGORITHMS)
def test_split_frames_and_sequence_wrap(algorithm):
    data = b"abcdefghij" * 100
    writer = _compression.CompressedStream(algorithm)
    writer.sequence = 255
    packets = []
    with mock.patch.object(_compression, "MAX_PAYLOAD_LENGTH", 400):
        writer.write(data, packets.append)
    assert [packet[3] for packet in packets] == [255, 0, 1]
    assert writer.sequence == 2
    reader = _compression.CompressedStream(algorithm)
    source = io.BytesIO(b"".join(packets))
    # A normal MySQL header or payload can straddle compressed frames.
    assert reader.read(399, source.read) == data[:399]
    assert reader.read(402, source.read) == data[399:801]
    assert reader.read(199, source.read) == data[801:]
    assert reader.sequence == 2


@pytest.mark.parametrize("algorithm", ALGORITHMS)
def test_multiple_mysql_packets_in_one_frame(algorithm):
    data = b"\x03\x00\x00\x01abc\x03\x00\x00\x02def"
    source = io.BytesIO(frame(compressed_payload(algorithm, data), 7, len(data)))
    reader = _compression.CompressedStream(algorithm)
    assert reader.read(4, source.read) == data[:4]
    assert reader.read(3, source.read) == b"abc"
    assert reader.read(7, source.read) == data[7:]
    assert reader.sequence == 8


@pytest.mark.parametrize("algorithm", ALGORITHMS)
@pytest.mark.parametrize(
    "invalid", ["length", "oversized", "truncated", "trailing", "corrupt"]
)
def test_invalid_compressed_frames(algorithm, invalid):
    data = b"a" * 1000
    payload = compressed_payload(algorithm, data)
    length = len(data)
    if invalid == "length":
        length += 1
    elif invalid == "oversized":
        length = 1
    elif invalid == "truncated":
        payload = payload[:-1]
    elif invalid == "trailing":
        payload += b"garbage"
    else:
        payload = b"not a compressed stream"
    with pytest.raises(err.InternalError, match="Invalid compressed packet"):
        _compression.CompressedStream(algorithm).read(
            1, io.BytesIO(frame(payload, original_length=length)).read
        )


def test_empty_compressed_frame_is_rejected():
    with pytest.raises(err.InternalError, match="Empty compressed packet"):
        _compression.CompressedStream("zlib").read(1, io.BytesIO(frame(b"")).read)


@pytest.mark.parametrize("wire", [b"\x01", frame(b"a")[:-1]])
def test_truncated_transport_closes_connection(wire):
    conn = pymysql.connect(defer_connect=True, ssl_disabled=True)
    conn._compression = _compression.CompressedStream("zlib")
    conn._sock = mock.Mock()
    conn._rfile = io.BytesIO(wire)
    with pytest.raises(err.OperationalError, match="Lost connection"):
        conn._read_bytes(1)
    assert not conn.open


def test_invalid_compression_closes_connection():
    conn = pymysql.connect(defer_connect=True, ssl_disabled=True)
    conn._compression = _compression.CompressedStream("zlib")
    conn._sock = mock.Mock()
    conn._rfile = io.BytesIO(frame(b"bad zlib", original_length=10))
    with pytest.raises(err.InternalError):
        conn._read_bytes(1)
    assert not conn.open


def test_unsolicited_compressed_error_closes_connection():
    conn = pymysql.connect(defer_connect=True, ssl_disabled=True)
    conn._compression = _compression.CompressedStream("zlib")
    conn._sock = mock.Mock()
    conn._current_timeout = None
    conn._next_seq_id = 1
    error = b"\xff" + struct.pack("<H", 4031) + b"#HY000idle timeout"
    packet = struct.pack("<I", len(error))[:3] + b"\0" + error
    conn._rfile = io.BytesIO(frame(packet))
    with pytest.raises(err.OperationalError) as exc:
        conn._read_packet()
    assert exc.value.args[0] == 2013
    assert not conn.open


def handshake_connection(compress, capabilities, **kwargs):
    conn = pymysql.connect(
        user="test", compress=compress, defer_connect=True, ssl_disabled=True, **kwargs
    )
    conn.server_version = "8.4.0"
    conn.server_capabilities = CLIENT.CAPABILITIES | capabilities
    conn.salt = b"01234567890123456789"
    conn._auth_plugin_name = "mysql_native_password"
    return conn


@pytest.mark.parametrize(
    ("requested", "capabilities", "expected"),
    [
        (None, CLIENT.COMPRESS, None),
        (False, CLIENT.COMPRESS, None),
        (True, 0, None),
        (True, CLIENT.COMPRESS, "zlib"),
        (
            True,
            CLIENT.ZSTD_COMPRESSION_ALGORITHM,
            "zstd" if _compression.zstd else None,
        ),
        ("zlib", CLIENT.COMPRESS | CLIENT.ZSTD_COMPRESSION_ALGORITHM, "zlib"),
        (
            True,
            CLIENT.COMPRESS | CLIENT.ZSTD_COMPRESSION_ALGORITHM,
            "zstd" if _compression.zstd else "zlib",
        ),
        pytest.param(
            "zstd",
            CLIENT.ZSTD_COMPRESSION_ALGORITHM,
            "zstd",
            marks=pytest.mark.skipif(
                _compression.zstd is None,
                reason="zstd requires compression.zstd or PyMySQL[zstd]",
            ),
        ),
    ],
)
def test_handshake_negotiation(requested, capabilities, expected):
    conn = handshake_connection(requested, capabilities)

    def auth_response():
        assert conn._compression is None
        return MysqlPacket(b"\0\0\0\2\0\0\0", "utf8")

    with (
        mock.patch.object(conn, "write_packet") as write,
        mock.patch.object(conn, "_read_packet", side_effect=auth_response),
    ):
        conn._request_authentication()
    data = write.call_args.args[0]
    flags = int.from_bytes(data[:4], "little")
    assert bool(flags & CLIENT.COMPRESS) == (expected == "zlib")
    assert bool(flags & CLIENT.ZSTD_COMPRESSION_ALGORITHM) == (expected == "zstd")
    assert (conn._compression.algorithm if conn._compression else None) == expected
    if expected == "zstd":
        assert data[-1] == 3  # zstd_compression_level


@pytest.mark.parametrize("algorithm", ALGORITHMS)
def test_required_algorithm_is_rejected_when_unsupported(algorithm):
    conn = handshake_connection(algorithm, 0)
    with pytest.raises(err.NotSupportedError, match="Server does not support"):
        conn._request_authentication()


def test_zstd_requires_available_backend():
    with (
        mock.patch.object(_compression, "zstd", None),
        pytest.raises(NotImplementedError, match=r"install PyMySQL\[zstd\]"),
    ):
        pymysql.connect(compress="zstd", defer_connect=True)


@pytest.mark.parametrize("python_version", [(3, 13), (3, 14)])
@pytest.mark.parametrize("available", [False, True])
def test_optional_zstd_backend_import(python_version, available):
    # Load a separate module so the active backend for other tests is preserved.
    spec = importlib.util.spec_from_file_location(
        "pymysql._compression_test", _compression.__file__
    )
    module = importlib.util.module_from_spec(spec)
    backend = object()
    selected_imports = []
    original_import = builtins.__import__

    def import_backend(name, *args, **kwargs):
        if name in ("compression", "backports"):
            selected_imports.append(name)
            if not available:
                raise ImportError("optional zstd backend is unavailable")
            return SimpleNamespace(zstd=backend)
        return original_import(name, *args, **kwargs)

    with (
        mock.patch.object(sys, "version_info", python_version),
        mock.patch.object(builtins, "__import__", side_effect=import_backend),
    ):
        spec.loader.exec_module(module)
    assert selected_imports == [
        "compression" if python_version >= (3, 14) else "backports"
    ]
    assert module.zstd is (backend if available else None)


@pytest.mark.parametrize("invalid", ["gzip", 1, 0, [], {}])
def test_invalid_option(invalid):
    with pytest.raises(ValueError, match="compress must be"):
        pymysql.connect(compress=invalid, defer_connect=True)


def test_compress_capability_flag():
    conn = handshake_connection(None, CLIENT.COMPRESS, client_flag=CLIENT.COMPRESS)
    with (
        mock.patch.object(conn, "write_packet"),
        mock.patch.object(
            conn, "_read_packet", return_value=MysqlPacket(b"\0\0\0\2\0\0\0", "utf8")
        ),
    ):
        conn._request_authentication()
    assert conn._compression.algorithm == "zlib"


def test_auth_switch_is_not_compressed():
    conn = handshake_connection("zlib", CLIENT.COMPRESS)
    responses = iter(
        [
            MysqlPacket(b"\xfemysql_native_password\0" + conn.salt + b"\0", "utf8"),
            MysqlPacket(b"\0\0\0\2\0\0\0", "utf8"),
        ]
    )

    def read():
        assert conn._compression is None
        return next(responses)

    def write(payload):
        assert conn._compression is None

    with (
        mock.patch.object(conn, "write_packet", side_effect=write),
        mock.patch.object(conn, "_read_packet", side_effect=read),
    ):
        conn._request_authentication()
    assert conn._compression.algorithm == "zlib"


def test_command_and_quit_reset_sequence():
    conn = pymysql.connect(defer_connect=True, ssl_disabled=True)
    conn._compression = _compression.CompressedStream("zlib")
    conn._compression.sequence = 23
    conn._sock = mock.Mock()
    conn._current_timeout = None
    conn._execute_command(COMMAND.COM_QUERY, "SELECT 1")
    assert conn._sock.sendall.call_args.args[0][3] == 0
    conn._compression.sequence = 23
    sock = conn._sock
    conn.close()
    assert sock.sendall.call_args.args[0][3] == 0


class TestCompression(base.PyMySQLTestCase):
    def compressed_connection(self, algorithm, **kwargs):
        if algorithm == "zstd" and _compression.zstd is None:
            pytest.skip("zstd requires compression.zstd or PyMySQL[zstd]")
        flag = (
            CLIENT.COMPRESS
            if algorithm == "zlib"
            else CLIENT.ZSTD_COMPRESSION_ALGORITHM
        )
        if not self.connections[0].server_capabilities & flag:
            pytest.skip(f"Server does not support {algorithm}")
        return self.connect(compress=algorithm, **kwargs)

    def exercise(self, algorithm, **kwargs):
        conn = self.compressed_connection(algorithm, **kwargs)
        assert conn._compression.algorithm == algorithm
        with conn.cursor() as cursor:
            for value in ("small", "a" * 1000, "abcdef" * 200000):
                cursor.execute("SELECT %s", (value,))
                assert cursor.fetchone() == (value,)
            cursor.execute("SELECT REPEAT('b', 1000000)")
            assert cursor.fetchone() == ("b" * 1000000,)
            cursor.execute("SHOW SESSION STATUS LIKE 'Compression%'")
            status = dict(cursor.fetchall())
            assert status["Compression"] == "ON"
            if "Compression_algorithm" in status:
                assert status["Compression_algorithm"] == algorithm
        conn.ping()

    def test_zlib(self):
        self.exercise("zlib")

    def test_zlib_without_tls(self):
        self.exercise("zlib", ssl_disabled=True)

    def test_zstd(self):
        self.exercise("zstd")

    def test_zstd_without_tls(self):
        self.exercise("zstd", ssl_disabled=True)

    def test_reconnect(self):
        conn = self.compressed_connection("zlib")
        conn.close()
        conn.connect()
        assert conn._compression.algorithm == "zlib"
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            assert cursor.fetchone() == (1,)

    def test_large_packet_zlib(self):
        self.large_packet("zlib")

    def test_large_packet_zstd(self):
        self.large_packet("zstd")

    def large_packet(self, algorithm):
        conn = self.compressed_connection(algorithm)
        with conn.cursor() as cursor:
            cursor.execute("SELECT @@max_allowed_packet")
            size = (1 << 24) + 100
            if cursor.fetchone()[0] < size + 1024:
                pytest.skip("Server max_allowed_packet must exceed 16 MiB")
            # Exercise both directions across the 24-bit framing limit.
            cursor.execute("SELECT LENGTH(%s)", ("a" * size,))
            assert cursor.fetchone() == (size,)
            cursor.execute("SELECT REPEAT('b', %s)", (size,))
            assert cursor.fetchone() == ("b" * size,)
            cursor.execute("SELECT 1")
            assert cursor.fetchone() == (1,)
