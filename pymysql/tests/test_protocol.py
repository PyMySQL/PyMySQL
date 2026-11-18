"""Unit tests for reading packet fields."""

import pytest

from pymysql.protocol import MysqlPacket


@pytest.mark.parametrize(
    ("data", "expected", "position"),
    [
        (b"\x03cat!", b"cat", 4),
        (b"\x00!", b"", 1),
        (b"\xfb!", None, 1),
        (b"\xfc\x2c\x01" + b"x" * 300 + b"!", b"x" * 300, 303),
        (b"\xfd\x03\x00\x00abc!", b"abc", 7),
        (b"\xfe\x03\x00\x00\x00\x00\x00\x00\x00abc!", b"abc", 12),
        (b"\xff!", None, 1),
    ],
)
def test_read_length_coded_string(data, expected, position):
    packet = MysqlPacket(data, "utf8")
    assert packet.read_length_coded_string() == expected
    assert packet._position == position


@pytest.mark.parametrize("data", [b"\x05ab", b"\xfc\x05\x00ab"])
def test_read_length_coded_string_truncated(data):
    packet = MysqlPacket(data, "utf8")
    with pytest.raises(AssertionError):
        packet.read_length_coded_string()
