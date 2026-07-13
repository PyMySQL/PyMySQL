"""Unit tests for local_infile='only_args' behavior."""
from unittest.mock import MagicMock, patch

import pytest

from pymysql import err
from pymysql.connections import MySQLResult
from pymysql.constants import CR


def _make_mock_connection(local_infile, allowed_files=None, encoding="utf8"):
    conn = MagicMock()
    conn._local_infile = local_infile
    conn._local_infile_allowed_files = allowed_files or set()
    conn.encoding = encoding
    conn.max_allowed_packet = 16 * 1024 * 1024
    conn._closed = False
    conn._next_seq_id = 0
    return conn


def _make_mock_packet(filename_bytes):
    packet = MagicMock()
    packet.is_load_local_packet.return_value = True
    packet.get_all_data.return_value = b"\xfb" + filename_bytes
    return packet


def test_only_args_allows_file_in_args():
    """LOAD LOCAL packet for a file in the allowed set is permitted."""
    conn = _make_mock_connection("only_args", {b"/tmp/data.csv"})
    result = MySQLResult(conn)
    packet = _make_mock_packet(b"/tmp/data.csv")

    with patch("pymysql.connections.LoadLocalFile") as mock_cls:
        mock_sender = MagicMock()
        mock_cls.return_value = mock_sender
        with patch.object(result, "_read_ok_packet"):
            result._read_load_local_packet(packet)

        mock_cls.assert_called_once()
        assert mock_cls.call_args[0][0] == b"/tmp/data.csv"
        mock_sender.send_data.assert_called_once()


def test_only_args_rejects_file_not_in_args():
    """LOAD LOCAL packet for a file not in the allowed set is rejected."""
    conn = _make_mock_connection("only_args", {b"/tmp/data.csv"})
    result = MySQLResult(conn)
    packet = _make_mock_packet(b"/etc/passwd")

    with pytest.raises(err.OperationalError) as exc_info:
        result._read_load_local_packet(packet)

    assert exc_info.value.args[0] == CR.CR_LOAD_DATA_LOCAL_INFILE_REJECTED
    assert "not in query arguments" in exc_info.value.args[1]


def test_only_args_rejects_when_no_args_set():
    """LOAD LOCAL packet is rejected when no allowed files are set."""
    conn = _make_mock_connection("only_args", set())
    result = MySQLResult(conn)
    packet = _make_mock_packet(b"/tmp/data.csv")

    with pytest.raises(err.OperationalError) as exc_info:
        result._read_load_local_packet(packet)

    assert exc_info.value.args[0] == CR.CR_LOAD_DATA_LOCAL_INFILE_REJECTED


def test_true_still_allows_any_file():
    """local_infile=True continues to allow any file (backward compatibility)."""
    conn = _make_mock_connection(True)
    result = MySQLResult(conn)
    packet = _make_mock_packet(b"/etc/passwd")

    with patch("pymysql.connections.LoadLocalFile") as mock_cls:
        mock_sender = MagicMock()
        mock_cls.return_value = mock_sender
        with patch.object(result, "_read_ok_packet"):
            result._read_load_local_packet(packet)

        mock_cls.assert_called_once()
        assert mock_cls.call_args[0][0] == b"/etc/passwd"


def test_false_still_rejects_all():
    """local_infile=False continues to reject all LOAD LOCAL packets."""
    conn = _make_mock_connection(False)
    result = MySQLResult(conn)
    packet = _make_mock_packet(b"/tmp/data.csv")

    with pytest.raises(RuntimeError, match="local_infile option is false"):
        result._read_load_local_packet(packet)
