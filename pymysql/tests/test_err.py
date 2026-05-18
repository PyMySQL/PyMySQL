import pytest
from unittest import mock

from pymysql import err
from pymysql.connections import Connection


def test_error_init_sqlstate():
    error = err.Error(1234, "boom", sqlstate="42000")
    assert error.args == (1234, "boom")
    assert error.sqlstate == "42000"

    error = err.Error(1234, "boom")
    assert error.args == (1234, "boom")
    assert error.sqlstate is None


def test_raise_mysql_exception():
    data = b"\xff\x15\x04#28000Access denied"
    with pytest.raises(err.OperationalError) as cm:
        err.raise_mysql_exception(data)
    assert cm.type == err.OperationalError
    assert cm.value.args == (1045, "Access denied")
    assert cm.value.sqlstate == "28000"

    data = b"\xff\x10\x04Too many connections"
    with pytest.raises(err.OperationalError) as cm:
        err.raise_mysql_exception(data)
    assert cm.type == err.OperationalError
    assert cm.value.args == (1040, "Too many connections")
    assert cm.value.sqlstate is None


def test_set_charset_deprecated():
    con = mock.Mock(spec=Connection)
    with pytest.warns(
        DeprecationWarning,
        match="'set_charset' is deprecated, use 'set_character_set' instead",
    ):
        Connection.set_charset(con, "utf8mb4")
    con.set_character_set.assert_called_once_with("utf8mb4")
