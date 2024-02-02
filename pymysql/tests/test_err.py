import pytest
from pymysql import err


def test_raise_mysql_exception():
    data = b"\xff\x15\x04#28000Access denied"
    with pytest.raises(err.OperationalError) as cm:
        err.raise_mysql_exception(data)
    assert cm.type == err.OperationalError
    assert cm.value.args == (1045, "Access denied")

    data = b"\xff\x10\x04Too many connections"
    with pytest.raises(err.OperationalError) as cm:
        err.raise_mysql_exception(data)
    assert cm.type == err.OperationalError
    assert cm.value.args == (1040, "Too many connections")
