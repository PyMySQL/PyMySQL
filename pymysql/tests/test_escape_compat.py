import pymysql


def test_escape_with_encoders_mysqldb_compat():
    con = pymysql.connect(defer_connect=True)
    assert con.escape(b"bytes", con.encoders) == "X'6279746573'"
    assert con.escape(False, con.encoders) == "0"

    prev = pymysql.connections._MYSQLDB_ESCAPE_COMPAT
    try:
        pymysql.install_as_MySQLdb()
        assert con.escape(b"bytes", con.encoders) == b"'bytes'"
        assert con.escape(False, con.encoders) == b"0"
    finally:
        pymysql.connections._MYSQLDB_ESCAPE_COMPAT = prev
    assert con.escape(b"bytes") == "X'6279746573'"
    assert con.escape(False, con.encoders) == "0"
