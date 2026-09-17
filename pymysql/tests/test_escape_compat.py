import pymysql


def test_escape_with_encoders_mysqldb_compat():
    con = pymysql.connect(defer_connect=True)
    assert con.escape(b"bytes", con.encoders) == b"'bytes'"
    assert con.escape(False, con.encoders) == b"0"
    assert con.escape(b"bytes") == "X'6279746573'"
