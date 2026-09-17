import sys

import pymysql


def test_escape_with_encoders_default_mode():
    con = pymysql.connect(defer_connect=True)
    assert con.escape(b"bytes", con.encoders) == "X'6279746573'"
    assert con.escape(False, con.encoders) == "0"
    assert con.escape(b"bytes") == "X'6279746573'"


def test_escape_with_encoders_mysqldb_compat_mode(monkeypatch):
    monkeypatch.setitem(sys.modules, "MySQLdb", sys.modules["pymysql"])
    con = pymysql.connect(defer_connect=True)
    assert con.escape(b"bytes", con.encoders) == b"'bytes'"
    assert con.escape(bytearray(b"bytes"), con.encoders) == b"'bytes'"
    assert con.escape(False, con.encoders) == b"0"
    assert con.escape(False, con.encoders.copy()) == "0"
    assert con.escape(b"bytes") == "X'6279746573'"
