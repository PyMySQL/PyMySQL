"""Test close() and __del__()"""

import os
import pymysql

# pymysql.connections.DEBUG = True
# pymysql._auth.DEBUG = True

host = "127.0.0.1"
port = 3306


def test_close():
    con = pymysql.connect(user="nopass_sha256", host=host, port=port, ssl=None)
    con.close()


def test_del():
    con = pymysql.connect(user="nopass_sha256", host=host, port=port, ssl=None)
    del con


def test_close_and_explicit_del():
    con = pymysql.connect(user="nopass_sha256", host=host, port=port, ssl=None)
    con.close()
    # Call __del__() explicitly as a test (don't do this in normal code)
    con.__del__()


def test_explicit_del():
    con = pymysql.connect(user="nopass_sha256", host=host, port=port, ssl=None)
    # Call __del__() explicitly as a test (don't do this in normal code)
    con.__del__()
