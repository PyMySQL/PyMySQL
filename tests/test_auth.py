"""Test for auth methods supported by MySQL 8"""

import os
import pymysql

# pymysql.connections.DEBUG = True
# pymysql._auth.DEBUG = True

host = "127.0.0.1"
port = 3306

ca = os.path.expanduser("~/ca.pem")
ssl = {'ca': ca, 'check_hostname': False}


def test_sha256_no_password():
    con = pymysql.connect(user="nopass_sha256", host=host, port=port, ssl=None)
    con.close()


def test_sha256_no_passowrd_ssl():
    con = pymysql.connect(user="nopass_sha256", host=host, port=port, ssl=ssl)
    con.close()


def test_sha256_password():
    con = pymysql.connect(user="user_sha256", password="pass_sha256", host=host, port=port, ssl=None)
    con.close()


def test_sha256_password_ssl():
    con = pymysql.connect(user="user_sha256", password="pass_sha256", host=host, port=port, ssl=ssl)
    con.close()


def test_caching_sha2_no_password():
    con = pymysql.connect(user="nopass_caching_sha2", host=host, port=port, ssl=None)
    con.close()


def test_caching_sha2_no_password():
    con = pymysql.connect(user="nopass_caching_sha2", host=host, port=port, ssl=ssl)
    con.close()


def test_caching_sha2_password():
    con = pymysql.connect(user="user_caching_sha2", password="pass_caching_sha2", host=host, port=port, ssl=None)
    con.close()

    # Fast path of caching sha2
    con = pymysql.connect(user="user_caching_sha2", password="pass_caching_sha2", host=host, port=port, ssl=None)
    con.query("FLUSH PRIVILEGES")
    con.close()


def test_caching_sha2_password_ssl():
    con = pymysql.connect(user="user_caching_sha2", password="pass_caching_sha2", host=host, port=port, ssl=ssl)
    con.close()

    # Fast path of caching sha2
    con = pymysql.connect(user="user_caching_sha2", password="pass_caching_sha2", host=host, port=port, ssl=None)
    con.query("FLUSH PRIVILEGES")
    con.close()
