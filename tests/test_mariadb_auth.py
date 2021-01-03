"""Test for auth methods supported by MariaDB 10.3+"""

import pymysql

# pymysql.connections.DEBUG = True
# pymysql._auth.DEBUG = True

host = "127.0.0.1"
port = 3306


def test_ed25519_no_password():
    con = pymysql.connect(user="nopass_ed25519", host=host, port=port, ssl=None)
    con.close()


def test_ed25519_password():  # nosec
    con = pymysql.connect(
        user="user_ed25519", password="pass_ed25519", host=host, port=port, ssl=None
    )
    con.close()


# default mariadb docker images aren't configured with SSL
