import io
import struct
import unittest
from unittest import mock

import pytest

import pymysql
from pymysql.constants import CLIENT, CR, ER, SERVER_STATUS
from pymysql.tests import base


def packet(payload, sequence=1):
    return struct.pack("<I", len(payload))[:3] + bytes([sequence]) + payload


def ok_packet(status=SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT):
    return packet(b"\x00\x00\x00" + struct.pack("<HH", status, 0))


def error_packet(code, message):
    return packet(b"\xff" + struct.pack("<H", code) + b"#HY000" + message)


class TestResetProtocol(unittest.TestCase):
    def connection(self, response):
        conn = pymysql.connect(
            defer_connect=True, charset="latin1", collation="latin1_bin"
        )
        conn._sock = mock.Mock()
        conn._rfile = io.BytesIO(response)
        conn._current_timeout = None
        conn.server_status = SERVER_STATUS.SERVER_STATUS_IN_TRANS
        conn._affected_rows = 7
        self.addCleanup(conn._force_close)
        return conn

    def test_reset_command_and_state(self):
        conn = self.connection(ok_packet() + ok_packet())
        self.assertIsNone(conn.reset())
        self.assertEqual(
            conn._sock.sendall.call_args_list,
            [
                mock.call(b"\x01\x00\x00\x00\x1f"),
                mock.call(
                    packet(b"\x03SET NAMES latin1 COLLATE latin1_bin", sequence=0)
                ),
            ],
        )
        self.assertTrue(conn.get_autocommit())
        self.assertTrue(conn.autocommit_mode)
        self.assertEqual(conn.server_status, SERVER_STATUS.SERVER_STATUS_AUTOCOMMIT)
        self.assertEqual(conn.affected_rows(), 0)
        self.assertEqual(conn.insert_id(), 0)
        self.assertIsNone(conn._result)
        self.assertEqual(conn._rfile.read(), b"")

    def test_reset_autocommit_off(self):
        conn = self.connection(ok_packet(0) + ok_packet(0))
        conn.autocommit_mode = True
        conn.reset()
        self.assertFalse(conn.get_autocommit())
        self.assertFalse(conn.autocommit_mode)

    def test_closed(self):
        conn = pymysql.connect(defer_connect=True)
        with mock.patch.object(conn, "connect") as connect:
            with self.assertRaises(pymysql.InterfaceError) as error:
                conn.reset()
            self.assertEqual(error.exception.args, (0, ""))
            connect.assert_not_called()

    def test_server_error(self):
        conn = self.connection(error_packet(ER.UNKNOWN_COM_ERROR, b"Unknown command"))
        with self.assertRaises(pymysql.OperationalError) as error:
            conn.reset()
        self.assertEqual(
            error.exception.args, (ER.UNKNOWN_COM_ERROR, "Unknown command")
        )
        conn._sock.sendall.assert_called_once_with(b"\x01\x00\x00\x00\x1f")
        self.assertEqual(conn.server_status, SERVER_STATUS.SERVER_STATUS_IN_TRANS)
        self.assertEqual(conn.affected_rows(), 7)

    def test_unexpected_response(self):
        conn = self.connection(packet(b"\xfe\x00\x00\x00\x00"))
        with self.assertRaises(pymysql.OperationalError) as error:
            conn.reset()
        self.assertEqual(error.exception.args[0], CR.CR_COMMANDS_OUT_OF_SYNC)

    def test_lost_connection(self):
        conn = self.connection(b"")
        with mock.patch.object(conn, "connect") as connect:
            with self.assertRaises(pymysql.OperationalError) as error:
                conn.reset()
            self.assertEqual(error.exception.args[0], CR.CR_SERVER_LOST)
            self.assertFalse(conn.open)
            connect.assert_not_called()

    def test_charset_failure_closes_connection(self):
        conn = self.connection(
            ok_packet() + error_packet(ER.UNKNOWN_COLLATION, b"Bad collation")
        )
        with self.assertRaises(pymysql.OperationalError) as error:
            conn.reset()
        self.assertEqual(error.exception.args, (ER.UNKNOWN_COLLATION, "Bad collation"))
        self.assertFalse(conn.open)

    def test_write_failure(self):
        conn = self.connection(b"")
        conn._sock.sendall.side_effect = OSError("Connection lost")
        with mock.patch.object(conn, "connect") as connect:
            with self.assertRaises(pymysql.OperationalError) as error:
                conn.reset()
            self.assertEqual(error.exception.args[0], CR.CR_SERVER_GONE_ERROR)
            self.assertFalse(conn.open)
            connect.assert_not_called()


class TestReset(base.PyMySQLTestCase):
    def test_reuse_and_transaction_rollback(self):
        conn = self.connect(autocommit=False)
        self.safe_create_table(
            conn,
            "reset_transaction",
            "CREATE TABLE reset_transaction (id INT PRIMARY KEY AUTO_INCREMENT) ENGINE=InnoDB",
        )
        thread_id = conn.thread_id()
        with conn.cursor() as cur:
            cur.execute("INSERT INTO reset_transaction VALUES ()")
            self.assertNotEqual(conn.insert_id(), 0)
            self.assertEqual(conn.affected_rows(), 1)
        self.assertIsNone(conn.reset())
        self.assertEqual(conn.thread_id(), thread_id)
        self.assertIsNone(conn._result)
        self.assertEqual(conn.insert_id(), 0)
        self.assertEqual(conn.affected_rows(), 0)
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM reset_transaction")
            self.assertEqual(cur.fetchone(), (0,))
            cur.execute("SELECT CONNECTION_ID(), LAST_INSERT_ID()")
            self.assertEqual(cur.fetchone(), (thread_id, 0))
            cur.execute("INSERT INTO reset_transaction VALUES ()")
        conn.commit()
        conn.reset()
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM reset_transaction")
            self.assertEqual(cur.fetchone(), (1,))

    def test_session_defaults(self):
        conn = self.connect(
            autocommit=False, sql_mode="ANSI", init_command="SET @reset_init=1"
        )
        with conn.cursor() as cur:
            cur.execute("SELECT @@GLOBAL.autocommit, @@GLOBAL.sql_mode")
            autocommit, sql_mode = cur.fetchone()
            conn.autocommit(not autocommit)
            cur.execute("CREATE TEMPORARY TABLE reset_temporary (id INT)")
            cur.execute("SET @reset_variable=42")
            cur.execute("SET SESSION sql_mode='NO_BACKSLASH_ESCAPES'")
        self.assertEqual(conn.escape("a'b"), "'a''b'")
        conn.reset()
        self.assertEqual(conn.get_autocommit(), bool(autocommit))
        self.assertEqual(conn.autocommit_mode, bool(autocommit))
        with conn.cursor() as cur:
            cur.execute(
                "SELECT @@autocommit, @@sql_mode, @reset_variable, @reset_init, DATABASE()"
            )
            self.assertEqual(
                cur.fetchone(),
                (autocommit, sql_mode, None, None, self.databases[0]["database"]),
            )
            value = "a'b\\c"
            cur.execute("SELECT %s", (value,))
            self.assertEqual(cur.fetchone(), (value,))
            with self.assertRaises(pymysql.ProgrammingError) as error:
                cur.execute("SELECT * FROM reset_temporary")
            self.assertEqual(error.exception.args[0], ER.NO_SUCH_TABLE)

    def test_current_database_is_retained(self):
        conn = self.connect()
        database = self.databases[1]["database"]
        conn.select_db(database)
        conn.reset()
        with conn.cursor() as cur:
            cur.execute("SELECT DATABASE()")
            self.assertEqual(cur.fetchone(), (database,))

    def test_charset_and_collation(self):
        for charset, collation, value in [
            ("latin1", "latin1_bin", "café"),
            ("utf8mb4", "utf8mb4_bin", "雪🌍"),
            ("utf8mb4", None, "雪🌍"),
        ]:
            with self.subTest(charset=charset, collation=collation):
                conn = self.connect(charset="utf8mb4")
                conn.set_character_set(charset, collation)
                encoding = conn.encoding
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT @@character_set_client, @@character_set_connection, @@character_set_results, @@collation_connection"
                    )
                    expected = cur.fetchone()
                conn.reset()
                self.assertEqual(conn.character_set_name(), charset)
                self.assertEqual(conn.encoding, encoding)
                self.assertEqual(conn.collation, collation)
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT @@character_set_client, @@character_set_connection, @@character_set_results, @@collation_connection"
                    )
                    self.assertEqual(cur.fetchone(), expected)
                    cur.execute("SELECT %s", (value,))
                    self.assertEqual(cur.fetchone(), (value,))

    def test_pending_results(self):
        conn = self.connect(client_flag=CLIENT.MULTI_STATEMENTS)
        # Exercise the same draining path as other connection commands.
        cur = conn.cursor()
        cur.execute("SELECT 1; SELECT 2")
        conn.reset()
        cur.close()
        with conn.cursor() as cur:
            cur.execute("SELECT 3")
            self.assertEqual(cur.fetchone(), (3,))

    def test_unbuffered_result(self):
        conn = self.connect()
        cur = conn.cursor(pymysql.cursors.SSCursor)
        cur.execute("SELECT 1 UNION ALL SELECT 2")
        with pytest.warns(
            UserWarning, match="Previous unbuffered result was left incomplete"
        ):
            conn.reset()
        cur.close()
        with conn.cursor() as cur:
            cur.execute("SELECT 3")
            self.assertEqual(cur.fetchone(), (3,))

    def test_closed_connection(self):
        conn = self.connect()
        conn.close()
        with self.assertRaises(pymysql.InterfaceError):
            conn.reset()
        self.assertFalse(conn.open)
