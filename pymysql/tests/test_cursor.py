from pymysql.tests import base
from pymysql import util

__all__ = ["TestCursor"]


class TestCursor(base.PyMySQLTestCase):
    # this test case does not work quite right yet, however,
    # we substitute in None for the erroneous field which is
    # compatible with the DB-API 2.0 spec and has not broken
    # any unit tests for anything we've tried.

    #def test_description(self):
    #    """ test description attribute """
    #    # result is from MySQLdb module
    #    r = (('Host', 254, 11, 60, 60, 0, 0),
    #         ('User', 254, 16, 16, 16, 0, 0),
    #         ('Password', 254, 41, 41, 41, 0, 0),
    #         ('Select_priv', 254, 1, 1, 1, 0, 0),
    #         ('Insert_priv', 254, 1, 1, 1, 0, 0),
    #         ('Update_priv', 254, 1, 1, 1, 0, 0),
    #         ('Delete_priv', 254, 1, 1, 1, 0, 0),
    #         ('Create_priv', 254, 1, 1, 1, 0, 0),
    #         ('Drop_priv', 254, 1, 1, 1, 0, 0),
    #         ('Reload_priv', 254, 1, 1, 1, 0, 0),
    #         ('Shutdown_priv', 254, 1, 1, 1, 0, 0),
    #         ('Process_priv', 254, 1, 1, 1, 0, 0),
    #         ('File_priv', 254, 1, 1, 1, 0, 0),
    #         ('Grant_priv', 254, 1, 1, 1, 0, 0),
    #         ('References_priv', 254, 1, 1, 1, 0, 0),
    #         ('Index_priv', 254, 1, 1, 1, 0, 0),
    #         ('Alter_priv', 254, 1, 1, 1, 0, 0),
    #         ('Show_db_priv', 254, 1, 1, 1, 0, 0),
    #         ('Super_priv', 254, 1, 1, 1, 0, 0),
    #         ('Create_tmp_table_priv', 254, 1, 1, 1, 0, 0),
    #         ('Lock_tables_priv', 254, 1, 1, 1, 0, 0),
    #         ('Execute_priv', 254, 1, 1, 1, 0, 0),
    #         ('Repl_slave_priv', 254, 1, 1, 1, 0, 0),
    #         ('Repl_client_priv', 254, 1, 1, 1, 0, 0),
    #         ('Create_view_priv', 254, 1, 1, 1, 0, 0),
    #         ('Show_view_priv', 254, 1, 1, 1, 0, 0),
    #         ('Create_routine_priv', 254, 1, 1, 1, 0, 0),
    #         ('Alter_routine_priv', 254, 1, 1, 1, 0, 0),
    #         ('Create_user_priv', 254, 1, 1, 1, 0, 0),
    #         ('Event_priv', 254, 1, 1, 1, 0, 0),
    #         ('Trigger_priv', 254, 1, 1, 1, 0, 0),
    #         ('ssl_type', 254, 0, 9, 9, 0, 0),
    #         ('ssl_cipher', 252, 0, 65535, 65535, 0, 0),
    #         ('x509_issuer', 252, 0, 65535, 65535, 0, 0),
    #         ('x509_subject', 252, 0, 65535, 65535, 0, 0),
    #         ('max_questions', 3, 1, 11, 11, 0, 0),
    #         ('max_updates', 3, 1, 11, 11, 0, 0),
    #         ('max_connections', 3, 1, 11, 11, 0, 0),
    #         ('max_user_connections', 3, 1, 11, 11, 0, 0))
    #    conn = self.connections[0]
    #    c = conn.cursor()
    #    c.execute("select * from mysql.user")
    #
    #    self.assertEqual(r, c.description)

    def test_fetch_no_result(self):
        """ test a fetchone() with no rows """
        conn = self.connections[0]
        c = conn.cursor()
        c.execute("create table test_nr (b varchar(32))")
        try:
            data = "pymysql"
            c.execute("insert into test_nr (b) values (%s)", (data,))
            self.assertEqual(None, c.fetchone())
        finally:
            c.execute("drop table test_nr")

    def test_aggregates(self):
        """ test aggregate functions """
        conn = self.connections[0]
        c = conn.cursor()
        try:
            c.execute('create table test_aggregates (i integer)')
            for i in range(0, 10):
                c.execute('insert into test_aggregates (i) values (%s)', (i,))
            c.execute('select sum(i) from test_aggregates')
            r, = c.fetchone()
            self.assertEqual(sum(range(0,10)), r)
        finally:
            c.execute('drop table test_aggregates')

    def test_single_tuple(self):
        """ test a single tuple """
        conn = self.connections[0]
        c = conn.cursor()
        try:
            c.execute("create table mystuff (id integer primary key)")
            c.execute("insert into mystuff (id) values (1)")
            c.execute("insert into mystuff (id) values (2)")
            c.execute("select id from mystuff where id in %s", ((1,),))
            self.assertEqual([(1,)], list(c.fetchall()))
        finally:
            c.execute("drop table mystuff")

    def test_iterating(self):
        """https://github.com/PyMySQL/PyMySQL/issues/82"""
        conn = self.connections[0]
        c = conn.cursor()
        try:
            c.execute("create table cursor_iterate (id integer auto_increment primary key, v integer)")
            c.execute("insert into cursor_iterate (v) VALUES (1),(2),(3)")
            c.execute("select v from cursor_iterate")
            assert c.fetchone()[0] == 1
            n = 2
            for row in c:
                assert row[0] == n
                n += 1
                c.execute("select v from cursor_iterate where id=1")
                assert c.fetchone()[0] == 1
        finally:
            c.execute("drop table cursor_iterate")


if __name__ == "__main__":
    base.unittest.main()
