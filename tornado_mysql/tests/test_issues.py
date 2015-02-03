import datetime
import unittest
import warnings

try:
    import imp
    reload = imp.reload
except AttributeError:
    pass

from tornado.testing import gen_test
from tornado import gen

import tornado_mysql
from tornado_mysql.tests import base

__all__ = ["TestOldIssues", "TestNewIssues", "TestGitHubIssues"]


class TestOldIssues(base.PyMySQLTestCase):
    @gen_test
    def test_issue_3(self):
        """ undefined methods datetime_or_None, date_or_None """
        conn = self.connections[0]
        c = conn.cursor()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            yield c.execute("drop table if exists issue3")
        yield c.execute("create table issue3 (d date, t time, dt datetime, ts timestamp)")
        try:
            yield c.execute("insert into issue3 (d, t, dt, ts) values (%s,%s,%s,%s)", (None, None, None, None))
            yield c.execute("select d from issue3")
            self.assertEqual(None, c.fetchone()[0])
            yield c.execute("select t from issue3")
            self.assertEqual(None, c.fetchone()[0])
            yield c.execute("select dt from issue3")
            self.assertEqual(None, c.fetchone()[0])
            yield c.execute("select ts from issue3")
            self.assertTrue(isinstance(c.fetchone()[0], datetime.datetime))
        finally:
            yield c.execute("drop table issue3")

    @gen_test
    def test_issue_4(self):
        """ can't retrieve TIMESTAMP fields """
        conn = self.connections[0]
        c = conn.cursor()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            yield c.execute("drop table if exists issue4")
        yield c.execute("create table issue4 (ts timestamp)")
        try:
            yield c.execute("insert into issue4 (ts) values (now())")
            yield c.execute("select ts from issue4")
            self.assertTrue(isinstance(c.fetchone()[0], datetime.datetime))
        finally:
            yield c.execute("drop table issue4")

    @gen_test
    def test_issue_5(self):
        """ query on information_schema.tables fails """
        con = self.connections[0]
        cur = con.cursor()
        yield cur.execute("select * from information_schema.tables")

    @gen_test
    def test_issue_6(self):
        """ exception: TypeError: ord() expected a character, but string of length 0 found """
        # ToDo: this test requires access to db 'mysql'.
        kwargs = self.databases[0].copy()
        kwargs['db'] = "mysql"
        conn = yield tornado_mysql.connect(**kwargs)
        c = conn.cursor()
        yield c.execute("select * from user")
        yield conn.close_async()

    @gen_test
    def test_issue_8(self):
        """ Primary Key and Index error when selecting data """
        conn = self.connections[0]
        c = conn.cursor()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            yield c.execute("drop table if exists test")
        yield c.execute("""CREATE TABLE `test` (`station` int(10) NOT NULL DEFAULT '0', `dh`
datetime NOT NULL DEFAULT '0000-00-00 00:00:00', `echeance` int(1) NOT NULL
DEFAULT '0', `me` double DEFAULT NULL, `mo` double DEFAULT NULL, PRIMARY
KEY (`station`,`dh`,`echeance`)) ENGINE=MyISAM DEFAULT CHARSET=latin1;""")
        try:
            yield c.execute("SELECT * FROM test")
            self.assertEqual(0, c.rowcount)
            yield c.execute("ALTER TABLE `test` ADD INDEX `idx_station` (`station`)")
            yield c.execute("SELECT * FROM test")
            self.assertEqual(0, c.rowcount)
        finally:
            yield c.execute("drop table test")

    @gen_test
    def test_issue_9(self):
        """ sets DeprecationWarning in Python 2.6 """
        try:
            reload(tornado_mysql)
        except DeprecationWarning:
            self.fail()

    @gen_test
    def test_issue_13(self):
        """ can't handle large result fields """
        conn = self.connections[0]
        cur = conn.cursor()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            yield cur.execute("drop table if exists issue13")
        yield cur.execute("create table issue13 (t text)")
        try:
            # ticket says 18k
            size = 18*1024
            yield cur.execute("insert into issue13 (t) values (%s)", ("x" * size,))
            yield cur.execute("select t from issue13")
            # use assertTrue so that obscenely huge error messages don't print
            r = cur.fetchone()[0]
            self.assertTrue("x" * size == r)
        finally:
            yield cur.execute("drop table issue13")

    @gen_test
    def test_issue_15(self):
        """ query should be expanded before perform character encoding """
        conn = self.connections[0]
        c = conn.cursor()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            yield c.execute("drop table if exists issue15")
        yield c.execute("create table issue15 (t varchar(32))")
        try:
            yield c.execute("insert into issue15 (t) values (%s)", (u'\xe4\xf6\xfc',))
            yield c.execute("select t from issue15")
            self.assertEqual(u'\xe4\xf6\xfc', c.fetchone()[0])
        finally:
            yield c.execute("drop table issue15")

    @gen_test
    def test_issue_16(self):
        """ Patch for string and tuple escaping """
        conn = self.connections[0]
        c = conn.cursor()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            yield c.execute("drop table if exists issue16")
        yield c.execute("create table issue16 (name varchar(32) primary key, email varchar(32))")
        try:
            yield c.execute("insert into issue16 (name, email) values ('pete', 'floydophone')")
            yield c.execute("select email from issue16 where name=%s", ("pete",))
            self.assertEqual("floydophone", c.fetchone()[0])
        finally:
            yield c.execute("drop table issue16")

    @unittest.skip("test_issue_17() requires a custom, legacy MySQL configuration and will not be run.")
    def test_issue_17(self):
        """ could not connect mysql use passwod """
        conn = self.connections[0]
        host = self.databases[0]["host"]
        db = self.databases[0]["db"]
        c = conn.cursor()
        # grant access to a table to a user with a password
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            yield c.execute("drop table if exists issue17")
        yield c.execute("create table issue17 (x varchar(32) primary key)")
        try:
            yield c.execute("insert into issue17 (x) values ('hello, world!')")
            yield c.execute("grant all privileges on %s.issue17 to 'issue17user'@'%%' identified by '1234'" % db)
            yield conn.commit()

            conn2 = yield tornado_mysql.connect(host=host, user="issue17user", passwd="1234", db=db)
            c2 = conn2.cursor()
            yield c2.execute("select x from issue17")
            self.assertEqual("hello, world!", c2.fetchone()[0])
        finally:
            yield c.execute("drop table issue17")

class TestNewIssues(base.PyMySQLTestCase):
    @gen_test
    def test_issue_34(self):
        try:
            yield tornado_mysql.connect(host="localhost", port=1237, user="root")
            self.fail()
        except tornado_mysql.OperationalError as e:
            self.assertEqual(2003, e.args[0])
        except Exception:
            self.fail()

    @gen_test
    def test_issue_33(self):
        conn = yield tornado_mysql.connect(charset="utf8", **self.databases[0])
        c = conn.cursor()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            yield c.execute(b"drop table if exists hei\xc3\x9fe".decode("utf8"))
        try:
            yield c.execute(b"create table hei\xc3\x9fe (name varchar(32))".decode("utf8"))
            yield c.execute(b"insert into hei\xc3\x9fe (name) values ('Pi\xc3\xb1ata')".decode("utf8"))
            yield c.execute(b"select name from hei\xc3\x9fe".decode("utf8"))
            self.assertEqual(b"Pi\xc3\xb1ata".decode("utf8"), c.fetchone()[0])
        finally:
            yield c.execute(b"drop table hei\xc3\x9fe".decode("utf8"))

    @unittest.skip("This test requires manual intervention")
    @gen_test
    def test_issue_35(self):
        conn = self.connections[0]
        c = conn.cursor()
        print("sudo killall -9 mysqld within the next 10 seconds")
        try:
            c.execute("select sleep(10)")
            self.fail()
        except tornado_mysql.OperationalError as e:
            self.assertEqual(2013, e.args[0])

    @gen_test
    def test_issue_36(self):
        conn = self.connections[0]
        c = conn.cursor()
        # kill connections[0]
        yield c.execute("show processlist")
        kill_id = None
        for row in c.fetchall():
            id = row[0]
            info = row[7]
            if info == "show processlist":
                kill_id = id
                break
        try:
            # now nuke the connection
            yield conn.kill(kill_id)
            # make sure this connection has broken
            yield c.execute("show tables")
            self.fail()
        except Exception:
            pass
        # check the process list from the other connection
        try:
            c = self.connections[1].cursor()
            yield c.execute("show processlist")
            ids = [row[0] for row in c.fetchall()]
            self.assertFalse(kill_id in ids)
        finally:
            del self.connections[0]

    @gen_test
    def test_issue_37(self):
        conn = self.connections[0]
        c = conn.cursor()
        self.assertEqual(1, (yield c.execute("SELECT @foo")))
        self.assertEqual((None,), c.fetchone())
        self.assertEqual(0, (yield c.execute("SET @foo = 'bar'")))
        yield c.execute("set @foo = 'bar'")

    @gen_test
    def test_issue_38(self):
        conn = self.connections[0]
        c = conn.cursor()
        datum = "a" * 1024 * 1023 # reduced size for most default mysql installs

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            yield c.execute("drop table if exists issue38")
        try:
            yield c.execute("create table issue38 (id integer, data mediumblob)")
            yield c.execute("insert into issue38 values (1, %s)", (datum,))
        finally:
            yield c.execute("drop table issue38")

    @gen_test
    def disabled_test_issue_54(self):
        conn = self.connections[0]
        c = conn.cursor()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            yield c.execute("drop table if exists issue54")
        big_sql = "select * from issue54 where "
        big_sql += " and ".join("%d=%d" % (i,i) for i in range(0, 100000))

        try:
            yield c.execute("create table issue54 (id integer primary key)")
            yield c.execute("insert into issue54 (id) values (7)")
            yield c.execute(big_sql)
            self.assertEqual(7, c.fetchone()[0])
        finally:
            yield c.execute("drop table issue54")

class TestGitHubIssues(base.PyMySQLTestCase):
    @gen_test
    def test_issue_66(self):
        """'Connection' object has no attribute 'insert_id'"""
        conn = self.connections[0]
        c = conn.cursor()
        self.assertEqual(0, conn.insert_id())
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            yield c.execute("drop table if exists issue66")
        yield c.execute("create table issue66 (id integer primary key auto_increment, x integer)")
        try:
            yield c.execute("insert into issue66 (x) values (1)")
            yield c.execute("insert into issue66 (x) values (1)")
            self.assertEqual(2, conn.insert_id())
        finally:
            yield c.execute("drop table issue66")

    @gen_test
    def test_issue_79(self):
        """ Duplicate field overwrites the previous one in the result of DictCursor """
        conn = self.connections[0]
        c = conn.cursor(tornado_mysql.cursors.DictCursor)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            yield c.execute("drop table if exists a")
            yield c.execute("drop table if exists b")
        yield c.execute("""CREATE TABLE a (id int, value int)""")
        yield c.execute("""CREATE TABLE b (id int, value int)""")

        a=(1,11)
        b=(1,22)
        try:
            yield c.execute("insert into a values (%s, %s)", a)
            yield c.execute("insert into b values (%s, %s)", b)

            yield c.execute("SELECT * FROM a inner join b on a.id = b.id")
            r = c.fetchall()[0]
            self.assertEqual(r['id'], 1)
            self.assertEqual(r['value'], 11)
            self.assertEqual(r['b.value'], 22)
        finally:
            yield c.execute("drop table a")
            yield c.execute("drop table b")

    @gen_test
    def test_issue_95(self):
        """ Leftover trailing OK packet for "CALL my_sp" queries """
        conn = self.connections[0]
        cur = conn.cursor()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            yield cur.execute("DROP PROCEDURE IF EXISTS `foo`")
        yield cur.execute("""CREATE PROCEDURE `foo` ()
        BEGIN
            SELECT 1;
        END""")
        try:
            yield cur.execute("""CALL foo()""")
            yield cur.execute("""SELECT 1""")
            self.assertEqual(cur.fetchone()[0], 1)
        finally:
            yield cur.execute("DROP PROCEDURE IF EXISTS `foo`")

    @gen_test
    def test_issue_114(self):
        """ autocommit is not set after reconnecting with ping() """
        conn = yield tornado_mysql.connect(charset="utf8", **self.databases[0])
        yield conn.autocommit(False)
        c = conn.cursor()
        yield c.execute("""select @@autocommit;""")
        self.assertFalse(c.fetchone()[0])
        yield conn.close_async()
        yield conn.ping()
        yield c.execute("""select @@autocommit;""")
        self.assertFalse(c.fetchone()[0])
        yield conn.close_async()

        # Ensure autocommit() is still working
        conn = yield tornado_mysql.connect(charset="utf8", **self.databases[0])
        c = conn.cursor()
        yield c.execute("""select @@autocommit;""")
        self.assertFalse(c.fetchone()[0])
        yield conn.close_async()
        yield conn.ping()
        yield conn.autocommit(True)
        yield c.execute("""select @@autocommit;""")
        self.assertTrue(c.fetchone()[0])
        yield conn.close_async()

    @gen_test
    def test_issue_175(self):
        """ The number of fields returned by server is read in wrong way """
        conn = self.connections[0]
        cur = conn.cursor()
        for length in (200, 300):
            columns = ', '.join('c{0} integer'.format(i) for i in range(length))
            sql = 'create table test_field_count ({0})'.format(columns)
            try:
                yield cur.execute(sql)
                yield cur.execute('select * from test_field_count')
                assert len(cur.description) == length
            finally:
                yield cur.execute('drop table test_field_count')


if __name__ == "__main__":
    import unittest
    unittest.main()
