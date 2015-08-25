import datetime
import decimal
import pymysql
import time
import unittest2
from pymysql.tests import base


class TempUser:
    def __init__(self, c, user, db, auth, authdata=None):
        self._c = c
        self._user = user
        self._db = db
        create = "CREATE USER %s" \
                 " IDENTIFIED WITH %s" % (user, auth)
        if authdata is not None:
            create += " AS '%s'" % authdata
        try:
            c.execute(create)
            self._created = True
        except pymysql.err.InternalError:
            # already exists - TODO need to check the same plugin applies
            self._created = False
        try:
            c.execute("GRANT SELECT ON %s.* TO %s" % (db, user))
            self._grant = True
        except pymysql.err.InternalError:
            self._grant = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._grant:
            self._c.execute("REVOKE SELECT ON %s.* FROM %s" % (self._db, self._user))
        if self._created:
            self._c.execute("DROP USER %s" % self._user)

class TestAuthentication(base.PyMySQLTestCase):

    socket_auth = False
    socket_found = False
    two_questions_found = False
    three_attempts_found = False
    pam_found = False

    import os
    osuser = os.environ.get('USER')

    # socket auth requires the current user and for the connection to be a socket
    # rest do grants @localhost due to incomplete logic - TODO change to @% then
    db = base.PyMySQLTestCase.databases[0].copy()

    socket_auth = db.get('unix_socket') is not None \
                  and db.get('host') in ('localhost', '127.0.0.1')

    cur = pymysql.connect(**db).cursor()
    del db['user']
    cur.execute("SHOW PLUGINS")
    for r in cur:
        if (r[1], r[2], r[3]) ==  (u'ACTIVE', u'AUTHENTICATION', u'auth_socket.so'):
            socket_plugin_name = r[0]
            socket_found = True
        if (r[1], r[2], r[3]) ==  (u'ACTIVE', u'AUTHENTICATION', u'dialog_examples.so'):
            if r[0] == 'two_questions':
                two_questions_found =  True
            elif r[0] == 'three_attempts':
                three_attempts_found =  True
        if (r[0], r[1], r[2]) ==  (u'pam', u'ACTIVE', u'AUTHENTICATION'):
            pam_found = True
            pam_plugin_name = r[3].split('.')[0]
            if pam_plugin_name == 'auth_pam':
                pam_plugin_name = 'pam'
            # MySQL: authentication_pam
            # https://dev.mysql.com/doc/refman/5.5/en/pam-authentication-plugin.html

            # MariaDB: pam
            # https://mariadb.com/kb/en/mariadb/pam-authentication-plugin/

            # Names differ but functionality is close

    def test_plugin(self):
        # Bit of an assumption that the current user is a native password
        self.assertEqual('mysql_native_password', self.connections[0].get_plugin_name())

    @unittest2.skipUnless(socket_auth, "connection to unix_socket required")
    @unittest2.skipIf(socket_found, "socket plugin already installed")
    def testSocketAuthInstallPlugin(self):
        # needs plugin. lets install it.
        cur = self.connections[0].cursor()
        try:
            cur.execute("install plugin auth_socket soname 'auth_socket.so'")
            TestAuthentication.socket_found = True
            self.socket_plugin_name = 'auth_socket'
            self.realtestSocketAuth()
        except pymysql.err.InternalError:
            try:
                cur.execute("install soname 'auth_socket'")
                TestAuthentication.socket_found = True
                self.socket_plugin_name = 'unix_socket'
                self.realtestSocketAuth()
            except pymysql.err.InternalError:
                TestAuthentication.socket_found = False
                raise unittest2.SkipTest('we couldn\'t install the socket plugin')
        finally:
            if TestAuthentication.socket_found:
                cur = self.connections[0].cursor()
                cur.execute("uninstall plugin %s" % self.socket_plugin_name)

    @unittest2.skipUnless(socket_auth, "connection to unix_socket required")
    @unittest2.skipUnless(socket_found, "no socket plugin")
    def testSocketAuth(self):
        self.realtestSocketAuth()

    def realtestSocketAuth(self):
        with TempUser(self.connections[0].cursor(), TestAuthentication.osuser + '@localhost',
                      self.databases[0]['db'], self.socket_plugin_name) as u:
            c = pymysql.connect(user=TestAuthentication.osuser, **self.db)

    class Dialog(object):
        fail=False

        def __init__(self, con):
            self.fail=TestAuthentication.Dialog.fail
            pass

        def prompt(self, echo, prompt):
            if self.fail:
               self.fail=False
               return 'bad guess at a password'
            return self.m.get(prompt)

    @unittest2.skipUnless(socket_auth, "connection to unix_socket required")
    @unittest2.skipUnless(two_questions_found, "no two questions auth plugin")
    def testDialogAuthTwoQuestions(self):
        TestAuthentication.Dialog.fail=False
        TestAuthentication.Dialog.m = {'Password, please:': b'notverysecret',
                    'Are you sure ?': b'yes, of course'}
        with TempUser(self.connections[0].cursor(), 'pymysql_test_two_questions@localhost',
                      self.databases[0]['db'], 'two_questions', 'notverysecret') as u:
            pymysql.connect(user='pymysql_test_two_questions', plugin_map={b'dialog': TestAuthentication.Dialog}, **self.db)

    @unittest2.skipUnless(socket_auth, "connection to unix_socket required")
    @unittest2.skipUnless(three_attempts_found, "no three attempts plugin")
    def testDialogAuthThreeAttempts(self):
        TestAuthentication.Dialog.m = {'Password, please:': b'stillnotverysecret'}
        TestAuthentication.Dialog.fail=True   # fail just once. We've got three attempts after all
        with TempUser(self.connections[0].cursor(), 'pymysql_test_three_attempts@localhost',
                      self.databases[0]['db'], 'three_attempts', 'stillnotverysecret') as u:
            pymysql.connect(user='pymysql_test_three_attempts', plugin_map={b'dialog': TestAuthentication.Dialog}, **self.db)

    @unittest2.skipUnless(socket_auth, "connection to unix_socket required")
    @unittest2.skipUnless(pam_found, "no pam plugin")
    def testPamAuth(self):
        db = self.db.copy()
        db['password'] = 'bad guess at password'
        with TempUser(self.connections[0].cursor(), TestAuthentication.osuser + '@localhost',
                      self.databases[0]['db'], self.pam_plugin_name) as u:
            try:
               c = pymysql.connect(user=TestAuthentication.osuser, **db)
            except pymysql.OperationalError as e:
               self.assertEqual(1045, e.args[0])
               return
            # else we had 'bad guess at password' work with pam. Well cool


class TestConnection(base.PyMySQLTestCase):

    def test_utf8mb4(self):
        """This test requires MySQL >= 5.5"""
        arg = self.databases[0].copy()
        arg['charset'] = 'utf8mb4'
        conn = pymysql.connect(**arg)

    def test_largedata(self):
        """Large query and response (>=16MB)"""
        cur = self.connections[0].cursor()
        cur.execute("SELECT @@max_allowed_packet")
        if cur.fetchone()[0] < 16*1024*1024 + 10:
            print("Set max_allowed_packet to bigger than 17MB")
            return
        t = 'a' * (16*1024*1024)
        cur.execute("SELECT '" + t + "'")
        assert cur.fetchone()[0] == t

    def test_autocommit(self):
        con = self.connections[0]
        self.assertFalse(con.get_autocommit())

        cur = con.cursor()
        cur.execute("SET AUTOCOMMIT=1")
        self.assertTrue(con.get_autocommit())

        con.autocommit(False)
        self.assertFalse(con.get_autocommit())
        cur.execute("SELECT @@AUTOCOMMIT")
        self.assertEqual(cur.fetchone()[0], 0)

    def test_select_db(self):
        con = self.connections[0]
        current_db = self.databases[0]['db']
        other_db = self.databases[1]['db']

        cur = con.cursor()
        cur.execute('SELECT database()')
        self.assertEqual(cur.fetchone()[0], current_db)

        con.select_db(other_db)
        cur.execute('SELECT database()')
        self.assertEqual(cur.fetchone()[0], other_db)

    def test_connection_gone_away(self):
        """
        http://dev.mysql.com/doc/refman/5.0/en/gone-away.html
        http://dev.mysql.com/doc/refman/5.0/en/error-messages-client.html#error_cr_server_gone_error
        """
        con = self.connections[0]
        cur = con.cursor()
        cur.execute("SET wait_timeout=1")
        time.sleep(2)
        with self.assertRaises(pymysql.OperationalError) as cm:
            cur.execute("SELECT 1+1")
        # error occures while reading, not writing because of socket buffer.
        #self.assertEquals(cm.exception.args[0], 2006)
        self.assertIn(cm.exception.args[0], (2006, 2013))

    def test_init_command(self):
        conn = pymysql.connect(
            init_command='SELECT "bar"; SELECT "baz"',
            **self.databases[0]
        )
        c = conn.cursor()
        c.execute('select "foobar";')
        self.assertEqual(('foobar',), c.fetchone())
        conn.close()


# A custom type and function to escape it
class Foo(object):
    value = "bar"


def escape_foo(x, d):
    return x.value


class TestEscape(base.PyMySQLTestCase):
    def test_escape_string(self):
        con = self.connections[0]
        cur = con.cursor()

        self.assertEqual(con.escape("foo'bar"), "'foo\\'bar'")
        cur.execute("SET sql_mode='NO_BACKSLASH_ESCAPES'")
        self.assertEqual(con.escape("foo'bar"), "'foo''bar'")

    def test_escape_builtin_encoders(self):
        con = self.connections[0]
        cur = con.cursor()

        val = datetime.datetime(2012, 3, 4, 5, 6)
        self.assertEqual(con.escape(val, con.encoders), "'2012-03-04 05:06:00'")

    def test_escape_custom_object(self):
        con = self.connections[0]
        cur = con.cursor()

        mapping = {Foo: escape_foo}
        self.assertEqual(con.escape(Foo(), mapping), "bar")

    def test_escape_fallback_encoder(self):
        con = self.connections[0]
        cur = con.cursor()

        class Custom(str):
            pass

        mapping = {pymysql.text_type: pymysql.escape_string}
        self.assertEqual(con.escape(Custom('foobar'), mapping), "'foobar'")

    def test_escape_no_default(self):
        con = self.connections[0]
        cur = con.cursor()

        self.assertRaises(TypeError, con.escape, 42, {})

    def test_escape_dict_value(self):
        con = self.connections[0]
        cur = con.cursor()

        mapping = con.encoders.copy()
        mapping[Foo] = escape_foo
        self.assertEqual(con.escape({'foo': Foo()}, mapping), {'foo': "bar"})

    def test_escape_list_item(self):
        con = self.connections[0]
        cur = con.cursor()

        mapping = con.encoders.copy()
        mapping[Foo] = escape_foo
        self.assertEqual(con.escape([Foo()], mapping), "(bar)")
