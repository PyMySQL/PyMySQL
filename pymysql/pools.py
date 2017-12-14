"""
mysql模块
一个同步的连接池和一个异步的连接池
"""
from functools import partial
import traceback
from threading import Lock, current_thread

import pymysql
import tormysql

from tornado.ioloop import IOLoop
from tornado import gen


class _MySQLConnection(dict):
    is_used = None
    connection = None


class MYSQLPool(object):
    """
    同步mysql连接池类
    """
    def __init__(self, host=None, user=None, password=None, db=None, 
            charset='utf8mb4', size=None):
        self._pool = []
        self.lock = Lock()
        kwargs = {
            'host': host,
            'user': user,
            'password': password,
            'db': db,
            'charset': charset,
        }
        self.kwargs = kwargs
        kwargs['cursorclass'] = pymysql.cursors.DictCursor
        for i in range(size):
            self._pool.append(
                _MySQLConnection(
                    is_used=False, connection=pymysql.Connection(**kwargs)))

    def close(self):
        for i in self._pool:
            i['connection'].close()

    def _get_connection(self):
        with self.lock:
            while 1:
                for conn in self._pool:
                    if not conn['is_used']:
                        conn['is_used'] = True
                        return conn
                    else:
                        continue

    def execute(self, sql, args=None):
        conn = self._get_connection()
        conn_m = conn['connection'] 
        tmp = sql[:6].lower()
        try:
            with conn_m.cursor() as cursor:
                cursor.execute(sql, args or ())
                if tmp == 'insert' :
                    insertId = conn_m.insert_id()
                    conn_m.commit()
                    # 返回插入的主键id
                    return insertId
                elif tmp in ['delete', 'update']:
                    conn_m.commit()
                elif tmp == 'select':
                    rows = cursor.fetchall()
                    result = []
                    for row in rows:
                        result.append(row)
                    return result
        except:
            err = traceback.format_exc()
            print(err)
            if tmp in ['insert', 'update', 'delete']:
                conn['connection'].rollback()
            if 'MySQL Connection not available' in err:
                conn['connection'] = _MySQLConnection(
                    is_used=False, connection=pymysql.Connection(**self.kwargs))
        finally:
            cursor.close()
            conn['is_used'] = False


class AsyncMySQLPool(object):
    """异步mysql连接池
    注意：
    * mac 下和 tornado集成会报错“ioloop is already running”
    """

    def __init__(self, host=None, user=None, password=None, db=None, 
            charset='utf8', size=None):
        """初始化连接池
        """
        self._pool = tormysql.ConnectionPool(
            max_connections=size,
            idle_seconds=7200,
            wait_connection_timeout=3,
            host=host, user=user, passwd=password, db=db, charset=charset,
            cursorclass=pymysql.cursors.DictCursor)
        self.ioloop = IOLoop.instance()

    def close(self):
        self._pool.close()

    def execute(self, sql, args):
        func = partial(self._execute, sql=sql, args=args)
        return self.ioloop.run_sync(func)

    @gen.coroutine
    def _execute(self, sql, args):
        """执行sql语句
        :param sql: sql语句
        :param args: 参数
        :return: 返回的都是数组对象
        """
        sql = sql.lower().strip()
        args = args or ()
        tmp = sql[:6]
        with (yield self._pool.Connection()) as conn:
            try:
                with conn.cursor() as cursor:
                    yield cursor.execute(sql, args=args)
                    if tmp == 'select':
                        datas = cursor.fetchall()   
                        return datas
            except Exception as e:
                err = traceback.format_exc()
                print(err)
                if tmp in ['insert', 'update', 'delete']:
                    yield conn.rollback()
            else:
                if tmp == 'insert':
                    insertId = conn.insert_id()
                    yield conn.commit()
                    # 返回插入的主键id
                    return insertId
                elif tmp in ['update', 'delete']:
                    yield conn.commit()


def mysql_pool_factory(isSync=True):
    """mysql连接池工厂
    :param isSync: 是否异步
    """
    factory = MYSQLPool if not isSync else AsyncMySQLPool
    return factory

def get_mysql_pool(host=None, user=None, password=None, charset='utf8', 
        db=None, size=None, isSync=True):
    """使用工厂方法返回一个连接池"""
    factory = mysql_pool_factory(isSync)
    kwargs = {
        'host': host,
        'user': user,
        'password': password,
        'charset': charset,
        'db': db,
        'size': size,
    }
    return factory(**kwargs)
