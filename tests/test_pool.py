import sys
sys.path.append('../')
import pymysql.pool

'''
$ mysql -u root -p12345678
>> create database pymysql_pool_test_db;
>> use pymysql_pool_test_db;
>> create table test(number int);
'''
host = 'localhost'
user = 'root'
passwd = '12345678'
db = 'pymysql_pool_test_db'
size = 10

from threading import Thread

def insert(mcp):
    connection = mcp.get_conn()
    n = int(time.time())
    with connection.cursor() as cursor:
        sql = "INSERT INTO `test` (`number`) VALUES (%s)"
        cursor.execute(sql, (n,))
    connection.commit()
    mcp.back_conn(connection)

import time

def test(add_n = 0):
    mcp = pymysql.pool.MySQLConnPool(host, user, passwd, db, size=size)

    task_threads = []
    for i in range(size + add_n):
        th = Thread(target=insert, args=(mcp,))
        th.start()
        task_threads.append(th)

    for i in range(size):
        task_threads[i].join()

if __name__ == '__main__':
    # test()
    test(100)
    # test(2)
