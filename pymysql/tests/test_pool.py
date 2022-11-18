import threading
from concurrent.futures import ThreadPoolExecutor

from pymysql.pool import Connection, ConnectionPool

mysql_config = {
    "host": "localhost",
    "port": 3306,
    "user": "root",
    "password": "root",
    "database": "test"
}


def test_connection():
    with Connection(**mysql_config) as conn:
        print("connection:", conn)
        cur = conn.cursor()
        cur.execute("show tables;")
        print("result:", cur.fetchall())
        cur.close()


def opera(pool, num):
    tn = threading.current_thread().name
    conn = pool.get_connection(delay=0.3, backoff=1.2)
    cur = conn.cursor()
    cur.execute("show tables;")
    print(f"Thread: {tn}, num: {num}, result: {cur.fetchall()}")
    cur.close()
    conn.close()


def test_connection_pool():
    pool = ConnectionPool(1, 3, **mysql_config)
    print("pool:", pool)
    conn = pool.get_connection()
    print("connection:", conn)
    cur = conn.cursor()
    cur.execute("show tables;")
    print("result:", cur.fetchall())
    cur.close()
    print('pool size:', pool.connect_num, '    pool idle connect num:', pool.size)
    conn.close()
    print('pool size:', pool.connect_num, '    pool idle connect num:', pool.size)


def test_connection_pool_with_mutilThread():
    pool = ConnectionPool(1, 5, **mysql_config)
    print('connection pool:', pool)
    threads = [threading.Thread(target=opera, args=(pool, num)) for num in range(10)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    print('pool size:', pool.size)


def test_connection_pool_with_threadPool():
    pool = ConnectionPool(1, 5, **mysql_config)
    print('connection pool:', pool)
    with ThreadPoolExecutor(max_workers=20) as executor:
        results = [executor.submit(opera, pool, num) for num in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]]

        for result in results:
            pass
