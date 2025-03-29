import asyncio
import unittest
import cymysql
from cymysql.tests import base


class AsyncTestCase(base.PyMySQLTestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_aio_connect(self):
        async def _test_select():
            conn = await cymysql.aio.connect(
                host=self.test_host,
                user="root",
                passwd=self.test_passwd,
                db="mysql",
            )
            cur = conn.cursor()
            await cur.execute("SELECT 42")
            result = await cur.fetchall()
            self.assertEqual(result, [(42,)])
        asyncio.run(_test_select())

    def test_aio_connect_with_loop(self):
        loop = asyncio.new_event_loop()

        async def _test_select():
            conn = await cymysql.aio.connect(
                host=self.test_host,
                user="root",
                passwd=self.test_passwd,
                db="mysql",
                loop=loop,
            )
            cur = conn.cursor()
            await cur.execute("SELECT 42")
            result = await cur.fetchall()
            self.assertEqual(result, [(42,),])
            await cur.close()
            await conn.close()
        loop.run_until_complete(_test_select())
        loop.close()

    def test_aio_with_cursor(self):
        loop = asyncio.new_event_loop()

        async def _test_select():
            conn = await cymysql.aio.connect(
                host=self.test_host,
                user="root",
                passwd=self.test_passwd,
                db="mysql",
                loop=loop,
            )
            async with conn.cursor(cursor=cymysql.aio.AsyncCursor) as cur:
                await cur.execute("SELECT 42 a")
                result = await cur.fetchall()
                self.assertEqual(result, [(42,)])
            await conn.close()
        loop.run_until_complete(_test_select())
        loop.close()

    def test_create_pool(self):
        async def _test_select(loop):
            pool = await cymysql.aio.create_pool(
                host=self.test_host,
                user="root",
                passwd=self.test_passwd,
                db="mysql",
                loop=loop,
            )
            async with pool.acquire() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 42")
                    self.assertEqual(
                        cur.description,
                        (('42', 8, None, 3, 3, 0, 0),),
                    )
                    (r,) = await cur.fetchone()
                    self.assertEqual(r, 42)
            pool.close()
            await pool.wait_closed()

        loop = asyncio.new_event_loop()
        loop.run_until_complete(_test_select(loop))
        loop.close()

    def test_dict_cursor(self):
        async def _test_select(loop):
            pool = await cymysql.aio.create_pool(
                host=self.test_host,
                user="root",
                passwd=self.test_passwd,
                db="mysql",
                loop=loop,
            )
            async with pool.acquire() as conn:
                async with conn.cursor(cymysql.aio.AsyncDictCursor) as cur:
                    await cur.execute("SELECT 42 a")
                    self.assertEqual(
                        cur.description,
                        (('a', 8, None, 3, 3, 0, 0),),
                    )
                    self.assertEqual(await cur.fetchone(), {'a': 42})
            pool.close()
            await pool.wait_closed()

        loop = asyncio.new_event_loop()
        loop.run_until_complete(_test_select(loop))
        loop.close()

    def test_executemany(self):
        async def _test_executemany():
            try:
                conn = await cymysql.aio.connect(
                    host=self.test_host,
                    user="root",
                    passwd=self.test_passwd,
                    db="test_cymysql",
                )
                cur = conn.cursor()
                await cur.execute("CREATE TABLE async_executemany(a varchar(1))")
                await cur.executemany(
                    "insert into async_executemany values (%s)",
                    [("A", ), ("B", ), ("C", )]
                )
                await cur.execute("SELECT * FROM async_executemany")
                result = await cur.fetchall()
                self.assertEqual(result, [("A", ), ("B", ), ("C", )])
            finally:
               await cur.execute("drop table async_executemany")

        asyncio.run(_test_executemany())


    def test_rowcount(self):
        async def _test_rowcount():
            try:
                conn = await cymysql.aio.connect(
                    host=self.test_host,
                    user="root",
                    passwd=self.test_passwd,
                    db="test_cymysql",
                )
                cur = conn.cursor()
                await cur.execute("CREATE TABLE async_rowcount(a varchar(1))")
                rowcount = await cur.executemany(
                    "insert into async_rowcount values (%s)",
                    [("A", ), ("B", ), ("C", )]
                )
                self.assertEqual(rowcount, 3)
                self.assertEqual(cur.rowcount, -1)
                await cur.execute("update async_rowcount set a = NULL")
                self.assertEqual(cur.rowcount, 3)
            finally:
               await cur.execute("drop table async_rowcount")

        asyncio.run(_test_rowcount())


if __name__ == "__main__":
    unittest.main()
