import os
import asyncio
import unittest
import cymysql
from cymysql.tests import base

class AsyncTestCase(base.PyMySQLTestCase):
    def test_aio_connect(self):
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
            await cur.close()
            conn.close()
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
            async with conn.cursor(cursor=cymysql.aio.AsyncDictCursor) as cur:
                await cur.execute("SELECT 42")
                result = await cur.fetchall()
                await cur.close()
            conn.close()
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

        loop = asyncio.get_event_loop()
        loop.run_until_complete(_test_select(loop))

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

        loop = asyncio.get_event_loop()
        loop.run_until_complete(_test_select(loop))


if __name__ == "__main__":
    import unittest
    unittest.main()
