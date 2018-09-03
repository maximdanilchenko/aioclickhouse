import logging
import asyncio
from collections import namedtuple
from aioclickhouse.connection import Connection


Stream = namedtuple("Stream", ["reader", "writer"])


class ConnectionPool:

    __slots__ = ("_loop", "_queue", "_clients", "_inited", "_closed", "_min_size")

    def __init__(
        self,
        host="127.0.0.1",
        port=7272,
        min_size=10,
        max_size=10,
        inactive_time=300,
        loop=None,
    ):
        """
        Radish connection pool holder.

        Usage:

        .. code-block:: python

            pool = await ConnectionPool(**POOL_SETTINGS)
            con = await pool.acquire()
            await con.execute("..
            await pool.close()

        Using "async with" statement:

        .. code-block:: python

            async with ConnectionPool(host='127.0.0.1') as pool:
                async with pool.acquire() as con:  # type: Connection
                    await con.execute("..

        :param host:
            Clickhouse DB server host to connect.

        :param port:
            Clickhouse DB server port to connect.

        :param min_size:
            Number of connection the pool will be initialized with.

        :param max_size:
            Maximum number of connections in the pool.

        :param inactive_time:
            After this number of seconds inactive connections should be closed.

        :param loop:
            Asyncio event loop.
        """
        if loop is None:
            loop = asyncio.get_event_loop()
        self._loop = loop
        self._queue = asyncio.LifoQueue(maxsize=max_size, loop=self._loop)
        self._clients = []
        for _ in range(max_size):
            cl = Connection(
                host=host, port=port, pool=self, inactive_time=inactive_time
            )
            self._queue.put_nowait(cl)
            cl._acquired = False
            self._clients.append(cl)

        self._inited = False
        self._closed = False
        self._min_size = min_size

    async def _init(self):
        if self._inited:
            return None
        if self._closed:
            raise Exception("Pool is closed")
        connect_tasks = [cl.connect() for cl in self._clients[-self._min_size :]]
        await asyncio.gather(*connect_tasks, loop=self._loop)
        self._inited = True
        return self

    def acquire(self):
        return PoolObjContext(self)

    async def _acquire(self):
        self._check_inited()
        cl = await self._queue.get()
        cl._acquired = True
        logging.debug(f"{cl} popped")
        return cl

    def release(self, entity):
        self._check_inited()
        self._queue.put_nowait(entity)
        entity._acquired = False
        logging.debug(f"{entity} released")

    async def close(self):
        logging.debug(f"Start closing {self}..")
        self._check_inited()
        coros = [cl.close() for cl in self._clients]
        await asyncio.gather(*coros, loop=self._loop)
        self._closed = True
        logging.debug(f"Done closing {self}")

    def _check_inited(self):
        if not self._inited:
            raise Exception("Pool is not inited")
        if self._closed:
            raise Exception("Pool is closed")

    async def __aenter__(self):
        await self._init()
        return self

    async def __aexit__(self, *exc):
        await self.close()

    def __await__(self):
        return self._init().__await__()

    def __repr__(self):
        return f"<Pool {id(self)}>"


class PoolObjContext:

    __slots__ = "pool", "pool_obj"

    def __init__(self, pool):
        self.pool = pool
        self.pool_obj = None

    async def __aenter__(self):
        self.pool_obj = await self.pool._acquire()
        return self.pool_obj

    async def __aexit__(self, *exc):
        await self.pool_obj.close()

    def __await__(self):
        return self.pool._acquire().__await__()
