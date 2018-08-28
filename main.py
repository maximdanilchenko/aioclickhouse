import logging
import asyncio
from aioclickhouse.connection import Connection


async def main():
    con = Connection(port=32769)
    await con.connect()
    await con.ping()
    con.disconnect()


if __name__ == "__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
