import asyncio
from aioclickhouse.constants import DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS
from aioclickhouse.reader import read_varint


class Progress:
    def __init__(self):
        self.rows = 0
        self.bytes = 0
        self.total_rows = 0

    async def read(self, server_revision, reader: asyncio.StreamReader):
        self.rows = await read_varint(reader)
        self.bytes = await read_varint(reader)

        revision = server_revision
        if revision >= DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS:
            self.total_rows = await read_varint(reader)
