from ..block import Block, BlockInfo
from ..columns.service import read_column, write_column
from ..reader import read_varint, read_binary_str
from ..writer import write_varint, write_binary_str
from ..constants import DBMS_MIN_REVISION_WITH_BLOCK_INFO


class BlockOutputStream:
    def __init__(self, fout, context):
        self.fout = fout
        self.context = context

    def reset(self):
        pass

    async def write(self, block):
        revision = self.context.server_info.revision
        if revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO:
            block.info.write(self.fout)

        # We write transposed data.
        n_columns = block.rows
        n_rows = block.columns

        write_varint(n_columns, self.fout)
        write_varint(n_rows, self.fout)

        for i, (col_name, col_type) in enumerate(block.columns_with_types):
            write_binary_str(col_name, self.fout)
            write_binary_str(col_type, self.fout)

            if n_columns:
                try:
                    items = [row[i] for row in block.data]
                except IndexError:
                    raise ValueError('Different rows length')

                write_column(self.context, col_name, col_type, items,
                             self.fout, types_check=block.types_check)

        await self.finalize()

    async def finalize(self):
        await self.fout.drain()


class BlockInputStream:
    def __init__(self, fin, context):
        self.fin = fin
        self.context = context

    def reset(self):
        pass

    async def read(self):
        info = BlockInfo()

        revision = self.context.server_info.revision
        if revision >= DBMS_MIN_REVISION_WITH_BLOCK_INFO:
            await info.read(self.fin)

        n_columns = await read_varint(self.fin)
        n_rows = await read_varint(self.fin)

        data, names, types = [], [], []

        for i in range(n_columns):
            column_name = await read_binary_str(self.fin)
            column_type = await read_binary_str(self.fin)

            names.append(column_name)
            types.append(column_type)

            if n_rows:
                column = await read_column(self.context, column_type, n_rows,
                                     self.fin)
                data.append(column)

        block = Block(
            columns_with_types=list(zip(names, types)),
            data=data,
            info=info,
            received_from_server=True
        )

        return block
