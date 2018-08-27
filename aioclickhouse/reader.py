import asyncio
from struct import Struct
from aioclickhouse.exceptions import ServerException


async def read_binary_str(reader: asyncio.StreamReader):
    length = await read_varint(reader)
    return await read_binary_str_fixed_len(reader, length)


async def read_binary_bytes(reader: asyncio.StreamReader):
    length = await read_varint(reader)
    return await read_binary_bytes_fixed_len(reader, length)


async def read_binary_str_fixed_len(reader: asyncio.StreamReader, length):
    return (await read_binary_bytes_fixed_len(reader, length)).decode()


async def read_binary_bytes_fixed_len(reader: asyncio.StreamReader, length):
    return await reader.read(length)


async def _read_one(f: asyncio.StreamReader):
    c = await f.read(1)
    if c == b'':
        raise EOFError("Unexpected EOF while reading bytes")

    return ord(c)


async def read_varint(f):
    """
    Reads integer of variable length using LEB128.
    """
    shift = 0
    result = 0

    while True:
        i = await _read_one(f)
        result |= (i & 0x7f) << shift
        shift += 7
        if not (i & 0x80):
            break

    return result


async def read_binary_int(reader: asyncio.StreamReader, fmt):
    """
    Reads int from readerfer with provided format.
    """
    # Little endian.
    s = Struct(f'<{fmt}')
    return s.unpack(await reader.read(s.size))[0]


async def read_binary_int8(reader: asyncio.StreamReader):
    return await read_binary_int(reader, 'b')


async def read_binary_int16(reader: asyncio.StreamReader):
    return await read_binary_int(reader, 'h')


async def read_binary_int32(reader: asyncio.StreamReader):
    return await read_binary_int(reader, 'i')


async def read_binary_int64(reader: asyncio.StreamReader):
    return await read_binary_int(reader, 'q')


async def read_binary_uint8(reader: asyncio.StreamReader):
    return await read_binary_int(reader, 'B')


async def read_binary_uint16(reader: asyncio.StreamReader):
    return await read_binary_int(reader, 'H')


async def read_binary_uint32(reader: asyncio.StreamReader):
    return await read_binary_int(reader, 'I')


async def read_binary_uint64(reader: asyncio.StreamReader):
    return await read_binary_int(reader, 'Q')


async def read_binary_uint128(reader: asyncio.StreamReader):
    hi = await read_binary_int(reader, 'Q')
    lo = await read_binary_int(reader, 'Q')

    return (hi << 64) + lo


async def read_exception(buf, additional_message=None):
    code = await read_binary_int32(buf)
    name = await read_binary_str(buf)
    message = await read_binary_str(buf)
    stack_trace = await read_binary_str(buf)
    has_nested = bool(await read_binary_uint8(buf))

    new_message = ''

    if additional_message:
        new_message += additional_message + '. '

    if name != 'DB::Exception':
        new_message += name + ". "

    new_message += message + ". Stack trace:\n\n" + stack_trace

    nested = None
    if has_nested:
        nested = read_exception(buf)

    return ServerException(new_message, code, nested=nested)
