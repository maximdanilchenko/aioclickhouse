import asyncio
import struct

MAX_UINT64 = (1 << 64) - 1


def _byte(b):
    return bytes((b, ))


def write_binary_str(text: str, writer: asyncio.StreamWriter):
    text = text.encode()
    write_binary_bytes(text, writer)


def write_binary_bytes(text: bytes, writer: asyncio.StreamWriter):
    write_varint(len(text), writer)
    writer.write(text)


def write_binary_str_fixed_len(text: str, writer: asyncio.StreamWriter, length):
    text = text.encode()
    write_binary_bytes_fixed_len(text, writer, length)


def write_binary_bytes_fixed_len(text: bytes, writer: asyncio.StreamWriter, length):
    diff = length - len(text)
    if diff > 0:
        text += _byte(0) * diff
    elif diff < 0:
        raise ValueError
    writer.write(text)


def write_varint(number, writer: asyncio.StreamWriter):
    """
    Writes integer of variable length using LEB128.
    """
    while True:
        towrite = number & 0x7f
        number >>= 7
        if number:
            writer.write(_byte(towrite | 0x80))
        else:
            writer.write(_byte(towrite))
            break


def write_binary_int(number, writer: asyncio.StreamWriter, fmt):
    """
    Writes int from writerfer with provided format.
    """
    writer.write(struct.pack(f'<{fmt}', number))


def write_binary_int8(number, writer: asyncio.StreamWriter):
    write_binary_int(number, writer, 'b')


def write_binary_int16(number, writer: asyncio.StreamWriter):
    write_binary_int(number, writer, 'h')


def write_binary_int32(number, writer: asyncio.StreamWriter):
    write_binary_int(number, writer, 'i')


def write_binary_int64(number, writer: asyncio.StreamWriter):
    write_binary_int(number, writer, 'q')


def write_binary_uint8(number, writer: asyncio.StreamWriter):
    write_binary_int(number, writer, 'B')


def write_binary_uint16(number, writer: asyncio.StreamWriter):
    write_binary_int(number, writer, 'H')


def write_binary_uint32(number, writer: asyncio.StreamWriter):
    write_binary_int(number, writer, 'I')


def write_binary_uint64(number, writer: asyncio.StreamWriter):
    write_binary_int(number, writer, 'Q')


def write_binary_uint128(number, writer: asyncio.StreamWriter):
    packed = struct.pack('<QQ', (number >> 64) & MAX_UINT64, number & MAX_UINT64)
    writer.write(packed)
