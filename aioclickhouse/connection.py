import logging
import asyncio
from collections import namedtuple

from async_timeout import timeout

from aioclickhouse.writer import write_varint, write_binary_str
from aioclickhouse.reader import read_binary_str, read_varint, read_exception
from aioclickhouse.exceptions import UnexpectedPacketFromServerError
from aioclickhouse.constants import (
    ClientPacketTypes,
    ServerPacketTypes,
    DBMS_VERSION_MAJOR,
    DBMS_VERSION_MINOR,
    CLIENT_VERSION,
    DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE,
)


Packet = namedtuple('Packet', [
    'type',
    'block',
    'exception',
    'progress',
    'profile_info',
])

ServerInfo = namedtuple('ServerInfo', [
    'name',
    'version_major',
    'version_minor',
    'revision',
    'timezone',
])


class Connection:
    def __init__(
        self, host="127.0.0.1", port=9000, *, database, user, password, loop=None
    ):
        self.host = host
        self.port = port
        self._writer: asyncio.StreamWriter = None
        self._reader: asyncio.StreamReader = None
        self._connected = False
        self._loop: asyncio.BaseEventLoop = loop or asyncio.get_event_loop
        self.database = database
        self.user = user
        self.password = password
        self.client_name = 'aioclickhouse_python'
        self.server_info = None

    async def connect(self):
        self._reader, self._writer = await asyncio.open_connection(self.host, self.port, loop=self._loop)
        self._connected = True
        await self.send_hello()
        await self.receive_hello()
        logging.debug(f"{self} connected")

    async def send_hello(self):
        write_varint(ClientPacketTypes.HELLO, self._writer)
        write_binary_str(self.client_name, self._writer)
        write_varint(DBMS_VERSION_MAJOR, self._writer)
        write_varint(DBMS_VERSION_MINOR, self._writer)
        write_varint(CLIENT_VERSION, self._writer)
        write_binary_str(self.database, self._writer)
        write_binary_str(self.user, self._writer)
        write_binary_str(self.password, self._writer)

        await self._writer.drain()

    async def receive_hello(self):
        packet_type = await read_varint(self._reader)

        if packet_type == ServerPacketTypes.HELLO:
            server_name = await read_binary_str(self._reader)
            server_version_major = await read_varint(self._reader)
            server_version_minor = await read_varint(self._reader)
            server_revision = await read_varint(self._reader)

            server_timezone = None
            if server_revision >= DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE:
                server_timezone = await read_binary_str(self._reader)

            self.server_info = ServerInfo(
                server_name, server_version_major, server_version_minor,
                server_revision, server_timezone
            )
        elif packet_type == ServerPacketTypes.EXCEPTION:
            raise await read_exception(self._reader)
        else:
            self.disconnect()
            message = self.unexpected_packet_message('Hello or Exception',
                                                     packet_type)
            raise UnexpectedPacketFromServerError(message)

    def disconnect(self):
        self._writer.close()

    async def ping(self):
        timeout = self.sync_request_timeout

        async with timeout(timeout):
            try:
                write_varint(ClientPacketTypes.PING, self.fout)
                self.fout.flush()

                packet_type = read_varint(self.fin)
                while packet_type == ServerPacketTypes.PROGRESS:
                    self.receive_progress()
                    packet_type = read_varint(self.fin)

                if packet_type != ServerPacketTypes.PONG:
                    msg = self.unexpected_packet_message('Pong', packet_type)
                    raise errors.UnexpectedPacketFromServerError(msg)

            except errors.Error:
                raise

            except (socket.error, EOFError) as e:
                # It's just a warning now.
                # Current connection will be closed, new will be established.
                logger.warning(
                    'Error on %s ping: %s', self.get_description(), e
                )
                return False

        return True

    def unexpected_packet_message(self, expected, packet_type):
        packet_type = ServerPacketTypes.to_str(packet_type)

        return (
            'Unexpected packet from server {} (expected {}, got {})'
            .format(self.get_description(), expected, packet_type)
        )

    def get_description(self):
        return '{}:{}'.format(self.host, self.port)
