import socket
import logging
import asyncio
from collections import namedtuple

from async_timeout import timeout

from aioclickhouse.writer import write_varint, write_binary_str
from aioclickhouse.reader import read_binary_str, read_varint, read_exception
from aioclickhouse.exceptions import UnexpectedPacketFromServerError, Error, UnknownPacketFromServerError
from aioclickhouse.constants import (
    ClientPacketTypes,
    ServerPacketTypes,
    QueryProcessingStage,
    Compression,
    DBMS_VERSION_MAJOR,
    DBMS_VERSION_MINOR,
    CLIENT_VERSION,
    DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE,
    DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC,
    DBMS_MIN_REVISION_WITH_CLIENT_INFO,
    DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES,
)
from aioclickhouse.progress import Progress
from aioclickhouse.clientinfo import ClientInfo
from aioclickhouse.settings.writer import write_settings
from aioclickhouse.context import Context
from .streams.native import BlockInputStream
from .streams.native import BlockOutputStream


Packet = namedtuple(
    "Packet", ["type", "block", "exception", "progress", "profile_info"]
)

ServerInfo = namedtuple(
    "ServerInfo", ["name", "version_major", "version_minor", "revision", "timezone"]
)


class Connection:
    def __init__(
        self,
        host="127.0.0.1",
        port=9000,
        *,
        database="default",
        user="default",
        password="",
        loop=None,
        sync_request_timeout=DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC,
    ):
        self.host = host
        self.port = port
        self._writer: asyncio.StreamWriter = None
        self._reader: asyncio.StreamReader = None
        self._connected = False
        self._loop: asyncio.BaseEventLoop = loop or asyncio.get_event_loop()
        self.database = database
        self.user = user
        self.password = password
        self.client_name = "aioclickhouse_python"
        self.server_info = None
        self.sync_request_timeout = sync_request_timeout
        self.context = Context()
        self.compression = Compression.DISABLED
        self.compressor_cls = None
        self.compress_block_size = None

        # Block writer/reader
        self.block_in = None
        self.block_out = None

    async def connect(self):
        self._reader, self._writer = await asyncio.open_connection(
            self.host, self.port, loop=self._loop
        )
        self._connected = True
        await self.send_hello()
        await self.receive_hello()

        self.block_in = self.get_block_in_stream()
        self.block_out = self.get_block_out_stream()

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
                server_name,
                server_version_major,
                server_version_minor,
                server_revision,
                server_timezone,
            )
            self.context.server_info = self.server_info
        elif packet_type == ServerPacketTypes.EXCEPTION:
            raise await read_exception(self._reader)
        else:
            self.disconnect()
            message = self.unexpected_packet_message("Hello or Exception", packet_type)
            raise UnexpectedPacketFromServerError(message)

    def disconnect(self):
        self._writer.close()

    async def ping(self):

        async with timeout(self.sync_request_timeout):
            try:
                write_varint(ClientPacketTypes.PING, self._writer)
                self._writer.drain()

                packet_type = await read_varint(self._reader)
                while packet_type == ServerPacketTypes.PROGRESS:
                    await self.receive_progress()
                    packet_type = await read_varint(self._reader)

                if packet_type != ServerPacketTypes.PONG:
                    msg = self.unexpected_packet_message("Pong", packet_type)
                    raise UnexpectedPacketFromServerError(msg)

            except Error:
                raise

            except (socket.error, EOFError) as e:
                # It's just a warning now.
                # Current connection will be closed, new will be established.
                logging.warning("Error on %s ping: %s", self.get_description(), e)
                return False

        return True

    async def send_query(self, query, query_id=None):
        if not self._connected:
            await self.connect()

        write_varint(ClientPacketTypes.QUERY, self._writer)

        write_binary_str(query_id or '', self._writer)

        revision = self.server_info.revision
        if revision >= DBMS_MIN_REVISION_WITH_CLIENT_INFO:
            client_info = ClientInfo(self.client_name)
            client_info.query_kind = ClientInfo.QueryKind.INITIAL_QUERY

            client_info.write(revision, self._writer)

        write_settings(self.context.settings, self._writer)

        write_varint(QueryProcessingStage.COMPLETE, self._writer)
        write_varint(self.compression, self._writer)

        write_binary_str(query, self._writer)

        logging.debug('Query: %s', query)

        await self._writer.drain()

    async def receive_data(self):
        revision = self.server_info.revision

        if revision >= DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES:
            await read_binary_str(self._reader)

        block = self.block_in.read()
        self.block_in.reset()
        return block

    async def receive_packet(self):
        packet = Packet()

        packet.type = packet_type = await read_varint(self._reader)

        if packet_type == ServerPacketTypes.DATA:
            packet.block = await self.receive_data()

        elif packet_type == ServerPacketTypes.EXCEPTION:
            packet.exception = await self.receive_exception()

        elif packet.type == ServerPacketTypes.PROGRESS:
            packet.progress = await self.receive_progress()

        elif packet.type == ServerPacketTypes.PROFILE_INFO:
            packet.profile_info = await self.receive_profile_info()

        elif packet_type == ServerPacketTypes.TOTALS:
            packet.block = await self.receive_data()

        elif packet_type == ServerPacketTypes.EXTREMES:
            packet.block = await self.receive_data()

        elif packet_type == ServerPacketTypes.END_OF_STREAM:
            pass

        else:
            self.disconnect()
            raise UnknownPacketFromServerError(
                'Unknown packet {} from server {}'.format(
                    packet_type, self.get_description()
                )
            )

        return packet

    async def receive_progress(self):
        progress = Progress()
        await progress.read(self.server_info.revision, self._reader)
        return progress

    def unexpected_packet_message(self, expected, packet_type):
        packet_type = ServerPacketTypes.to_str(packet_type)

        return "Unexpected packet from server {} (expected {}, got {})".format(
            self.get_description(), expected, packet_type
        )

    def get_description(self):
        return "{}:{}".format(self.host, self.port)

    def get_block_in_stream(self):
        return BlockInputStream(self.fin, self.context)

    def get_block_out_stream(self):
        return BlockOutputStream(self.fout, self.context)
