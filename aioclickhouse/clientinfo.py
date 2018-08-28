import socket
import getpass
import asyncio
from enum import IntEnum

from aioclickhouse.constants import (
    DBMS_VERSION_MAJOR,
    DBMS_VERSION_MINOR,
    CLIENT_VERSION,
    DBMS_MIN_REVISION_WITH_CLIENT_INFO,
    DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO,
)
from aioclickhouse.exceptions import LogicalError
from aioclickhouse.writer import write_binary_str, write_varint, write_binary_uint8


class ClientInfo:
    class Interface(IntEnum):
        TCP = 1
        HTTP = 2

    class QueryKind(IntEnum):
        # Uninitialized object.
        NO_QUERY = 0
        INITIAL_QUERY = 1
        # Query that was initiated by another query for distributed query
        # execution.
        SECONDARY_QUERY = 2

    client_version_major = DBMS_VERSION_MAJOR
    client_version_minor = DBMS_VERSION_MINOR
    client_revision = CLIENT_VERSION
    interface = Interface.TCP

    initial_user = ""
    initial_query_id = ""
    initial_address = "0.0.0.0:0"

    quota_key = ""

    def __init__(self, client_name):
        self.query_kind = ClientInfo.QueryKind.NO_QUERY

        self.os_user = getpass.getuser()
        self.client_hostname = socket.gethostname()
        self.client_name = client_name

    @property
    def empty(self):
        return self.query_kind == ClientInfo.QueryKind.NO_QUERY

    def write(self, server_revision, writer: asyncio.StreamWriter):
        if server_revision < DBMS_MIN_REVISION_WITH_CLIENT_INFO:
            raise LogicalError(
                "Method ClientInfo.write is called for unsupported server revision"
            )

        write_binary_uint8(self.query_kind, writer)
        if self.empty:
            return

        write_binary_str(self.initial_user, writer)
        write_binary_str(self.initial_query_id, writer)
        write_binary_str(self.initial_address, writer)

        write_binary_uint8(self.interface, writer)

        write_binary_str(self.os_user, writer)
        write_binary_str(self.client_hostname, writer)
        write_binary_str(self.client_name, writer)
        write_varint(self.client_version_major, writer)
        write_varint(self.client_version_minor, writer)
        write_varint(self.client_revision, writer)

        if server_revision >= DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO:
            write_binary_str(self.quota_key, writer)
