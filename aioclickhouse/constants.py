from enum import IntEnum


DBMS_VERSION_MAJOR = 1
DBMS_VERSION_MINOR = 1

DEFAULT_PORT = 9000
DEFAULT_SECURE_PORT = 9440

DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES = 50264
DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS = 51554
DBMS_MIN_REVISION_WITH_BLOCK_INFO = 51903
DBMS_MIN_REVISION_WITH_CLIENT_INFO = 54032
DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE = 54058
DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO = 54060

# Timeouts
DBMS_DEFAULT_CONNECT_TIMEOUT_SEC = 10
DBMS_DEFAULT_TIMEOUT_SEC = 300

DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC = 5

DEFAULT_COMPRESS_BLOCK_SIZE = 1048576
DEFAULT_INSERT_BLOCK_SIZE = 1048576

CLIENT_VERSION = 54337


class ClientPacketTypes:
    """
    Packet types that client transmits
    """
    # Name, version, revision, default DB
    HELLO = 0

    # Query id, query settings, stage up to which the query must be executed,
    # whether the compression must be used, query text
    # (without data for INSERTs).
    QUERY = 1

    # A block of data (compressed or not).
    DATA = 2

    # Cancel the query execution.
    CANCEL = 3

    # Check that connection to the server is alive.
    PING = 4

    # Check status of tables on the server.
    TABLES_STATUS_REQUEST = 5

    _types_str = [
        'Hello', 'Query', 'Data', 'Cancel', 'Ping', 'TablesStatusRequest'
    ]

    @classmethod
    def to_str(cls, packet):
        return 'Unknown packet' if packet > 5 else cls._types_str[packet]


class ServerPacketTypes:
    """
    Packet types that server transmits.
    """
    # Name, version, revision.
    HELLO = 0

    # A block of data (compressed or not).
    DATA = 1

    # The exception during query execution.
    EXCEPTION = 2

    # Query execution progress: rows read, bytes read.
    PROGRESS = 3

    # Ping response
    PONG = 4

    # All packets were transmitted
    END_OF_STREAM = 5

    # Packet with profiling info.
    PROFILE_INFO = 6

    # A block with totals (compressed or not).
    TOTALS = 7

    # A block with minimums and maximums (compressed or not).
    EXTREMES = 8

    # A response to TablesStatus request.
    TABLES_STATUS_RESPONSE = 9

    _types_str = [
        'Hello', 'Data', 'Exception', 'Progress', 'Pong', 'EndOfStream',
        'ProfileInfo', 'Totals', 'Extremes', 'TablesStatusResponse'
    ]

    @classmethod
    def to_str(cls, packet):
        return 'Unknown packet' if packet > 9 else cls._types_str[packet]


class Compression(IntEnum):
    DISABLED = 0
    ENABLED = 1


class CompressionMethod(IntEnum):
    LZ4 = 1
    LZ4HC = 2
    ZSTD = 3


class CompressionMethodByte(IntEnum):
    LZ4 = 0x82
    ZSTD = 0x90


class QueryProcessingStage(IntEnum):
    """
    Determines till which state SELECT query should be executed.
    """
    FETCH_COLUMNS = 0
    WITH_MERGEABLE_STATE = 1
    COMPLETE = 2
