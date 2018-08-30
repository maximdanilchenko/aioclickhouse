from io import BytesIO

try:
    from clickhouse_cityhash.cityhash import CityHash128
except ImportError:
    raise RuntimeError(
        'Package clickhouse-cityhash is required to use compression'
    )

from aioclickhouse.exceptions import ChecksumDoesntMatchError


class BaseCompressor:
    """
    Partial file-like object with write method.
    """
    method = None
    method_byte = None

    def __init__(self):
        self.data = BytesIO()

    def write(self, p_str):
        self.data.write(p_str)

    def get_compressed_data(self, extra_header_size):
        raise NotImplementedError


class BaseDecompressor:
    method = None
    method_byte = None

    def __init__(self, real_stream):
        self.stream = real_stream

    def check_hash(self, compressed_data, compressed_hash):
        if CityHash128(compressed_data) != compressed_hash:
            raise ChecksumDoesntMatchError()

    def get_decompressed_data(self, method_byte, compressed_hash,
                              extra_header_size):
        raise NotImplementedError
