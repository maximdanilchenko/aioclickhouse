from aioclickhouse.exceptions import UnknownCompressionMethod
from aioclickhouse.constants import CompressionMethodByte


def get_compressor_cls(alg):
    try:
        module = importlib.import_module('.' + alg, __name__)
        return module.Compressor

    except ImportError:
        raise UnknownCompressionMethod(
            "Unknown compression method: '{}'".format(alg)
        )


def get_decompressor_cls(method_type):
    if method_type == CompressionMethodByte.LZ4:
        module = importlib.import_module('.lz4', __name__)

    elif method_type == CompressionMethodByte.ZSTD:
        module = importlib.import_module('.zstd', __name__)

    else:
        raise UnknownCompressionMethod()

    return module.Decompressor
