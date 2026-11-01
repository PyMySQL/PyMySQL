"""Framing for the MySQL compressed packet stream."""

import sys
import zlib

from . import err

try:
    if sys.version_info >= (3, 14):
        from compression import zstd
    else:
        from backports import zstd
except ImportError:
    zstd = None

MAX_PAYLOAD_LENGTH = (1 << 24) - 1
MIN_COMPRESS_LENGTH = 400


def _pack_int24(value):
    return value.to_bytes(3, "little")


class CompressedStream:
    def __init__(self, algorithm):
        self.algorithm = algorithm
        self.sequence = 0
        self._buffer = b""
        self._offset = 0

    def write(self, data, write_bytes):
        for start in range(0, len(data), MAX_PAYLOAD_LENGTH):
            payload = data[start : start + MAX_PAYLOAD_LENGTH]
            original_length = 0
            if len(payload) >= MIN_COMPRESS_LENGTH:
                if self.algorithm == "zstd":
                    compressed = zstd.compress(payload, level=3)
                else:
                    compressed = zlib.compress(payload, level=2)
                if len(compressed) < len(payload):
                    original_length = len(payload)
                    payload = compressed
            write_bytes(
                b"".join(
                    (
                        _pack_int24(len(payload)),
                        bytes([self.sequence]),
                        _pack_int24(original_length),
                        payload,
                    )
                )
            )
            self.sequence = (self.sequence + 1) % 256

    def read(self, size, read_bytes):
        parts = []
        while size:
            if self._offset == len(self._buffer):
                self._read_frame(read_bytes)
            count = min(size, len(self._buffer) - self._offset)
            parts.append(self._buffer[self._offset : self._offset + count])
            self._offset += count
            size -= count
        return b"".join(parts)

    def _read_frame(self, read_bytes):
        header = read_bytes(7)
        length = int.from_bytes(header[:3], "little")
        original_length = int.from_bytes(header[4:], "little")
        # Like libmysqlclient and go-sql-driver/mysql, accept the server's
        # sequence: an early error can precede receipt of all client frames.
        self.sequence = (header[3] + 1) % 256
        if not length:
            raise err.InternalError("Empty compressed packet")
        payload = read_bytes(length)
        if original_length:
            # These stateful decoders stop at EOF and expose no reset API.
            # Use an independent context for each frame, including across
            # connections/threads. One-shot helpers also create contexts and
            # cannot enforce the declared output limit before allocation.
            if self.algorithm == "zstd":
                decoder = zstd.ZstdDecompressor()
                decode_error = zstd.ZstdError
            else:
                decoder = zlib.decompressobj()
                decode_error = zlib.error
            try:
                # Bound output to detect invalid lengths without expanding an
                # arbitrarily large compressed payload into memory.
                payload = decoder.decompress(payload, original_length + 1)
            except decode_error as exc:
                raise err.InternalError("Invalid compressed packet") from exc
            if (
                len(payload) != original_length
                or not decoder.eof
                or decoder.unused_data
            ):
                raise err.InternalError("Invalid compressed packet length or stream")
        self._buffer = payload
        self._offset = 0
