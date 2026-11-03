"""Independent, read-only reconstruction of the Disk Manager backup format.

The wire format follows dataplane/backup/{meta,s3,encrypt,keys}.go,
protos/backup_chunk_map.proto and snapshot/storage/{chunks,compressor}.
Only a supplied backup ObjectStore is read; the source disk/YDB is never used.
"""

import base64
import binascii
import hashlib
import json
import os
import re
import struct
import sys
import tempfile
import zlib
from dataclasses import dataclass
from pathlib import Path

from .transport import (AuthError, BackupError, InvalidBackup, ObjectNotFound,
                        PendingBackup, remaining)


CHUNK_SIZE = 4 * 1024 * 1024
MAX_CHUNK_MAP_BYTES = 64 * 1024 * 1024
MAX_CHUNK_ID_BYTES = 1024


class MissingDecryptionKey(AuthError):
    """The object names a KEK that was deliberately not supplied."""


class RejectedDecryptionKey(InvalidBackup):
    """AES-GCM rejected the supplied KEK while unwrapping the object DEK."""


@dataclass(frozen=True)
class RestoreReport:
    snapshot_id: str
    disk_id: str
    size_bytes: int
    sha256: str
    chunk_count: int
    nonzero_chunks: int
    encryption_key_ids: tuple


def _identifier(value):
    if (not isinstance(value, str) or not value or len(value.encode()) > MAX_CHUNK_ID_BYTES
            or value in (".", "..") or any(ord(c) < 33 or ord(c) == 127 for c in value)
            or "/" in value or "\\" in value):
        raise InvalidBackup("Invalid backup identifier")
    return value


def _chunk_map(data, expected_count):
    """Decode the deliberately tiny proto3 schema, failing on unknown fields.

    Every entry (including a zero-length string) represents one disk position.
    Unknown fields are rejected so a format extension cannot silently change
    reconstruction semantics. This parser does not implement general protobuf.
    """
    result = []
    offset = 0
    while offset < len(data):
        if data[offset] != 0x0A:  # field 1, length-delimited string
            raise InvalidBackup("Unsupported chunk map field")
        offset += 1
        size = 0
        for shift in range(0, 35, 7):
            if offset >= len(data):
                raise InvalidBackup("Truncated chunk map")
            octet = data[offset]
            offset += 1
            size |= (octet & 127) << shift
            if not octet & 128:
                break
        else:
            raise InvalidBackup("Invalid chunk map length")
        if size > MAX_CHUNK_ID_BYTES or offset + size > len(data):
            raise InvalidBackup("Invalid chunk map entry")
        try:
            value = data[offset:offset + size].decode("utf-8", errors="strict")
        except UnicodeError:
            raise InvalidBackup("Invalid chunk map identifier encoding") from None
        if value:
            _identifier(value)
        result.append(value)
        if len(result) > expected_count:
            raise InvalidBackup("Chunk map is larger than the expected disk")
        offset += size
    if len(result) != expected_count:
        raise InvalidBackup("Chunk map does not cover the expected disk")
    return result


def _json_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise InvalidBackup("Duplicate snapshot metadata field")
        result[key] = value
    return result


def _decode_zstd(data):
    if not data or len(data) > CHUNK_SIZE * 2:
        raise InvalidBackup("Invalid compressed Zstandard size")
    if getattr(sys, "is_standalone_binary", False):
        # YA's python-zstandard binding is version-coupled to a different zstd
        # release. Use our tiny stable-public-API adapter, not that binding and
        # never an unchecked bypass of its version guard.
        from ._zstd import decompress
        return decompress(data, CHUNK_SIZE)
    import zstandard

    size = zstandard.frame_content_size(data)
    # frame_content_size() uses -1, whereas CONTENTSIZE_UNKNOWN exposes the
    # unsigned C constant in python-zstandard 0.25.0.
    if size not in (CHUNK_SIZE, -1, zstandard.CONTENTSIZE_UNKNOWN):
        raise InvalidBackup("Invalid Zstandard frame size")
    return zstandard.ZstdDecompressor(max_window_size=CHUNK_SIZE).decompress(
        data, max_output_size=CHUNK_SIZE, allow_extra_data=False)


def _decode_chunk(data, metadata):
    compression = metadata.get("compression", "")
    checksum = metadata.get("checksum")
    if checksum is None or not re.fullmatch(r"[0-9]{1,10}", checksum):
        raise InvalidBackup("Chunk checksum is missing or malformed")
    checksum = int(checksum)
    if checksum > 0xFFFFFFFF:
        raise InvalidBackup("Chunk checksum is out of range")
    try:
        if compression == "":
            plain = data
        elif compression == "gzip":
            decoder = zlib.decompressobj(16 + zlib.MAX_WBITS)
            plain = decoder.decompress(data, CHUNK_SIZE + 1)
            if not decoder.eof or decoder.unused_data or decoder.unconsumed_tail:
                raise InvalidBackup("Invalid gzip chunk framing")
        elif compression == "lz4":
            import lz4.frame
            decoder = lz4.frame.LZ4FrameDecompressor()
            plain = decoder.decompress(data, max_length=CHUNK_SIZE + 1)
            if not decoder.eof or decoder.unused_data:
                raise InvalidBackup("Invalid LZ4 chunk framing")
        elif compression == "lz4_block":
            import lz4.block
            if len(data) < 8 or struct.unpack("<Q", data[:8])[0] != CHUNK_SIZE:
                raise InvalidBackup("Invalid LZ4 block size")
            plain = lz4.block.decompress(data[8:], uncompressed_size=CHUNK_SIZE)
        elif compression in ("zstd", "zstd_cgo"):
            if compression == "zstd":
                if len(data) < 8 or struct.unpack("<Q", data[:8])[0] != CHUNK_SIZE:
                    raise InvalidBackup("Invalid Zstandard block size")
                data = data[8:]
            plain = _decode_zstd(data)
        else:
            raise InvalidBackup("Unsupported chunk compression")
    except ImportError:
        raise BackupError("Install the pinned backup-reader dependencies") from None
    except BackupError:
        raise
    except Exception:
        # Native codec errors can contain data: never propagate their text.
        raise InvalidBackup("Chunk decompression failed") from None
    if len(plain) != CHUNK_SIZE:
        raise InvalidBackup("Decoded chunk has an unexpected size")
    if zlib.crc32(plain) != checksum:
        raise InvalidBackup("Chunk CRC32 mismatch")
    return plain


class BackupReader:
    def __init__(self, store, keys=None, require_encryption=False):
        self.store = store
        self.keys = {}
        for key_id, key in (keys or {}).items():
            _identifier(key_id)
            if not isinstance(key, bytes):
                raise AuthError("A backup KEK must contain raw bytes")
            # Same rule as backup.NewS3: preserve an exactly 32-byte raw key,
            # even if its boundary bytes are whitespace. No base64 guessing.
            if len(key) != 32:
                key = key.strip()
            if len(key) != 32:
                raise AuthError("A backup KEK must contain 32 raw bytes")
            self.keys[key_id] = key
        self.require_encryption = require_encryption

    def _object(self, key, deadline, max_bytes, used_keys):
        remaining(deadline)
        obj = self.store.get(key, deadline=deadline, max_bytes=max_bytes)
        remaining(deadline)
        if len(obj.data) > max_bytes:
            raise InvalidBackup("Backup object exceeds size limit")
        key_id = obj.metadata.get("key-id")
        wrapped = obj.metadata.get("encrypted-dek")
        if key_id is None and wrapped is None:
            if self.require_encryption:
                raise InvalidBackup("An unencrypted backup object was found")
            return obj.data, obj.metadata
        if not key_id or not wrapped:
            raise InvalidBackup("Incomplete backup encryption metadata")
        if key_id not in self.keys:
            raise MissingDecryptionKey("Required backup decryption key is unavailable")
        try:
            from cryptography.exceptions import InvalidTag
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        except ImportError:
            raise BackupError("Install the pinned backup-reader dependencies") from None
        try:
            encrypted_dek = base64.b64decode(wrapped, validate=True)
            if len(encrypted_dek) != 12 + 32 + 16 or len(obj.data) < 12 + 16:
                raise InvalidBackup("Invalid encrypted backup object size")
            # Key prefix is intentionally excluded: Go encrypts before s.Key().
            dek = AESGCM(self.keys[key_id]).decrypt(
                encrypted_dek[:12], encrypted_dek[12:], key_id.encode("utf-8"))
        except InvalidTag:
            raise RejectedDecryptionKey("Backup KEK authentication failed") from None
        except (ValueError, binascii.Error):
            raise InvalidBackup("Invalid encrypted backup key metadata") from None
        try:
            if len(dek) != 32:
                raise InvalidBackup("Invalid decrypted data key size")
            plain = AESGCM(dek).decrypt(obj.data[:12], obj.data[12:], key.encode("utf-8"))
        except (InvalidTag, ValueError, binascii.Error):
            raise InvalidBackup("Backup authentication failed (key, data or AAD mismatch)") from None
        used_keys.add(key_id)
        return plain, obj.metadata

    def check_key_rejection(self, snapshot_id, disk_id, size_bytes, destination, *,
                            deadline, expected_sha256, wrong_key):
        """Probe the real backup with a missing or certainly different KEK.

        Call only after a complete positive restore. Transport/auth failures,
        missing objects and generic corruption are not evidence of key rejection.
        No configured key or remote object is changed.
        """
        keys = ({key_id: bytes([key[0] ^ 1]) + key[1:] for key_id, key in self.keys.items()}
                if wrong_key else {})
        expected_error = RejectedDecryptionKey if wrong_key else MissingDecryptionKey
        probe = BackupReader(self.store, keys, require_encryption=True)
        try:
            probe.restore(snapshot_id, disk_id, size_bytes, destination,
                          deadline=deadline, expected_sha256=expected_sha256)
        except expected_error:
            if os.path.lexists(destination):
                raise InvalidBackup("Key rejection left a published restore image") from None
            return
        raise InvalidBackup("Backup restore unexpectedly accepted an unavailable or incorrect key")

    def restore(self, snapshot_id, disk_id, size_bytes, destination, *, deadline,
                expected_sha256=None):
        """Validate and atomically create a private raw image; never overwrite.

        Missing map means not ready. Once the map is published, missing metadata
        or a referenced chunk is an invalid backup, never a source-data fallback.
        A returned report means *all* bytes, holes and chunk CRCs were checked.
        """
        _identifier(snapshot_id)
        _identifier(disk_id)
        if not isinstance(size_bytes, int) or isinstance(size_bytes, bool) or size_bytes <= 0:
            raise ValueError("A positive expected disk size is required")
        if expected_sha256 is not None and not re.fullmatch(r"[0-9a-fA-F]{64}", expected_sha256):
            raise ValueError("Expected SHA-256 must be a hexadecimal digest")
        destination = Path(destination)
        if os.path.lexists(destination):
            raise BackupError("Restore destination already exists; refusing overwrite")
        remaining(deadline)
        count = (size_bytes + CHUNK_SIZE - 1) // CHUNK_SIZE
        used_keys = set()
        try:
            map_data, _ = self._object("chunk_maps/" + snapshot_id, deadline,
                                       min(MAX_CHUNK_MAP_BYTES, count * 1028) + 28, used_keys)
        except ObjectNotFound:
            raise PendingBackup("Backup chunk map is not yet published") from None
        chunks = _chunk_map(map_data, count)
        try:
            meta_data, _ = self._object(
                "snapshots/" + disk_id + "/" + snapshot_id + "/meta.json",
                deadline, 1024 * 1024, used_keys)
        except ObjectNotFound:
            raise InvalidBackup("Published backup has no snapshot metadata") from None
        try:
            meta = json.loads(meta_data, object_pairs_hook=_json_object)
        except (ValueError, UnicodeError):
            raise InvalidBackup("Invalid snapshot metadata JSON") from None
        if (not isinstance(meta, dict) or meta.get("id") != snapshot_id
                or meta.get("disk_id") != disk_id or type(meta.get("size")) is not int
                or meta["size"] != size_bytes):
            raise InvalidBackup("Snapshot metadata identity or disk size mismatch")
        if meta.get("encryption_mode", 0) != 0:
            raise InvalidBackup("Source-disk encryption is not supported by this tester")
        descriptor = None
        temporary = None
        try:
            descriptor, temporary = tempfile.mkstemp(prefix=".snapshot-backup-", dir=destination.parent)
            digest = hashlib.sha256()
            zeros = bytes(CHUNK_SIZE)
            with os.fdopen(descriptor, "wb") as output:
                descriptor = None
                for index, chunk_id in enumerate(chunks):
                    remaining(deadline)
                    length = min(CHUNK_SIZE, size_bytes - index * CHUNK_SIZE)
                    if chunk_id:
                        try:
                            data, metadata = self._object("chunks/" + chunk_id, deadline,
                                                          CHUNK_SIZE * 2 + 28, used_keys)
                        except ObjectNotFound:
                            raise InvalidBackup("Published backup references a missing chunk") from None
                        data = _decode_chunk(data, metadata)
                        output.write(data[:length])
                        digest.update(data[:length])
                    else:
                        output.seek(length, os.SEEK_CUR)
                        digest.update(zeros[:length])
                output.truncate(size_bytes)
                output.flush()
                os.fsync(output.fileno())
            actual = digest.hexdigest()
            if expected_sha256 is not None and actual != expected_sha256.lower():
                raise InvalidBackup("Restored disk SHA-256 does not match the reference")
            remaining(deadline)
            # link() is atomic and fails even if a symlink appeared concurrently.
            # os.replace() would silently overwrite the destination and is unsafe.
            os.link(temporary, destination)
            return RestoreReport(snapshot_id, disk_id, size_bytes, actual, count,
                                 sum(bool(chunk) for chunk in chunks), tuple(sorted(used_keys)))
        except OSError:
            raise BackupError("Unable to create the private restore image") from None
        finally:
            if descriptor is not None:
                os.close(descriptor)
            if temporary is not None:
                os.unlink(temporary)
