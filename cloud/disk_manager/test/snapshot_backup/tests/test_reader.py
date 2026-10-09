import base64
import gzip
import hashlib
import json
import os
from pathlib import Path
import struct
import tempfile
import time
import unittest
from unittest import mock
import zlib

from cloud.disk_manager.test.snapshot_backup.reader import BackupReader, CHUNK_SIZE
from cloud.disk_manager.test.snapshot_backup.tests.zstd_vectors import (
    FRAME, UNKNOWN_SIZE_FRAME, SHORT_FRAME, LONG_FRAME,
)
from cloud.disk_manager.test.snapshot_backup.transport import (
    AuthError, BackupError, BackupTimeout, InvalidBackup, ObjectNotFound,
    PendingBackup, StoredObject, TransportError,
)


def chunk_map(*ids):
    # Independent fixture encoding of BackupChunkMap repeated string field 1.
    result = bytearray()
    for value in ids:
        data = value.encode()
        result.append(10)
        length = len(data)
        while length >= 128:
            result.append((length & 127) | 128)
            length >>= 7
        result.append(length)
        result.extend(data)
    return bytes(result)


class MemoryStore:
    def __init__(self, objects):
        self.objects = objects
        self.requests = []

    def get(self, key, *, deadline, max_bytes):
        self.requests.append(key)
        if key not in self.objects:
            raise ObjectNotFound("Fixture object absent")
        return self.objects[key]


class ReaderTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.destination = Path(self.directory.name) / "restored.raw"
        self.size = CHUNK_SIZE * 3
        self.data = (bytes(range(256)) * (CHUNK_SIZE // 256))
        self.reference = self.data + bytes(CHUNK_SIZE) + self.data
        self.objects = {
            "chunk_maps/snapshot": StoredObject(chunk_map("parent.chunk", "", "parent.chunk"), {}),
            "snapshots/disk/snapshot/meta.json": StoredObject(json.dumps({
                "id": "snapshot", "disk_id": "disk", "size": self.size,
                "encryption_mode": 0}).encode(), {}),
            "chunks/parent.chunk": StoredObject(self.data, {"Checksum": str(zlib.crc32(self.data))}),
        }

    def restore(self, **kwargs):
        store = MemoryStore(self.objects)
        reader = BackupReader(store, **kwargs.pop("reader_options", {}))
        result = reader.restore("snapshot", "disk", self.size, self.destination,
                                deadline=kwargs.pop("deadline", time.monotonic() + 30), **kwargs)
        return result, store

    def assert_no_partial_output(self):
        self.assertEqual(list(Path(self.directory.name).iterdir()), [])

    def test_full_image_includes_zero_hole_and_inherited_chunk(self):
        result, store = self.restore(expected_sha256=hashlib.sha256(self.reference).hexdigest())
        self.assertEqual(self.destination.read_bytes(), self.reference)
        self.assertEqual(result.sha256, hashlib.sha256(self.reference).hexdigest())
        self.assertEqual(result.size_bytes, self.size)
        self.assertEqual((result.chunk_count, result.nonzero_chunks), (3, 2))
        self.assertEqual(os.stat(self.destination).st_mode & 0o777, 0o600)
        self.assertEqual(store.requests, ["chunk_maps/snapshot", "snapshots/disk/snapshot/meta.json",
                                          "chunks/parent.chunk", "chunks/parent.chunk"])
        with self.assertRaises(AttributeError):
            result.sha256 = "wrong"

    def test_zero_only_backup_needs_no_chunk_objects(self):
        self.objects["chunk_maps/snapshot"] = StoredObject(chunk_map("", "", ""), {})
        result, store = self.restore()
        self.assertEqual(result.nonzero_chunks, 0)
        self.assertEqual(len(store.requests), 2)
        self.assertEqual(self.destination.read_bytes(), bytes(self.size))

    def test_last_partial_chunk_is_validated_then_cropped_to_exact_disk_size(self):
        self.size = CHUNK_SIZE + 4096
        self.objects["chunk_maps/snapshot"] = StoredObject(chunk_map("parent.chunk", "parent.chunk"), {})
        self.objects["snapshots/disk/snapshot/meta.json"] = StoredObject(json.dumps({
            "id": "snapshot", "disk_id": "disk", "size": self.size}).encode(), {})
        self.restore()
        self.assertEqual(self.destination.read_bytes(), self.data + self.data[:4096])

    def test_missing_final_map_is_pending(self):
        del self.objects["chunk_maps/snapshot"]
        with self.assertRaises(PendingBackup):
            self.restore()
        self.assert_no_partial_output()

    def test_missing_referenced_chunk_or_published_metadata_is_invalid(self):
        for key in ("chunks/parent.chunk", "snapshots/disk/snapshot/meta.json"):
            with self.subTest(key=key):
                obj = self.objects.pop(key)
                with self.assertRaises(InvalidBackup):
                    self.restore()
                self.objects[key] = obj
                self.assert_no_partial_output()

    def test_identity_size_type_and_duplicate_metadata_are_rejected(self):
        original = self.objects["snapshots/disk/snapshot/meta.json"]
        for patch in ({"id": "other"}, {"disk_id": "other"}, {"size": 1},
                      {"size": str(self.size)}, {"size": True}, {"encryption_mode": 1}):
            with self.subTest(patch=patch):
                meta = json.loads(original.data)
                meta.update(patch)
                self.objects["snapshots/disk/snapshot/meta.json"] = StoredObject(json.dumps(meta).encode(), {})
                with self.assertRaises(InvalidBackup):
                    self.restore()
                self.assert_no_partial_output()
        self.objects["snapshots/disk/snapshot/meta.json"] = StoredObject(
            b'{"id":"wrong","id":"snapshot","disk_id":"disk","size":12582912}', {})
        with self.assertRaises(InvalidBackup):
            self.restore()

    def test_invalid_maps_and_incomplete_coverage_are_rejected(self):
        for data in (b"", b"\x0a\x80", b"\x0a\xff\xff\xff\xff\xff", b"\x12\x00",
                     b"\x0a\x01\xff", chunk_map("x"), chunk_map("", "", "", ""),
                     chunk_map("../wrong", "", ""), chunk_map("/wrong", "", "")):
            with self.subTest(data=data):
                self.objects["chunk_maps/snapshot"] = StoredObject(data, {})
                with self.assertRaises(InvalidBackup):
                    self.restore()
                self.assert_no_partial_output()

    def test_crc_missing_malformed_or_mismatch_is_invalid(self):
        for metadata in ({}, {"Checksum": "-1"}, {"Checksum": "4294967296"},
                         {"Checksum": "0"}, {"Checksum": "1.0"}):
            with self.subTest(metadata=metadata):
                self.objects["chunks/parent.chunk"] = StoredObject(self.data, metadata)
                with self.assertRaises(InvalidBackup):
                    self.restore()
                self.assert_no_partial_output()

    def test_full_reference_digest_mismatch_leaves_no_output(self):
        with self.assertRaisesRegex(InvalidBackup, "SHA-256"):
            self.restore(expected_sha256="0" * 64)
        self.assert_no_partial_output()

    def test_existing_file_and_dangling_symlink_are_not_overwritten(self):
        self.destination.write_bytes(b"keep")
        with self.assertRaises(BackupError):
            self.restore()
        self.assertEqual(self.destination.read_bytes(), b"keep")
        self.destination.unlink()
        self.destination.symlink_to(Path(self.directory.name) / "not-present")
        with self.assertRaises(BackupError):
            self.restore()
        self.assertTrue(self.destination.is_symlink())

    def test_concurrent_destination_creation_is_not_overwritten(self):
        link = os.link

        def race(source, destination):
            Path(destination).write_bytes(b"someone-else")
            return link(source, destination)

        with mock.patch("os.link", side_effect=race), self.assertRaises(BackupError):
            self.restore()
        self.assertEqual(self.destination.read_bytes(), b"someone-else")
        self.assertEqual(list(Path(self.directory.name).iterdir()), [self.destination])

    def test_deadline_prevents_any_requests(self):
        with self.assertRaises(BackupTimeout):
            self.restore(deadline=time.monotonic() - 1)
        self.assert_no_partial_output()

    def test_all_current_compression_formats(self):
        import lz4.block
        import lz4.frame

        encodings = {
            "": self.data, "gzip": gzip.compress(self.data),
            "lz4": lz4.frame.compress(self.data),
            "lz4_block": struct.pack("<Q", CHUNK_SIZE) + lz4.block.compress(self.data, store_size=False),
            "zstd": struct.pack("<Q", CHUNK_SIZE) + FRAME, "zstd_cgo": FRAME,
        }
        for codec, encoded in encodings.items():
            with self.subTest(codec=codec):
                self.objects["chunks/parent.chunk"] = StoredObject(encoded, {
                    "Checksum": str(zlib.crc32(self.data)), "Compression": codec})
                self.restore()
                self.assertEqual(self.destination.read_bytes(), self.reference)
                self.destination.unlink()

    def test_zstd_without_content_size_restores_the_same_complete_chunk(self):
        for codec in ("zstd", "zstd_cgo"):
            with self.subTest(codec=codec):
                data = UNKNOWN_SIZE_FRAME
                if codec == "zstd":
                    data = struct.pack("<Q", CHUNK_SIZE) + data
                self.objects["chunks/parent.chunk"] = StoredObject(data, {
                    "Checksum": str(zlib.crc32(self.data)), "Compression": codec})
                self.restore()
                self.assertEqual(self.destination.read_bytes(), self.reference)
                self.destination.unlink()

    def test_zstd_corruption_truncation_extra_frames_and_wrong_sizes_are_invalid(self):
        corrupted = FRAME[:-1] + bytes([FRAME[-1] ^ 1])
        for codec in ("zstd", "zstd_cgo"):
            for frame in (corrupted, FRAME[:-1], FRAME + b"trailing", FRAME + FRAME,
                          UNKNOWN_SIZE_FRAME + b"trailing", SHORT_FRAME, LONG_FRAME, b""):
                with self.subTest(codec=codec, length=len(frame)):
                    data = struct.pack("<Q", CHUNK_SIZE) + frame if codec == "zstd" else frame
                    self.objects["chunks/parent.chunk"] = StoredObject(data, {
                        "Checksum": str(zlib.crc32(self.data)), "Compression": codec})
                    with self.assertRaises(InvalidBackup):
                        self.restore()
                    self.assert_no_partial_output()

    def test_wrong_block_size_trailing_data_unknown_codec_and_short_raw_fail(self):
        for codec, data in (("lz4_block", struct.pack("<Q", 1) + b"data"),
                            ("zstd", struct.pack("<Q", 1) + b"data"),
                            ("gzip", gzip.compress(self.data) + b"trailing"),
                            ("gzip", gzip.compress(self.data[:-1])),
                            ("gzip", gzip.compress(self.data + b"x")),
                            ("unknown", self.data), ("", self.data[:-1])):
            with self.subTest(codec=codec, length=len(data)):
                self.objects["chunks/parent.chunk"] = StoredObject(data, {
                    "Checksum": str(zlib.crc32(self.data)), "Compression": codec})
                with self.assertRaises(InvalidBackup):
                    self.restore()
                self.assert_no_partial_output()

    def encrypt_objects(self, keys=None):
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM

        keys = keys or {"kek-v1": bytes(range(32))}
        # The fixture follows Go: nonce || AES-GCM ciphertext || 16-byte tag.
        for index, (path, obj) in enumerate(list(self.objects.items())):
            key_id = list(keys)[index % len(keys)]
            kek = keys[key_id]
            dek = bytes([index + 1]) * 32
            dek_iv = bytes([index + 1]) * 12
            iv = bytes([index + 11]) * 12
            wrapped = dek_iv + AESGCM(kek).encrypt(dek_iv, dek, key_id.encode())
            encrypted = iv + AESGCM(dek).encrypt(iv, obj.data, path.encode())
            self.objects[path] = StoredObject(encrypted, dict(obj.metadata, **{
                "Key-Id": key_id, "Encrypted-Dek": base64.b64encode(wrapped).decode()}))
        return keys

    def test_encrypted_objects_and_multiple_historical_keys(self):
        keys = self.encrypt_objects({"kek-v1": bytes(range(32)), "kek-v2": bytes(range(32, 64))})
        report, _ = self.restore(reader_options={"keys": keys, "require_encryption": True})
        self.assertEqual(report.encryption_key_ids, ("kek-v1", "kek-v2"))
        self.assertEqual(self.destination.read_bytes(), self.reference)

    def test_no_key_is_auth_error_wrong_key_or_tamper_is_invalid(self):
        keys = self.encrypt_objects()
        with self.assertRaises(AuthError):
            self.restore()
        with self.assertRaises(InvalidBackup):
            self.restore(reader_options={"keys": {"kek-v1": b"x" * 32}})
        obj = self.objects["chunks/parent.chunk"]
        self.objects["chunks/parent.chunk"] = StoredObject(obj.data[:-1] + bytes([obj.data[-1] ^ 1]), obj.metadata)
        with self.assertRaises(InvalidBackup):
            self.restore(reader_options={"keys": keys})
        self.assert_no_partial_output()

    def test_key_probes_reject_missing_and_wrong_keys_without_changing_real_keys(self):
        keys = self.encrypt_objects()
        reader = BackupReader(MemoryStore(self.objects), keys, require_encryption=True)
        expected = hashlib.sha256(self.reference).hexdigest()
        reader.restore("snapshot", "disk", self.size, self.destination,
                       deadline=time.monotonic() + 30, expected_sha256=expected)
        self.destination.unlink()
        for wrong_key in (False, True):
            reader.check_key_rejection("snapshot", "disk", self.size, self.destination,
                                       deadline=time.monotonic() + 30,
                                       expected_sha256=expected, wrong_key=wrong_key)
            self.assert_no_partial_output()
            self.assertEqual(reader.keys, keys)
        reader.restore("snapshot", "disk", self.size, self.destination,
                       deadline=time.monotonic() + 30, expected_sha256=expected)
        self.assertEqual(self.destination.read_bytes(), self.reference)

    def test_key_probes_do_not_accept_network_auth_missing_object_or_format_errors(self):
        keys = self.encrypt_objects()
        store = MemoryStore(self.objects)
        reader = BackupReader(store, keys, require_encryption=True)
        for wrong_key in (False, True):
            for error in (TransportError("offline"), AuthError("forbidden"),
                          InvalidBackup("corrupt metadata"), ObjectNotFound("missing map")):
                with self.subTest(wrong_key=wrong_key, error=type(error).__name__):
                    expected = PendingBackup if isinstance(error, ObjectNotFound) else type(error)
                    with mock.patch.object(store, "get", side_effect=error), self.assertRaises(expected):
                        reader.check_key_rejection("snapshot", "disk", self.size, self.destination,
                                                   deadline=time.monotonic() + 30,
                                                   expected_sha256=hashlib.sha256(self.reference).hexdigest(),
                                                   wrong_key=wrong_key)
                    self.assert_no_partial_output()

    def test_key_probe_unexpected_success_is_a_failure(self):
        reader = BackupReader(MemoryStore({}), {"kek": b"x" * 32}, require_encryption=True)
        for wrong_key in (False, True):
            with mock.patch.object(BackupReader, "restore"), self.assertRaisesRegex(InvalidBackup, "unexpectedly"):
                reader.check_key_rejection("snapshot", "disk", self.size, self.destination,
                                           deadline=time.monotonic() + 30,
                                           expected_sha256="a" * 64, wrong_key=wrong_key)

    def test_key_probes_reject_unencrypted_backup_instead_of_counting_it_as_key_failure(self):
        reader = BackupReader(MemoryStore(self.objects), {"key": b"x" * 32}, require_encryption=True)
        for wrong_key in (False, True):
            with self.assertRaisesRegex(InvalidBackup, "unencrypted"):
                reader.check_key_rejection("snapshot", "disk", self.size, self.destination,
                                           deadline=time.monotonic() + 30,
                                           expected_sha256="a" * 64, wrong_key=wrong_key)

    def test_object_cannot_be_moved_to_another_path_due_to_aad(self):
        keys = self.encrypt_objects()
        self.objects["chunk_maps/snapshot"] = self.objects["snapshots/disk/snapshot/meta.json"]
        with self.assertRaises(InvalidBackup):
            self.restore(reader_options={"keys": keys})

    def test_missing_encryption_metadata_and_encryption_downgrade_fail(self):
        with self.assertRaises(InvalidBackup):
            self.restore(reader_options={"require_encryption": True})
        keys = self.encrypt_objects()
        obj = self.objects["chunk_maps/snapshot"]
        self.objects["chunk_maps/snapshot"] = StoredObject(obj.data, {"Key-Id": "kek-v1"})
        with self.assertRaises(InvalidBackup):
            self.restore(reader_options={"keys": keys})

    def test_raw_32_byte_kek_whitespace_and_editor_newline(self):
        key = b" " + b"x" * 30 + b"\n"
        keys = self.encrypt_objects({"key": key})
        self.restore(reader_options={"keys": keys})
        self.destination.unlink()
        # A non-whitespace raw key with an editor newline follows Go TrimSpace.
        self.assertEqual(BackupReader(MemoryStore({}), {"key": b"x" * 32 + b"\n"}).keys["key"], b"x" * 32)
        with self.assertRaises(AuthError):
            BackupReader(MemoryStore({}), {"key": b"x" * 44})


if __name__ == "__main__":
    unittest.main()
