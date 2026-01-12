import itertools
import logging
import os
import random
import string
import time
from enum import Enum

import pytest
import yatest.common as common
from cloud.filestore.public.sdk.python.client import Client, CreateClient
from cloud.filestore.public.sdk.python.client.error import ClientError

BLOCK_SIZE = 4096
BLOCKS_COUNT = 1000

ROOT_NODE_ID = 1
INVALID_HANDLE_ID = 0


class Operation(Enum):
    READ = "read"
    WRITE = "write"


def validate_read_data(read_data, target_data, length, offset, output_path):
    """Validate that read data matches target data and dump files on mismatch."""
    if read_data != target_data:
        expected_file = os.path.join(
            output_path,
            f"expected_{length}_{offset}.bin",
        )
        actual_file = os.path.join(
            output_path,
            f"actual_{length}_{offset}.bin",
        )

        with open(expected_file, "wb") as f:
            f.write(target_data)
        with open(actual_file, "wb") as f:
            f.write(read_data)

        raise AssertionError(
            f"Data mismatch for size {length} and offset {offset}. "
            f"Expected data written to {expected_file}, "
            f"actual data written to {actual_file}"
        )


def setup_test_filestore(nfs_client, fs_name, shard_count=None):
    """Create a filestore and session with a test file node."""
    nfs_client.create_filestore(
        fs_name,
        "project",
        "folder",
        "cloud",
        BLOCK_SIZE,
        BLOCKS_COUNT,
        shard_count=shard_count,
    )

    session_id = nfs_client.create_session(fs_name).Session.SessionId.encode(
        "utf-8"
    )

    node = nfs_client.create_node(
        fs_name,
        session_id,
        Client.File(0o644),
        ROOT_NODE_ID,
        "test_file",
    ).Node

    return session_id, node


def create_mmap_region(nfs_client, filename, size) -> (int, str):
    """Create a file and mmap it, returning the region ID and file path."""
    base_path = os.path.join(common.output_path(), "shm")
    file_path = os.path.join(base_path, filename)

    with open(file_path, "wb") as f:
        f.truncate(size)

    response = nfs_client.mmap(file_path=filename, size=size)
    return response.Id, file_path


def test_mmap_unmmap():
    logger = logging.getLogger("test")
    port = os.getenv("NFS_SERVER_PORT")

    with CreateClient(f"localhost:{port}", log=logger) as nfs_client:
        region_id, file_path = create_mmap_region(nfs_client, "testfile", 4096)
        logger.info(f"Successfully mmaped file with region ID: {region_id}")

        regions = nfs_client.list_mmap_regions()
        assert len(regions.Regions) == 1
        assert regions.Regions[0].Id == region_id
        assert regions.Regions[0].Size == 4096
        assert regions.Regions[0].FilePath == file_path
        assert regions.Regions[0].LatestActivityTimestamp > 0

        with pytest.raises(ClientError, match="Mmap region not found"):
            nfs_client.munmap(mmap_id=region_id + 1)

        nfs_client.munmap(mmap_id=region_id)
        logger.info(f"Successfully unmapped region: {region_id}")

        regions = nfs_client.list_mmap_regions()
        assert len(regions.Regions) == 0

        with pytest.raises(ClientError, match="No such file or directory"):
            nfs_client.mmap(file_path="nonexistent", size=4096)


def test_ping_region():
    logger = logging.getLogger("test")
    port = os.getenv("NFS_SERVER_PORT")

    with CreateClient(f"localhost:{port}", log=logger) as nfs_client:
        region_id, _ = create_mmap_region(
            nfs_client, "testfile_ping", 4096
        )
        logger.info(f"Created mmap region with ID: {region_id}")

        old_timestamp = (
            nfs_client.list_mmap_regions().Regions[0].LatestActivityTimestamp
        )
        time.sleep(2)  # Ensure timestamp difference
        nfs_client.ping_mmap_region(mmap_id=region_id)

        new_timestamp = (
            nfs_client.list_mmap_regions().Regions[0].LatestActivityTimestamp
        )
        assert new_timestamp > old_timestamp


SHARED_MEMORY_SIZE = 10 * 1024 * 1024  # 10 MB


@pytest.mark.parametrize("shard_count", [None, 10])
def test_memory_mapped_io(shard_count):
    logger = logging.getLogger("test")
    port = os.getenv("NFS_SERVER_PORT")

    with CreateClient(f"localhost:{port}", log=logger) as nfs_client:
        region_id, file_path = create_mmap_region(
            nfs_client, "testfile", SHARED_MEMORY_SIZE
        )
        logger.info(f"Created mmap region with ID: {region_id}")

        session_id, node = setup_test_filestore(nfs_client, "fs", shard_count)
        logger.info(f"Created session: {session_id}, node: {node}")

        def read_from_shared_memory(offset, length):
            with open(file_path, "rb") as f:
                f.seek(offset)
                return f.read(length)

        def write_to_shared_memory(offset, data):
            with open(file_path, "r+b") as f:
                f.seek(offset)
                f.write(data)

        seed = int(time.time())
        logger.info(f"Seeding random with: {seed}")
        random.seed(seed)

        LENGTHS = [
            BLOCK_SIZE - 1,
            BLOCK_SIZE,
            BLOCK_SIZE + 1,
            2 * BLOCK_SIZE - 1,
            2 * BLOCK_SIZE,
            2 * BLOCK_SIZE + 1,
            4 * 1024,
            16 * 1024,
            64 * 1024,
            256 * 1024,
            1024 * 1024,
            random.randint(1, 1024 * 1024),
        ]
        OFFSETS = [
            0,
            BLOCK_SIZE - 1,
            BLOCK_SIZE,
            BLOCK_SIZE + 1,
            random.randint(0, 1024 * 1024),
        ]
        OPERATIONS = [
            Operation.READ,
            Operation.WRITE,
        ]

        target_file_content = bytearray()

        ops = list(itertools.product(LENGTHS, OFFSETS, OPERATIONS))
        random.shuffle(ops)
        for length, offset, operation in ops:
            if operation == Operation.READ:
                io_vecs = [
                    (random.randint(0, SHARED_MEMORY_SIZE - length), length)
                ]

                read_data_size = nfs_client.read_data(
                    "fs",
                    session_id,
                    INVALID_HANDLE_ID,
                    offset,
                    length,
                    node_id=node.Id,
                    io_vecs=io_vecs,
                    region_id=region_id,
                ).Length

                target_data = target_file_content[
                    offset : offset + length
                ].ljust(length, b"\x00")

                read_data = read_from_shared_memory(
                    io_vecs[0][0], read_data_size
                )
                # pad with zeros if read less than requested
                read_data = read_data.ljust(length, b"\x00")

                validate_read_data(
                    read_data, target_data, length, offset, common.output_path()
                )
                logger.info(
                    f"Data verified successfully for size {length} at offset {offset}"
                )

            if operation == Operation.WRITE:
                logger.info(f"Writing {length} bytes at offset {offset}")
                data = "".join(
                    random.choices(string.ascii_letters, k=length)
                ).encode("utf-8")
                io_vecs = [
                    (random.randint(0, SHARED_MEMORY_SIZE - length), length)
                ]
                write_to_shared_memory(io_vecs[0][0], data)
                nfs_client.write_data(
                    "fs",
                    session_id,
                    INVALID_HANDLE_ID,
                    offset,
                    b"",
                    node_id=node.Id,
                    io_vecs=io_vecs,
                    region_id=region_id,
                )
                target_file_content = target_file_content.ljust(
                    offset + length, b"\x00"
                )
                target_file_content[offset : offset + length] = data

                logger.info(f"Wrote data of size: {len(data)}")


def test_invalid_region_id():
    logger = logging.getLogger("test")
    port = os.getenv("NFS_SERVER_PORT")

    with CreateClient(f"localhost:{port}", log=logger) as nfs_client:
        region_id, _ = create_mmap_region(
            nfs_client, "testfile_invalid", SHARED_MEMORY_SIZE
        )
        session_id, node = setup_test_filestore(nfs_client, "fs_invalid_region")

        invalid_region_id = region_id + 1
        test_length = 1024

        # Test read with invalid region ID
        with pytest.raises(ClientError):
            nfs_client.read_data(
                "fs_invalid_region",
                session_id,
                INVALID_HANDLE_ID,
                0,
                test_length,
                node_id=node.Id,
                io_vecs=[(0, test_length)],
                region_id=invalid_region_id,
            )

        # Test write with invalid region ID
        with pytest.raises(ClientError):
            nfs_client.write_data(
                "fs_invalid_region",
                session_id,
                INVALID_HANDLE_ID,
                0,
                b"",
                node_id=node.Id,
                io_vecs=[(0, test_length)],
                region_id=invalid_region_id,
            )

        nfs_client.munmap(mmap_id=region_id)


def test_iovec_bounds_validation():
    logger = logging.getLogger("test")
    port = os.getenv("NFS_SERVER_PORT")

    with CreateClient(f"localhost:{port}", log=logger) as nfs_client:
        region_id, _ = create_mmap_region(
            nfs_client, "testfile_bounds", SHARED_MEMORY_SIZE
        )
        session_id, node = setup_test_filestore(nfs_client, "fs_bounds")

        # Test cases for out-of-bounds iovecs
        test_cases = [
            # (offset, length, description)
            (SHARED_MEMORY_SIZE, 1, "offset at boundary"),
            (SHARED_MEMORY_SIZE - 1, 2, "length exceeds boundary"),
            (SHARED_MEMORY_SIZE + 1, 1024, "offset beyond boundary"),
            (SHARED_MEMORY_SIZE // 2, SHARED_MEMORY_SIZE, "large length"),
        ]

        for offset, length, description in test_cases:
            logger.info(f"Testing bounds violation: {description}")

            # Test read with out-of-bounds iovec
            with pytest.raises(ClientError, match="out of bounds"):
                nfs_client.read_data(
                    "fs_bounds",
                    session_id,
                    INVALID_HANDLE_ID,
                    0,
                    length,
                    node_id=node.Id,
                    io_vecs=[(offset, length)],
                    region_id=region_id,
                )

            # Test write with out-of-bounds iovec
            with pytest.raises(ClientError, match="out of bounds"):
                nfs_client.write_data(
                    "fs_bounds",
                    session_id,
                    INVALID_HANDLE_ID,
                    0,
                    b"",
                    node_id=node.Id,
                    io_vecs=[(offset, length)],
                    region_id=region_id,
                )

        # Test valid boundary case - should succeed
        valid_offset = SHARED_MEMORY_SIZE - 1024
        valid_length = 1024

        # This should not raise an error
        nfs_client.read_data(
            "fs_bounds",
            session_id,
            INVALID_HANDLE_ID,
            0,
            valid_length,
            node_id=node.Id,
            io_vecs=[(valid_offset, valid_length)],
            region_id=region_id,
        )

        logger.info("Valid boundary case succeeded as expected")

        nfs_client.munmap(mmap_id=region_id)
