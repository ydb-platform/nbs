import json
import os
import yatest.common as yatest_common

from subprocess import run


TOOLS_PATH = 'cloud/blockstore/tools/fs'
GEN_TOOL_NAME = 'gen_used_blocks_map'
GEN_TOOL_PATH = yatest_common.binary_path(f"{TOOLS_PATH}/{GEN_TOOL_NAME}/{GEN_TOOL_NAME}")
DATA_PATH = yatest_common.source_path(f"{TOOLS_PATH}/{GEN_TOOL_NAME}/tests/data")


def gen_used_blocks_map(meta):

    meta_file = os.path.join(yatest_common.output_path(), 'meta.json')
    with open(meta_file, 'w') as f:
        json.dump(meta, f, indent=4)

    results_path = os.path.join(yatest_common.output_path(), "results.json")
    with open(results_path, 'w') as f:
        run([
            GEN_TOOL_PATH,
            "--meta-file", meta_file,
            "--format", "json",
            "-vvv"
        ], check=True, text=True, stdout=f)

    return [yatest_common.canonical_file(results_path)]


def test_parts():
    meta = {
        "block_size": 4096,
        "size": 99857989632,
        "sector_size": 512,
        "partitions": {
            "label": "gpt",
            "device": "/dev/nbd0",
            "unit": "sectors",
            "firstlba": 2048,
            "lastlba": 195035102,
            "partitions": [{
                "node": "/dev/nbd0p1",
                "start": 2048,
                "size": 83886080,
                "fs": {
                    "name": "ext4",
                    "dumpe2fs": os.path.join(DATA_PATH, "p1.ext4.txt")
                }
            }, {
                "node": "/dev/nbd0p2",
                "start": 86190080,
                "size": 104857600,
                "fs": {
                    "name": "xfs",
                    "sb": os.path.join(DATA_PATH, "p2.xfs.sb.txt"),
                    "freesp": os.path.join(DATA_PATH, "p2.xfs.freesp.txt")
                }
            }],
            "unpartitioned": [
                [83888128, 86190080],
                [191047680, 195035103]
            ]
        }
    }

    return gen_used_blocks_map(meta)


def test_no_parts():
    meta = {
        "block_size": 4096,
        "size": 99857989632,
        "sector_size": 512,
        "fs": {
            "name": "ext4",
            "dumpe2fs": os.path.join(DATA_PATH, "p1.ext4.txt")
        }
    }

    return gen_used_blocks_map(meta)


def test_no_fs():
    meta = {
        "block_size": 4096,
        "size": 99857989632,
        "sector_size": 512
    }

    return gen_used_blocks_map(meta)


def test_parts_8K():
    meta = {
        "block_size": 8192,
        "size": 99857989632,
        "sector_size": 512,
        "partitions": {
            "label": "gpt",
            "device": "/dev/nbd0",
            "unit": "sectors",
            "firstlba": 2048,
            "lastlba": 195035102,
            "partitions": [{
                "node": "/dev/nbd0p1",
                "start": 2048,
                "size": 83886080,
            }, {
                "node": "/dev/nbd0p2",
                "start": 86190080,
                "size": 104857600,
            }],
            "unpartitioned": [
                [83888128, 86190080],
                [191047680, 195035103]
            ]
        }
    }

    return gen_used_blocks_map(meta)


def test_ntfs_parts():
    meta = {
        "block_size": 4096,
        "size": 99857989632,
        "sector_size": 512,
        "partitions": {
            "label": "gpt",
            "device": "/dev/vdb",
            "unit": "sectors",
            "firstlba": 34,
            "lastlba": 195035102,
            "partitions": [
                {
                    "node": "/dev/vdb1",
                    "start": 34,
                    "size": 32734,
                    "fs": None
                },
                {
                    "node": "/dev/vdb2",
                    "start": 32768,
                    "size": 102400000,
                    "fs": {
                        "name": "ntfs",
                        "ntfswipe": os.path.join(DATA_PATH, "p2.ntfs.txt")
                    }
                },
                {
                    "node": "/dev/vdb3",
                    "start": 102432768,
                    "size": 92598272,
                    "fs": {
                        "name": "ntfs",
                        "ntfswipe": os.path.join(DATA_PATH, "p3.ntfs.txt")
                    }
                }
            ],
            "unpartitioned": [[195031040, 195035103]]
        }
    }

    return gen_used_blocks_map(meta)
