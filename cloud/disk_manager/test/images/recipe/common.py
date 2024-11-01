import binascii
import os


def file_crc32(file_path, batch_size):
    crc32 = 0
    file_size = os.path.getsize(file_path)
    with open(file_path, "rb") as f:
        for _ in range(0, file_size, batch_size):
            batch = f.read(batch_size)
            crc32 = binascii.crc32(batch, crc32)
    return crc32
