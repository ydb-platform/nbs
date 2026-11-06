from . common import file_crc32
from yatest.common import process


class RawImageGenerator():

    def __init__(self, image_file_path, chunk_size, chunks_count):
        self.__image_file_path = image_file_path
        self.__chunk_size = chunk_size
        self.__chunks_count = chunks_count

        assert chunk_size > 0, "chunk size should be positive"
        assert chunks_count > 1, "chunks count should be greater than 1"

    def generate(self):
        # Write some zero bytes in the beginning of the file
        # to ensure that image format will be treated as raw.
        process.execute([
            "dd",
            "if=/dev/zero",
            "of={}".format(self.__image_file_path),
            "bs={}".format(self.__chunk_size),
            "count={}".format(1),
        ])

        # Fill image with random bytes.
        process.execute([
            "dd",
            "if=/dev/urandom",
            "of={}".format(self.__image_file_path),
            "bs={}".format(self.__chunk_size),
            "count={}".format(self.__chunks_count - 1),
            "seek={}".format(1),
        ])

    @property
    def image_crc32(self):
        return file_crc32(self.__image_file_path, self.__chunk_size)
