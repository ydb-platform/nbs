from . common import file_crc32
from yatest.common import process


class RawImageGenerator():

    def __init__(self, image_file_path, image_size):
        self.__image_size = image_size
        self.__image_file_path = image_file_path

        self.__batch_size = 1024 * 1024

        assert image_size % self.__batch_size == 0, "image size should be divisible by {}, image size = {}".format(
            self.__batch_size, image_size,
        )
        assert image_size > self.__batch_size, "image size should be greater than {}, image size = {}".format(
            self.__batch_size, image_size,
        )

    def generate(self):
        # Write some zero bytes in the beginning of the file
        # to ensure that image format will be treated as raw.
        process.execute([
            "dd",
            "if=/dev/zero",
            "of={}".format(self.__image_file_path),
            "bs={}".format(self.__batch_size),
            "count={}".format(1),
        ])

        # Fill image with random bytes.
        batch_count = self.__image_size // self.__batch_size - 1
        process.execute([
            "dd",
            "if=/dev/urandom",
            "of={}".format(self.__image_file_path),
            "bs={}".format(self.__batch_size),
            "count={}".format(batch_count),
            "seek={}".format(1),
        ])

    @property
    def image_crc32(self):
        return file_crc32(self.__image_file_path, self.__batch_size)
