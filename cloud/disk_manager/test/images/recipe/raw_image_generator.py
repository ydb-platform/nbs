import binascii

from yatest.common import process


class RawImageGenerator():

    def __init__(self, image_file_path, image_size):
        self.__image_size = image_size
        self.__image_file_path = image_file_path

        self.__batch_size = 1024 * 1024
        if image_size % self.__batch_size != 0:
            raise Exception("image size should be divisible by {}, image size = {}".format(self.__batch_size, image_size))

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
        crc32 = 0
        with open(self.__image_file_path, "rb") as f:
            for _ in range(0, self.__image_size, self.__batch_size):
                batch = f.read(self.__batch_size)
                crc32 = binascii.crc32(batch, crc32)
        return crc32
