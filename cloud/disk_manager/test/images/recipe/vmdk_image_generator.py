import random

from . common import file_crc32
from yatest.common import process


class VMDKImageGenerator():

    def __init__(self, raw_image_file_path, vmdk_image_file_path):
        self.__raw_image_file_path = raw_image_file_path
        self.__vmdk_image_file_path = vmdk_image_file_path

        self.__chunk_size = 4*1024*1024
        self.__raw_image_chunks = random.randrange(8, 32)
        self.__raw_image_size = self.__chunk_size * self.__raw_image_chunks

    def generate(self):
        process.execute([
            "dd",
            "if=/dev/urandom",
            "of={}".format(self.__raw_image_file_path),
            "bs={}".format(self.__chunk_size),
            "count={}".format(self.__raw_image_chunks),
        ])

        holes = random.randrange(0, 10)
        for i in range(holes):
            hole_size = random.randrange(200*1024, 8*1024*1024)
            max_holes = int(self.__raw_image_size / hole_size)
            seek = random.randrange(0, max_holes - 1)
            process.execute([
                "dd",
                "if=/dev/zero",
                "of={}".format(self.__raw_image_file_path),
                "bs={}".format(hole_size),
                "count={}".format(1),
                "seek={}".format(seek),
                "conv=notrunc",
            ])

        process.execute([
            "qemu-img",
            "convert",
            "-f", "raw",
            "-O", "vmdk",
            self.__raw_image_file_path,
            self.__vmdk_image_file_path,
        ])

    @property
    def raw_image_size(self):
        return self.__raw_image_size

    @property
    def raw_image_crc32(self):
        return file_crc32(self.__raw_image_file_path, self.__chunk_size)
