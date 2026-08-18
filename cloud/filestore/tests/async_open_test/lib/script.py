import os
import sys
import time


FILE_CONTENT = b"x"
DROP_PAGE_CACHE_AND_SLABS = 3


def drop_caches():
    os.sync()
    with open("/proc/sys/vm/drop_caches", "w") as f:
        f.write(f"{DROP_PAGE_CACHE_AND_SLABS}\n")


def prepare_files(count):
    for i in range(count):
        with open(f"async_open_test_{i}", "wb") as f:
            f.write(FILE_CONTENT)
    os.sync()


def hold_files(
        count,
        ready_path,
        release_path,
        verify_path=None,
        verified_path=None,
        unlink_path=None,
        unlinked_path=None):
    drop_caches()

    fds = []
    for i in range(count):
        fd = os.open(f"async_open_test_{i}", os.O_RDONLY)
        assert os.read(fd, len(FILE_CONTENT)) == FILE_CONTENT
        fds.append(fd)

    with open(ready_path, "w") as f:
        f.write("ready")

    if unlink_path:
        while not os.path.exists(unlink_path):
            time.sleep(0.1)

        for i in range(count):
            os.unlink(f"async_open_test_{i}")

        with open(unlinked_path, "w") as f:
            f.write("unlinked")

    if verify_path:
        while not os.path.exists(verify_path):
            time.sleep(0.1)

        # otherwise the reads below can be served by the guest page cache
        # without ever using the restored handles
        drop_caches()

        for fd in fds:
            os.lseek(fd, 0, os.SEEK_SET)
            assert os.read(fd, len(FILE_CONTENT)) == FILE_CONTENT

        with open(verified_path, "w") as f:
            f.write("verified")

    while not os.path.exists(release_path):
        time.sleep(0.1)

    for fd in fds:
        os.close(fd)


def main():
    mode = sys.argv[1]
    count = int(sys.argv[2])
    if mode == "prepare":
        prepare_files(count)
    elif mode == "hold":
        hold_files(
            count,
            sys.argv[3],
            sys.argv[4],
            *sys.argv[5:])
    else:
        raise ValueError(f"unknown mode: {mode}")


if __name__ == "__main__":
    main()
