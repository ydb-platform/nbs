import collections
import errno
import json
import os
import stat
import sys
import tempfile
import time

TARGET = "value.json"
READER_READY = "reader-ready"
WRITER_DONE = "writer-done"
TOGGLER_READY = "toggler-ready"
CREATOR_DONE = "creator-done"
PUBLISH_COUNT = 200
CREATE_COUNT = 500
PUBLISH_PERIOD_SECONDS = 0.01
READ_PERIOD_SECONDS = 0.005
SETUP_TIMEOUT_SECONDS = 60


def read(path):
    try:
        with open(path) as f:
            return json.load(f)
    except OSError as e:
        return {"error": errno.errorcode[e.errno]}


def touch(path):
    open(path, "w").close()


def wait_for(path):
    deadline = time.monotonic() + SETUP_TIMEOUT_SECONDS
    while not os.path.exists(path):
        assert time.monotonic() < deadline, f"{path} did not appear"
        time.sleep(0.1)


def publish(root, seq):
    fd, tmp = tempfile.mkstemp(dir=root)
    with os.fdopen(fd, "w") as f:
        json.dump({"seq": seq}, f)
    os.replace(tmp, os.path.join(root, TARGET))


def writer(root):
    os.makedirs(root, exist_ok=True)
    publish(root, 0)
    wait_for(os.path.join(root, READER_READY))
    for seq in range(1, PUBLISH_COUNT + 1):
        publish(root, seq)
        time.sleep(PUBLISH_PERIOD_SECONDS)
    touch(os.path.join(root, WRITER_DONE))


def reader(root):
    wait_for(os.path.join(root, TARGET))
    touch(os.path.join(root, READER_READY))
    samples = 0
    errors = collections.Counter()
    while not os.path.exists(os.path.join(root, WRITER_DONE)):
        result = read(os.path.join(root, TARGET))
        samples += 1
        if "error" in result:
            errors[result["error"]] += 1
        time.sleep(READ_PERIOD_SECONDS)
    print(json.dumps({"samples": samples, "errors": errors}))


def toggler(root):
    os.makedirs(root, exist_ok=True)
    touch(os.path.join(root, TOGGLER_READY))
    target = os.path.join(root, TARGET)
    # mknod and unlink are single-phase leader operations, so the name
    # flips every few ms while the creator's shard phase is delayed
    while not os.path.exists(os.path.join(root, CREATOR_DONE)):
        try:
            os.mknod(target, stat.S_IFREG | 0o644)
        except FileExistsError:
            pass
        try:
            os.unlink(target)
        except FileNotFoundError:
            pass


def creator(root):
    wait_for(os.path.join(root, TOGGLER_READY))
    target = os.path.join(root, TARGET)
    created = 0
    errors = collections.Counter()
    for seq in range(CREATE_COUNT):
        try:
            fd = os.open(target, os.O_WRONLY | os.O_CREAT, 0o644)
        except OSError as e:
            errors[errno.errorcode[e.errno]] += 1
            continue
        try:
            os.write(fd, json.dumps({"seq": seq}).encode())
            created += 1
        except OSError as e:
            errors[errno.errorcode[e.errno]] += 1
        finally:
            os.close(fd)
        # a negative dentry is never trusted by the kernel, so the next open
        # goes through LOOKUP + CREATE instead of OPEN by the cached ino
        try:
            os.unlink(target)
        except FileNotFoundError:
            pass
    touch(os.path.join(root, CREATOR_DONE))
    print(json.dumps({"created": created, "errors": errors}))


if __name__ == "__main__":
    globals()[sys.argv[1]](sys.argv[2])
