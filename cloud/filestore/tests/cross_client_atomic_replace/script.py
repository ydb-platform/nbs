import collections
import json
import os
import sys
import tempfile
import time

TARGET = "value.json"
READER_READY = "reader-ready"
WRITER_DONE = "writer-done"
PUBLISH_COUNT = 150
PUBLISH_PERIOD_SECONDS = 0.1
READ_PERIOD_SECONDS = 0.005
SETUP_TIMEOUT_SECONDS = 60


def read(path):
    try:
        with open(path) as f:
            return json.load(f)
    except OSError as e:
        return {"errno": e.errno}


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
        if "errno" in result:
            errors[str(result["errno"])] += 1
        time.sleep(READ_PERIOD_SECONDS)
    print(json.dumps({"samples": samples, "errors": errors}))


if __name__ == "__main__":
    globals()[sys.argv[1]](sys.argv[2])
