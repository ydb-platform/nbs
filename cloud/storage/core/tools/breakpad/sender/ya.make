PY3_PROGRAM(yc-storage-breakpad-sender)

PEERDIR(
    cloud/storage/core/tools/breakpad/common
    cloud/storage/core/tools/breakpad/sender/senders

    contrib/python/requests/py3
    library/python/retry
)

PY_SRCS(
    conductor.py
    coredump_formatter.py
    coredump.py
    crash_processor.py
    limiter.py
    sender.py
)

PY_MAIN(cloud.storage.core.tools.breakpad.sender.sender:main)

END()
