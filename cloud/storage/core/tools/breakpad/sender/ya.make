PY3_PROGRAM(yc-storage-breakpad-sender)

PEERDIR(
    cloud/storage/core/tools/breakpad/common

    contrib/python/requests/py3
    library/python/retry
)

PY_SRCS(
    command.py
    conductor.py
    coredump_formatter.py
    coredump.py
    crash_processor.py
    limiter.py
    sender.py
    sentry.py
)

PY_MAIN(cloud.storage.core.tools.breakpad.sender.crash_processor:main)

END()
