PY3_PROGRAM(yc-storage-breakpad-sender)

PEERDIR(
    cloud/storage/core/tools/breakpad/common

    contrib/python/requests/py3
    library/python/retry
)

PY_SRCS(
    senders/base.py
    senders/cores.py
    senders/email.py
    senders/multi.py
    senders/sentry.py
    conductor.py
    coredump_formatter.py
    coredump.py
    crash_processor.py
    limiter.py
    sender.py
)

PY_MAIN(cloud.storage.core.tools.breakpad.sender.sender:main)

END()
