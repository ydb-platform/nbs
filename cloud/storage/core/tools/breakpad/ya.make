PY2_LIBRARY()

PEERDIR(
    library/python/retry
)

IF (NOT OPENSOURCE)
    PEERDIR(
        contrib/python/requests     # TODO: NBS-4453
    )
ENDIF()

PY_SRCS(
    command.py
    conductor.py
    core_checker.py
    coredump.py
    coredump_formatter.py
    crash_info.py
    crash_processor.py
    error_collector.py
    launcher.py
    limiter.py
    oom_checker.py
    sender.py
)

END()

RECURSE(
    launcher
    sender
)
