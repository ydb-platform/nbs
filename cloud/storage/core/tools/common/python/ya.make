PY3_LIBRARY()

PY_SRCS(
    core_pattern.py
    daemon.py
)

PEERDIR(
    contrib/python/requests
)

END()
