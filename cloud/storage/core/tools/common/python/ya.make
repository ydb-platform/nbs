PY3_LIBRARY()

PY_SRCS(
    core_pattern.py
    daemon.py
    port_reservation.py
)

PEERDIR(
    contrib/python/requests/py3

    library/python/filelock
    library/python/testing/yatest_common
)

END()

RECURSE_FOR_TESTS(
    ut
)
