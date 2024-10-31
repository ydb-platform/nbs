PY23_LIBRARY()

PY_SRCS(
    harness/__init__.py
    harness/daemon.py
    harness/kikimr_cluster.py
    harness/kikimr_config.py
    harness/kikimr_runner.py
    harness/util.py
)

PEERDIR(
    contrib/ydb/tests/library
)

END()