PY23_LIBRARY()

PY_SRCS(
    __init__.py
    report.py
    trace.py
)

PEERDIR(
    contrib/python/lxml
)

END()
