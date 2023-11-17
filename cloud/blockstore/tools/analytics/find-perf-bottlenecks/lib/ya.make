PY23_LIBRARY()

OWNER(g:cloud-nbs)

PY_SRCS(
    __init__.py
    report.py
    trace.py
)

PEERDIR(
    contrib/python/lxml
)

END()
