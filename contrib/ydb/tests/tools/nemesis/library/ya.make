SUBSCRIBER(g:kikimr)

PY3_LIBRARY()

PY_SRCS(
    __init__.py
    base.py
    bridge_pile.py
    catalog.py
    datacenter.py
    disk.py
    host.py
    node.py
    tablet.py
    monitor.py
)

PEERDIR(
    contrib/python/Flask
    contrib/ydb/tests/library
    contrib/ydb/tests/library/clients
    library/python/monlib
    contrib/ydb/core/protos
)

END()
