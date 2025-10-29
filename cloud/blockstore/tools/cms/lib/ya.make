PY3_LIBRARY()

PY_SRCS(
    cms.py
    conductor.py
    config_main.py
    config.py
    patcher_main.py
    proto.py
    pssh.py
    tools.py
)

PEERDIR(
    cloud/blockstore/config
    contrib/python/jsondiff
    contrib/python/requests/py3
    ydb/core/protos
    ydb/public/api/protos
)

END()
