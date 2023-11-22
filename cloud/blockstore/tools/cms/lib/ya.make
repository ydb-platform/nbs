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
    contrib/ydb/core/protos
    contrib/ydb/public/api/protos
)

IF (NOT OPENSOURCE)
    PEERDIR(
        contrib/python/requests     # TODO: NBS-4453
    )
ENDIF()

END()
