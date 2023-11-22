PY3_LIBRARY()

PEERDIR(
    cloud/blockstore/config

    cloud/blockstore/public/sdk/python/client
    cloud/blockstore/public/sdk/python/protos

    cloud/storage/core/tools/common/python
    cloud/storage/core/tools/testing/qemu/lib

    contrib/ydb/core/protos
    contrib/ydb/public/api/protos
    contrib/ydb/tests/library

    contrib/python/retrying
)

IF (NOT OPENSOURCE)
    PEERDIR(
        contrib/python/requests     # TODO: NBS-4453
    )
ENDIF()

PY_SRCS(
    __init__.py
    access_service.py
    client.py
    config.py
    daemon.py
    disk_agent_runner.py
    endpoints.py
    loadtest_env.py
    nbs_http_proxy.py
    nbs_runner.py
    nonreplicated_setup.py
    rdma.py
    stats.py
    test_base.py
    test_with_plugin.py
)

END()
