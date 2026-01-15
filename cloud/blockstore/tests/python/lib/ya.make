PY3_LIBRARY()

PEERDIR(
    cloud/blockstore/config

    cloud/blockstore/public/sdk/python/client
    cloud/blockstore/public/sdk/python/protos

    cloud/storage/core/tools/common/python
    cloud/storage/core/tools/testing/access_service/lib
    cloud/storage/core/tools/testing/access_service_new/lib
    cloud/storage/core/tools/testing/qemu/lib

    contrib/ydb/core/protos
    contrib/ydb/public/api/protos
    contrib/ydb/tests/library

    contrib/python/requests/py3
    contrib/python/retrying
)

PY_SRCS(
    __init__.py
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
    test_client.py
    test_with_plugin.py
)

END()
