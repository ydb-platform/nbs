PY3_LIBRARY()

PEERDIR(
    cloud/blockstore/config

    cloud/blockstore/public/sdk/python/client
    cloud/blockstore/public/sdk/python/protos

    cloud/storage/core/tools/common/python

    ydb/core/protos
    ydb/public/api/protos
    ydb/tests/library

    contrib/python/requests
    contrib/python/retrying
)

PY_SRCS(
    __init__.py
    access_service.py
    disk_agent_runner.py
    endpoints.py
    loadtest_env.py
    nbs_http_proxy.py
    nbs_runner.py
    nonreplicated_setup.py
    stats.py
    test_base.py
    test_with_plugin.py
)

END()
