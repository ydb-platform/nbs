PY3_LIBRARY()

PEERDIR(
    cloud/filestore/config

    cloud/filestore/public/sdk/python/client
    cloud/filestore/public/sdk/python/protos

    cloud/storage/core/protos
    cloud/storage/core/tools/testing/access_service
    cloud/storage/core/tools/testing/access_service_new

    contrib/python/requests/py3
    contrib/python/retrying

    contrib/ydb/tests/library
)

PY_SRCS(
    client.py
    common.py
    daemon_config.py
    endpoint.py
    http_proxy.py
    kikimr.py
    loadtest.py
    server.py
    vhost.py
)

END()
