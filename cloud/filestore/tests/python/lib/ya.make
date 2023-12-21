PY3_LIBRARY()

PEERDIR(
    cloud/filestore/config

    cloud/filestore/public/sdk/python/client
    cloud/filestore/public/sdk/python/protos

    cloud/storage/core/protos

    contrib/python/requests/py3
    contrib/python/retrying

    ydb/tests/library
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
