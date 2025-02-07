PY3_LIBRARY()

PEERDIR(
    cloud/blockstore/public/api/grpc
    cloud/blockstore/public/api/protos
    cloud/blockstore/public/sdk/python/protos
    contrib/python/requests/py3
)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/opensource.inc)

PY_SRCS(
    __init__.py
    base_client.py
    safe_client.py
    client.py
    credentials.py
    durable.py
    discovery.py
    error.py
    error_codes.py
    future.py
    grpc_client.py
    http_client.py
    request.py
    scheduler.py
    session.py
)

END()

RECURSE_FOR_TESTS(
    ut
)
