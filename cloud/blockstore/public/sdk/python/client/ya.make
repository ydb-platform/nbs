PY23_LIBRARY()

PEERDIR(
    cloud/blockstore/public/api/grpc
    cloud/blockstore/public/sdk/python/protos
)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/opensource.inc)

IF (OPENSOURCE)
    PEERDIR(contrib/python/requests/py3)
ELSE()
    PEERDIR(contrib/python/requests)
ENDIF()

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
    ut_py23
)
