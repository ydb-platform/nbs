PY3_LIBRARY(filestore-public-sdk-python-client)

PY_SRCS(
    __init__.py
    base_client.py
    client.py
    credentials.py
    durable.py
    endpoint.py
    error.py
    error_codes.py
    grpc_client.py
    request.py
)

PEERDIR(
    cloud/filestore/public/api/grpc
    cloud/filestore/public/sdk/python/protos

    contrib/python/requests/py3
)

END()

RECURSE_FOR_TESTS(
    ut
)
