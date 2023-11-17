PY3_LIBRARY()

PEERDIR(
    cloud/blockstore/public/sdk/python/client

    library/python/resource
)

RESOURCE(
    response_for_tests/describe_volume.json describe_volume.json
    response_for_tests/stat_volume.json stat_volume.json
)

PY_SRCS(
    client.py
    __init__.py
)

END()
