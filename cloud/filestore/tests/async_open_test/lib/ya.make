PY3_LIBRARY()

PY_SRCS(
    __init__.py
    script.py
)

PEERDIR(
    cloud/filestore/public/sdk/python/client
    cloud/filestore/tests/python/lib
    cloud/storage/core/tools/testing/qemu/lib
)

END()
