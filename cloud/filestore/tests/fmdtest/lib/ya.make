PY3_LIBRARY()

PY_SRCS(
    __init__.py
    multi_client.py
)

PEERDIR(
    cloud/storage/core/tools/testing/qemu/lib
)

END()
