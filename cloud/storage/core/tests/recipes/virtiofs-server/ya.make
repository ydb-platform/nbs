PY3_PROGRAM(virtiofs-server-recipe)

PY_SRCS(__main__.py)

DEPENDS(
    cloud/storage/core/tools/testing/virtiofs_server/bin
)

PEERDIR(
    cloud/storage/core/tools/testing/virtiofs_server/lib
    cloud/storage/core/tools/testing/qemu/lib
    cloud/storage/core/tests/common

    library/python/testing/recipe
    library/python/testing/yatest_common
)

END()
