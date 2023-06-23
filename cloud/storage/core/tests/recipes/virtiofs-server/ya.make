PY3_PROGRAM(virtiofs-server-recipe)

PY_SRCS(__main__.py)

DEPENDS(
    cloud/storage/core/tools/testing/virtiofs_server/bin
)

PEERDIR(
    cloud/filestore/tests/python/lib
    cloud/storage/core/tools/testing/virtiofs_server/lib
    devtools/ya/core/config

    library/python/testing/recipe
    library/python/testing/yatest_common
)

END()
