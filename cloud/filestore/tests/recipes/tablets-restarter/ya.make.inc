PY3_PROGRAM(filestore-tablets-restarter)


DEPENDS(
    cloud/filestore/apps/client
)

PEERDIR(
    cloud/filestore/tests/python/lib

    library/python/testing/recipe
    library/python/testing/yatest_common
)

PY_SRCS(
    __main__.py
)

END()
