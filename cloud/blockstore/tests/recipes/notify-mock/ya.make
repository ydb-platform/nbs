PY3_PROGRAM(notify-mock-recipe)

PY_SRCS(__main__.py)

PEERDIR(
    cloud/blockstore/tests/python/lib

    library/python/testing/recipe
    library/python/testing/yatest_common
)

IF (NOT OPENSOURCE)
    PEERDIR(
        contrib/python/requests     # TODO: NBS-4453
    )
ENDIF()

FILES(
    start.sh
    stop.sh
)

END()
