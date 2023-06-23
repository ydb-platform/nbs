PY3_PROGRAM(qemu-recipe)

PY_SRCS(__main__.py)

PEERDIR(
    cloud/storage/core/tools/testing/qemu/lib

    library/python/testing/recipe
    library/python/testing/yatest_common
)

END()
