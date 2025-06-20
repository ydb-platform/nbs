PY3_PROGRAM(service-kikimr-recipe)

PY_SRCS(
    cloud/filestore/tests/recipes/service-kikimr/__main__.py
)

PEERDIR(
    cloud/filestore/tests/python/lib
    cloud/storage/core/tests/common
    cloud/storage/core/tools/testing/access_service/lib
    cloud/storage/core/tools/testing/access_service_new/lib

    library/python/testing/recipe
    library/python/testing/yatest_common
)

END()
