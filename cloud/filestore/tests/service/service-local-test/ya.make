PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

PEERDIR(
    cloud/filestore/public/sdk/python/client
)

TEST_SRCS(
    test.py
)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-local.inc)

END()
