PY3TEST()

SET(SERVICE_LOCAL_FILE_IO aio)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/loadtest/service-local-test/ya.make.inc)

END()

RECURSE_FOR_TESTS(
    io_uring
)
