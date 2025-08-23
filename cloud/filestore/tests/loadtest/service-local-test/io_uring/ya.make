PY3TEST()

SET(SERVICE_LOCAL_FILE_IO io_uring)
SRCDIR(${ARCADIA_ROOT}/cloud/filestore/tests/loadtest/service-local-test)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/loadtest/service-local-test/ya.make.inc)

END()
