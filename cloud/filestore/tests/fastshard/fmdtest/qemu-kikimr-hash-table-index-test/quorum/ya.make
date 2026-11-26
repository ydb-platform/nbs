PY3TEST()

SET(FILESTORE_FAST_SHARD_STORAGE_KIND quorum_mirror)
SRCDIR(${ARCADIA_ROOT}/cloud/filestore/tests/fastshard/fmdtest/qemu-kikimr-hash-table-index-test)
INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/fastshard/fmdtest/qemu-kikimr-hash-table-index-test/ya.make.inc)

END()
