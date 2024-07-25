PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

DEPENDS(
    cloud/blockstore/libs/encryption/ut_keyring/bin
)

TEST_SRCS(
    test.py
)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
