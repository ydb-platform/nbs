PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

TAG(
    ya:not_autocheck
    ya:manual
)

DEPENDS(
    cloud/blockstore/libs/encryption/ut_keyring/bin
)

TEST_SRCS(
    test.py
)

SET(QEMU_ENABLE_KVM False)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
