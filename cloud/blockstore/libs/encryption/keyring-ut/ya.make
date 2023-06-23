PY3TEST()

TAG(
    ya:not_autocheck
    ya:manual
)

SIZE(MEDIUM)

DEPENDS(
    cloud/blockstore/libs/encryption/keyring-ut/bin
)

TEST_SRCS(
    test.py
)

SET(QEMU_ENABLE_KVM False)
INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/qemu.inc)

END()
