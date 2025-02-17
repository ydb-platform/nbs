PY3TEST()

SET(ARCADIA_SANDBOX_SINGLESLOT TRUE)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/loadtest/local-nonrepl/ya.make.inc)

DEPENDS(
    cloud/blockstore/apps/server
)

END()

RECURSE_FOR_TESTS(
    dedicated
)
