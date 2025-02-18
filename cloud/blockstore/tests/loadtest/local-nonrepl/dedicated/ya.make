PY3TEST()

SET(ARCADIA_SANDBOX_SINGLESLOT TRUE)

SRCDIR(${ARCADIA_ROOT}/cloud/blockstore/tests/loadtest/local-nonrepl)
INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/loadtest/local-nonrepl/ya.make.inc)

ENV(DEDICATED_DISK_AGENT="true")

DEPENDS(
    cloud/blockstore/apps/disk_agent
    cloud/blockstore/apps/server
)

END()
