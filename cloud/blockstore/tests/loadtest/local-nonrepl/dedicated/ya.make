PY3TEST()

ENV(ARCADIA_SANDBOX_SINGLESLOT="true")

SRCDIR(${ARCADIA_ROOT}/cloud/blockstore/tests/loadtest/local-nonrepl)
INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/loadtest/local-nonrepl/ya.make.inc)

ENV(DEDICATED_DISK_AGENT="true")

DEPENDS(
    cloud/blockstore/apps/disk_agent
    cloud/blockstore/apps/server
)

END()
