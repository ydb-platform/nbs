PY3TEST()

SET(ARCADIA_SANDBOX_SINGLESLOT TRUE)

SRCDIR(${ARCADIA_ROOT}/cloud/blockstore/tests/loadtest/local-nonrepl)
INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/loadtest/local-nonrepl/ya.make.inc)

ENV(DEDICATED_DISK_AGENT="true")
ENV(NBS_LOCAL_NONREPL_BACKEND="io_uring")

DEPENDS(
    cloud/blockstore/apps/disk_agent
    cloud/blockstore/apps/server
)

END()
