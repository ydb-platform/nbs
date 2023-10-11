OWNER(g:cloud-nbs)

PY3TEST()

TEST_SRCS(
    test.py
)

TAG(
    ya:external
    ya:fat
    ya:manual
)

SIZE(LARGE)

REQUIREMENTS(
    cpu:4
    ram:16
)

DEPENDS(
    cloud/disk_manager/test/loadtest/cmd
)

DATA(
    arcadia/cloud/disk_manager/test/loadtest/cmd/cmd
)

TIMEOUT(7200)

END()

RECURSE(
    cmd
)

