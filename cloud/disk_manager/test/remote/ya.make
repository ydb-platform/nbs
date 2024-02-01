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
    cloud/disk_manager/test/remote/cmd
)

DATA(
    arcadia/cloud/disk_manager/test/remote/cmd/cmd
    arcadia/cloud/nbs_internal/disk_manager/test/remote/configs/hw-nbs-stable-lab
)

TIMEOUT(14400)

END()

RECURSE(
    cmd
)
