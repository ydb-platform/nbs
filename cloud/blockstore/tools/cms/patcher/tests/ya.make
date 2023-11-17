PY3TEST()

SIZE(SMALL)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/blockstore/tools/cms/patcher
)

DATA(
    arcadia/cloud/blockstore/tools/cms/patcher/tests/data
)

END()
