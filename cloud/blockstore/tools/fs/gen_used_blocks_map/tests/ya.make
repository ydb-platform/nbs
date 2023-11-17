PY3TEST()

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/blockstore/tools/fs/gen_used_blocks_map
)

PEERDIR(
)

DATA(
    arcadia/cloud/blockstore/tools/fs/gen_used_blocks_map/tests/data
)

END()
