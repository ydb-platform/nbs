EXECTEST()

RUN(
    stream_store
)

RUN(
    pack_tuple
)

DEPENDS(
    contrib/ydb/library/yql/utils/simd/exec/stream_store
    contrib/ydb/library/yql/utils/simd/exec/pack_tuple
)

PEERDIR(
    contrib/ydb/library/yql/utils/simd
)

END()

RECURSE(
    pack_tuple
    stream_store
)