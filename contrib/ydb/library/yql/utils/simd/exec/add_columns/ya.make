OWNER(g:yql)

PROGRAM(add_columns)

SRCS(main.cpp)

SIZE(MEDIUM)

CFLAGS(-mavx2)

PEERDIR(contrib/ydb/library/yql/utils/simd)

END()
