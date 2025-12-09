UNITTEST()

FORK_SUBTESTS()
SRCS(
    main.cpp
)
PEERDIR(
    contrib/ydb/library/lockfree_bucket
)

END()
