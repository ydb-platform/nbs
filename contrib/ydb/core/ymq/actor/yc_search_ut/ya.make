UNITTEST()

PEERDIR(
    contrib/ydb/core/testlib/default
    contrib/ydb/core/ymq/actor
)

SRCS(
    index_events_processor_ut.cpp
    test_events_writer.cpp
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

END()
