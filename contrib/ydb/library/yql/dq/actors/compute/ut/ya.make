UNITTEST_FOR(contrib/ydb/library/yql/dq/actors/compute)

SRCS(
    dq_compute_issues_buffer_ut.cpp
    dq_source_watermark_tracker_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/library/yql/dq/actors
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql/pg_dummy
)

END()
