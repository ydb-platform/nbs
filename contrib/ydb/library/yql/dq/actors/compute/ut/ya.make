UNITTEST_FOR(contrib/ydb/library/yql/dq/actors/compute)

TAG(ya:manual)

SRCS(
    dq_compute_actor_ut.cpp
    dq_compute_actor_async_input_helper_ut.cpp
    dq_compute_issues_buffer_ut.cpp
    dq_source_watermark_tracker_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    contrib/ydb/library/yql/dq/actors
    contrib/ydb/library/actors/wilson
    contrib/ydb/library/actors/testlib
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/library/yql/minikql/comp_nodes/no_llvm
)

YQL_LAST_ABI_VERSION()

END()
