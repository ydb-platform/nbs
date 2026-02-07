UNITTEST_FOR(contrib/ydb/core/audit)

PEERDIR(
    contrib/ydb/library/actors/testlib
    contrib/ydb/core/audit/heartbeat_actor
)

SRCS(
    audit_log_service_ut.cpp
    audit_log_ut.cpp
)

END()
