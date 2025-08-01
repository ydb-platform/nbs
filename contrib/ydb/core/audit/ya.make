LIBRARY()

SRCS(
    audit_log.h
    audit_log_service.h
    audit_log_impl.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    library/cpp/json
    library/cpp/logger
    contrib/ydb/core/base
)

END()
