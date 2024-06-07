LIBRARY()

SRCS(
    change_exchange.cpp
    change_record.cpp
    change_sender_common_ops.cpp
    change_sender_monitoring.cpp
)

GENERATE_ENUM_SERIALIZATION(change_record.h)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/scheme
    contrib/ydb/library/actors/core
    contrib/ydb/library/yverify_stream
    library/cpp/monlib/service/pages
)

YQL_LAST_ABI_VERSION()

END()
