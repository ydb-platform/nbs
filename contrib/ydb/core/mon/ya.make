LIBRARY()

SRCS(
    async_http_mon.cpp
    async_http_mon.h
    mon.cpp
    mon.h
    sync_http_mon.cpp
    sync_http_mon.h
    crossref.cpp
    crossref.h
)

PEERDIR(
    library/cpp/json
    library/cpp/lwtrace/mon
    library/cpp/protobuf/json
    library/cpp/string_utils/url
    contrib/ydb/core/base
    contrib/ydb/core/grpc_services/base
    contrib/ydb/core/mon/audit
    contrib/ydb/core/protos
    contrib/ydb/library/aclib
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/http
    contrib/ydb/library/yql/public/issue
    contrib/ydb/public/sdk/cpp/client/ydb_types/status
)

END()
