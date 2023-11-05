#pragma once

#include <contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/internal_header.h>

#include <contrib/ydb/public/sdk/cpp/client/ydb_query/query.h>
#include <contrib/ydb/public/sdk/cpp/client/ydb_query/tx.h>
#include <contrib/ydb/public/sdk/cpp/client/impl/ydb_internal/grpc_connections/grpc_connections.h>
#include <contrib/ydb/public/sdk/cpp/client/ydb_params/params.h>

namespace NYdb::NQuery {

class TExecQueryImpl {
public:
    static TAsyncExecuteQueryIterator StreamExecuteQuery(const std::shared_ptr<TGRpcConnectionsImpl>& connections,
        const TDbDriverStatePtr& driverState, const TString& query, const TTxControl& txControl,
        const TMaybe<TParams>& params, const TExecuteQuerySettings& settings, const TString& sessionId);

    static TAsyncExecuteQueryResult ExecuteQuery(const std::shared_ptr<TGRpcConnectionsImpl>& connections,
        const TDbDriverStatePtr& driverState, const TString& query, const TTxControl& txControl,
        const TMaybe<TParams>& params, const TExecuteQuerySettings& settings, const TString& sessionId);
};

} // namespace NYdb::NQuery::NImpl
