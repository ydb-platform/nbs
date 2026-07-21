#pragma once

#include <grpcpp/support/status.h>

#include <contrib/ydb/library/yql/dq/actors/protos/dq_status_codes.pb.h>
#include <contrib/ydb/library/yql/providers/generic/connector/api/service/protos/error.pb.h>
#include <contrib/ydb/public/api/protos/ydb_status_codes.pb.h>
#include <contrib/ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>
#include <contrib/ydb/library/yql/public/issue/yql_issue.h>
#include <contrib/ydb/library/yql/utils/yql_panic.h>

namespace NYql::NConnector {
    NApi::TError NewSuccess();

    template <typename TResponse>
    bool IsSuccess(const TResponse& response) {
        if (!response.has_error()) {
            return true;
        }

        return response.error().status() == Ydb::StatusIds_StatusCode::StatusIds_StatusCode_SUCCESS;
    }

    TIssues ErrorToIssues(const NApi::TError& error, TString prefix = "");

    NDqProto::StatusIds::StatusCode ErrorToDqStatus(const NApi::TError& error);

    NApi::TError ErrorFromGRPCStatus(const NYdbGrpc::TGrpcStatus& status);

    inline bool GrpcStatusEndOfStream(const NYdbGrpc::TGrpcStatus& status) noexcept {
        return status.GRpcStatusCode == grpc::OUT_OF_RANGE && status.Msg == "Read EOF";
    }

    inline bool GrpcStatusNeedsRetry(const NYdbGrpc::TGrpcStatus& status) noexcept {
        return status.GRpcStatusCode == grpc::UNAVAILABLE;
    }
} // namespace NYql::NConnector
