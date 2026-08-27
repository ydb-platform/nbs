#include "executeaction.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>

#include <util/datetime/base.h>
#include <util/generic/ptr.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NFileStore::NLoadTest {

NProtoPrivate::TStorageStats TExecuteActionController::GetStorageStats()
{
    NProtoPrivate::TGetStorageStatsRequest request;
    request.SetFileSystemId(FilesystemId);
    request.SetCacheTTL(0);
    request.SetMode(NProtoPrivate::STATS_REQUEST_MODE_FORCE_FETCH_SHARDS);
    NProtoPrivate::TGetStorageStatsResponse response;
    ExecuteAction("getstoragestats", request, &response);
    if (response.HasError()) {
        STORAGE_ERROR("Failed to get storage stats");
    }
    return response.GetStats();
}

void TExecuteActionController::FlushBytes()
{
    ForcedOperation(NProtoPrivate::TForcedOperationRequest::E_FLUSH);
}

void TExecuteActionController::Flush()
{
    ForcedOperation(NProtoPrivate::TForcedOperationRequest::E_FLUSH_BYTES);
}

void TExecuteActionController::Compaction()
{
    ForcedOperation(NProtoPrivate::TForcedOperationRequest::E_COMPACTION);
}

void TExecuteActionController::Cleanup()
{
    ForcedOperation(NProtoPrivate::TForcedOperationRequest::E_CLEANUP);
}

void TExecuteActionController::CollectGarbage()
{
    ForcedOperation(NProtoPrivate::TForcedOperationRequest::E_COLLECT_GARBAGE);
}

void TExecuteActionController::ForcedOperation(
    NProtoPrivate::TForcedOperationRequest::EForcedOperationType type)
{
    const auto& typeName =
        NProtoPrivate::TForcedOperationRequest::EForcedOperationType_Name(type);

    NProtoPrivate::TForcedOperationRequest request;
    request.SetFileSystemId(FilesystemId);
    request.SetOpType(type);
    NProtoPrivate::TForcedOperationResponse response;
    ExecuteAction("forcedoperation", request, &response);
    if (response.HasError()) {
        throw yexception() << "failed to start forced operation " << typeName
                           << ": " << response.GetError();
    }

    while (true) {
        NProtoPrivate::TForcedOperationStatusRequest statusRequest;
        statusRequest.SetFileSystemId(FilesystemId);
        statusRequest.SetOperationId(response.GetOperationId());
        NProtoPrivate::TForcedOperationStatusResponse statusResponse;
        ExecuteAction("forcedoperationstatus", statusRequest, &statusResponse);

        if (statusResponse.GetError().GetCode() == E_NOT_FOUND) {
            throw yexception()
                << typeName << ": operation not found (tablet rebooted?)";
        }

        if (statusResponse.HasError()) {
            throw yexception()
                << typeName
                << ": failed to read op status: " << statusResponse.GetError();
        }

        bool done = false;
        switch (statusResponse.GetStatus()) {
            case NProtoPrivate::TForcedOperationStatusResponse::E_UNKNOWN:
            case NProtoPrivate::TForcedOperationStatusResponse::E_PENDING:
            case NProtoPrivate::TForcedOperationStatusResponse::E_RUNNING:
                STORAGE_INFO(TStringBuilder() << typeName << ": waiting...");
                break;
            case NProtoPrivate::TForcedOperationStatusResponse::E_COMPLETED:
            case NProtoPrivate::TForcedOperationStatusResponse::E_FAILED:
                STORAGE_INFO(TStringBuilder() << typeName << ": completed");
                done = true;
                break;
            default:
                STORAGE_INFO(
                    TStringBuilder() << typeName << ": unknown status");
                break;
        }
        if (done) {
            break;
        }

        Sleep(TDuration::Seconds(1));
    }
}

template <typename TRequest, typename TResponse>
void TExecuteActionController::ExecuteAction(
    const TString& action,
    const TRequest& requestProto,
    TResponse* responseProto)
{
    TString input;
    google::protobuf::util::MessageToJsonString(requestProto, &input);

    auto request = std::make_shared<NProto::TExecuteActionRequest>();
    request->SetAction(action);
    request->SetInput(std::move(input));

    const auto requestId = GetRequestId(*request);
    auto result = Client
                      ->ExecuteAction(
                          MakeIntrusive<TCallContext>(FilesystemId, requestId),
                          std::move(request))
                      .GetValueSync();

    if (HasError(result)) {
        responseProto->MutableError()->CopyFrom(result.GetError());
        return;
    }

    if (!google::protobuf::util::JsonStringToMessage(
             result.GetOutput(),
             responseProto)
             .ok())
    {
        responseProto->MutableError()->CopyFrom(MakeError(
            E_BADMSG,
            TStringBuilder()
                << "failed to parse response json: " << result.GetOutput()));
    }
}

}   // namespace NCloud::NFileStore::NLoadTest
