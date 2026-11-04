#include "filestore_index_operations.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/ptr.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>

#include <google/protobuf/util/json_util.h>

namespace NCloud::NFileStore::NLoadTest {

TFileStoreCleanup::TFileStoreCleanup(
    TLog& log,
    const IFileStoreServicePtr& client,
    const TString& filesystemId,
    TInstant deadlineTs)
    : Log(log)
    , Client(client)
    , FilesystemId(filesystemId)
    , DeadlineTs(deadlineTs)
    , FilesystemIds{filesystemId}
{
    const auto shardIds = GetShardFileSystemIds();
    FilesystemIds.insert(FilesystemIds.end(), shardIds.begin(), shardIds.end());
}

NProtoPrivate::TStorageStats TFileStoreCleanup::GetStorageStats()
{
    NProtoPrivate::TGetStorageStatsRequest request;
    request.SetFileSystemId(FilesystemId);
    request.SetCacheTTL(0);
    NProtoPrivate::TGetStorageStatsResponse response;
    ExecuteAction("getstoragestats", request, &response);
    if (HasError(response)) {
        throw yexception() << FilesystemId << ": "
                           << "failed to execute getstoragestats: "
                           << response.GetError();
    }
    return response.GetStats();
}

TVector<TString> TFileStoreCleanup::GetShardFileSystemIds()
{
    NProtoPrivate::TGetFileSystemTopologyRequest request;
    request.SetFileSystemId(FilesystemId);
    NProtoPrivate::TGetFileSystemTopologyResponse response;
    ExecuteAction("getfilesystemtopology", request, &response);
    if (HasError(response)) {
        throw yexception() << FilesystemId << ": "
                           << "failed to execute getfilesystemtopology: "
                           << response.GetError();
    }

    const auto& shardIds = response.GetShardFileSystemIds();
    return {shardIds.begin(), shardIds.end()};
}

void TFileStoreCleanup::FlushBytes()
{
    // Each request processes one chunk and may reply before trimming finishes.
    ForcedOperationUntil(
        NProtoPrivate::TForcedOperationRequest::E_FLUSH_BYTES,
        [](const NProtoPrivate::TStorageStats& stats)
        {
            return stats.GetFreshBytesCount() == 0 &&
                   stats.GetDeletedFreshBytesCount() == 0;
        });
}

void TFileStoreCleanup::Flush()
{
    ForcedOperation(NProtoPrivate::TForcedOperationRequest::E_FLUSH);
}

void TFileStoreCleanup::Compaction()
{
    ForcedOperation(NProtoPrivate::TForcedOperationRequest::E_COMPACTION);
}

void TFileStoreCleanup::Cleanup()
{
    ForcedOperation(NProtoPrivate::TForcedOperationRequest::E_CLEANUP);
}

void TFileStoreCleanup::CollectGarbage()
{
    // Pending barriers may prevent a request from draining the garbage queue.
    ForcedOperationUntil(
        NProtoPrivate::TForcedOperationRequest::E_COLLECT_GARBAGE,
        [](const NProtoPrivate::TStorageStats& stats)
        { return stats.GetGarbageQueueSize() == 0; });
}

template <typename TPredicate>
void TFileStoreCleanup::ForcedOperationUntil(
    NProtoPrivate::TForcedOperationRequest::EForcedOperationType type,
    TPredicate isCompleted)
{
    while (true) {
        ForcedOperation(type);
        if (isCompleted(GetStorageStats())) {
            return;
        }

        if (TInstant::Now() >= DeadlineTs) {
            throw yexception() << "fs=" << FilesystemId << " "
                               << NProtoPrivate::TForcedOperationRequest::
                                      EForcedOperationType_Name(type)
                               << ": deadline exceeded.";
        }
        Sleep(TDuration::Seconds(1));
    }
}

void TFileStoreCleanup::ForcedOperation(
    NProtoPrivate::TForcedOperationRequest::EForcedOperationType type)
{
    for (const auto& filesystemId: FilesystemIds) {
        ForcedOperation(filesystemId, type);
    }
}

void TFileStoreCleanup::ForcedOperation(
    const TString& filesystemId,
    NProtoPrivate::TForcedOperationRequest::EForcedOperationType type)
{
    const auto& typeName =
        NProtoPrivate::TForcedOperationRequest::EForcedOperationType_Name(type);

    auto operationId = RunForcedOperation(filesystemId, type);

    while (true) {
        NProtoPrivate::TForcedOperationStatusRequest statusRequest;
        statusRequest.SetFileSystemId(filesystemId);
        statusRequest.SetOperationId(operationId);
        NProtoPrivate::TForcedOperationStatusResponse statusResponse;
        ExecuteAction("forcedoperationstatus", statusRequest, &statusResponse);

        if (statusResponse.GetError().GetCode() == E_NOT_FOUND) {
            STORAGE_WARN(
                TStringBuilder() << "fs=" << filesystemId << " " << typeName
                                 << ": operation status not found, restarting");
            operationId = RunForcedOperation(filesystemId, type);
            continue;
        }

        if (HasError(statusResponse)) {
            throw yexception()
                << "fs=" << filesystemId << " " << typeName
                << ": failed to read op status: " << statusResponse.GetError();
        }

        switch (statusResponse.GetOperationStatus()) {
            case NProtoPrivate::TForcedOperationStatusResponse::E_UNKNOWN:
            case NProtoPrivate::TForcedOperationStatusResponse::E_PENDING:
            case NProtoPrivate::TForcedOperationStatusResponse::E_RUNNING:
                STORAGE_INFO(
                    TStringBuilder() << "fs=" << filesystemId << " " << typeName
                                     << ": waiting...");
                break;
            case NProtoPrivate::TForcedOperationStatusResponse::E_COMPLETED:
                STORAGE_INFO(
                    TStringBuilder() << "fs=" << filesystemId << " " << typeName
                                     << ": completed");
                return;
            case NProtoPrivate::TForcedOperationStatusResponse::E_FAILED: {
                const auto& error = statusResponse.GetOperationError();
                if (GetErrorKind(error) == EErrorKind::ErrorRetriable) {
                    STORAGE_WARN(
                        TStringBuilder()
                        << "fs=" << filesystemId << " " << typeName
                        << ": operation failed, restarting: "
                        << FormatError(error));
                    Sleep(TDuration::Seconds(1));
                    operationId = RunForcedOperation(filesystemId, type);
                    continue;
                }
                throw yexception()
                    << "fs=" << filesystemId << " " << typeName << ": failed: "
                    << (HasError(error) ? FormatError(error) : "without error");
            }
            default:
                STORAGE_WARN(
                    TStringBuilder() << "fs=" << filesystemId << " " << typeName
                                     << ": unknown status");
                break;
        }

        Sleep(TDuration::Seconds(1));
    }
}

TString TFileStoreCleanup::RunForcedOperation(
    const TString& filesystemId,
    NProtoPrivate::TForcedOperationRequest::EForcedOperationType type)
{
    while (true) {
        NProtoPrivate::TForcedOperationRequest request;
        request.SetFileSystemId(filesystemId);
        request.SetOpType(type);

        NProtoPrivate::TForcedOperationResponse response;
        ExecuteAction("forcedoperation", request, &response);
        if (HasError(response)) {
            if (response.GetError().GetCode() == E_TRY_AGAIN) {
                Sleep(TDuration::Seconds(1));
                continue;
            }
            const auto& typeName = NProtoPrivate::TForcedOperationRequest::
                EForcedOperationType_Name(type);
            throw yexception() << "fs=" << filesystemId << " "
                               << "failed to start forced operation "
                               << typeName << ": " << response.GetError();
        }

        // The same type of operation is already running.
        // In essence, this is a form of E_TRY_AGAIN.
        // TODO: return current operationId in response.
        if (response.GetError().GetCode() == S_ALREADY) {
            Sleep(TDuration::Seconds(1));
            continue;
        }
        return response.GetOperationId();
    }
}

template <typename TRequest, typename TResponse>
void TFileStoreCleanup::ExecuteAction(
    const TString& action,
    const TRequest& requestProto,
    TResponse* responseProto)
{
    const auto& filesystemId = requestProto.GetFileSystemId();
    if (TInstant::Now() >= DeadlineTs) {
        throw yexception() << "fs=" << filesystemId << " " << action
                           << ": deadline exceeded";
    }

    TString input;
    auto status =
        google::protobuf::util::MessageToJsonString(requestProto, &input);
    if (!status.ok()) {
        responseProto->MutableError()->CopyFrom(MakeError(
            E_BADMSG,
            TStringBuilder() << "failed to serialize request in json: "
                             << status.ToString()));
        return;
    }

    auto request = std::make_shared<NProto::TExecuteActionRequest>();
    request->SetAction(action);
    request->SetInput(input);

    const auto requestId = GetRequestId(*request);
    auto future = Client->ExecuteAction(
        MakeIntrusive<TCallContext>(filesystemId, requestId),
        std::move(request));
    if (!future.Wait(DeadlineTs)) {
        throw yexception()
            << "fs=" << filesystemId
            << ": deadline exceeded waiting for response; request=" << input;
    }
    const auto& result = future.GetValue();

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
