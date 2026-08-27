#pragma once

#include <cloud/filestore/libs/service/public.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NLoadTest {

class TExecuteActionController
{
    TLog& Log;
    IFileStoreServicePtr Client;
    TString FilesystemId;

public:
    TExecuteActionController(
        TLog& log,
        const IFileStoreServicePtr& client,
        const TString& filesystemId)
        : Log(log)
        , Client(client)
        , FilesystemId(filesystemId)
    {}

    NProtoPrivate::TStorageStats GetStorageStats();

    void FlushBytes();
    void Flush();
    void Compaction();
    void Cleanup();
    void CollectGarbage();

private:
    void ForcedOperation(
        NProtoPrivate::TForcedOperationRequest::EForcedOperationType type);

    template <typename TRequest, typename TResponse>
    void ExecuteAction(
        const TString& action,
        const TRequest& requestProto,
        TResponse* responseProto);
};

}   // namespace NCloud::NFileStore::NLoadTest
