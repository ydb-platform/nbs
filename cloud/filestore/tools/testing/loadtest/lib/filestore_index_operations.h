#pragma once

#include <cloud/filestore/libs/service/public.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NLoadTest {

class TFileStoreIndexOperations
{
    TLog& Log;
    IFileStoreServicePtr Client;
    TString FilesystemId;
    TInstant DeadlineTs;
    TVector<TString> FilesystemIds;

public:
    TFileStoreIndexOperations(
        TLog& log,
        const IFileStoreServicePtr& client,
        const TString& filesystemId,
        TInstant deadlineTs);

    NProtoPrivate::TStorageStats GetStorageStats();

    void FlushBytes();
    void Flush();
    void Compaction();
    void Cleanup();
    void CollectGarbage();

private:
    TVector<TString> GetShardFileSystemIds();

    void ForcedOperation(
        NProtoPrivate::TForcedOperationRequest::EForcedOperationType type);
    template <typename TPredicate>
    void ForcedOperationUntil(
        NProtoPrivate::TForcedOperationRequest::EForcedOperationType type,
        TPredicate isCompleted);
    void ForcedOperation(
        const TString& filesystemId,
        NProtoPrivate::TForcedOperationRequest::EForcedOperationType type);
    TString RunForcedOperation(
        const TString& filesystemId,
        NProtoPrivate::TForcedOperationRequest::EForcedOperationType type);

    template <typename TRequest, typename TResponse>
    void ExecuteAction(
        const TString& action,
        const TRequest& requestProto,
        TResponse* responseProto);
};

}   // namespace NCloud::NFileStore::NLoadTest
