#include "checkpoint.h"

#include <cloud/storage/core/libs/common/verify.h>

#include <util/generic/algorithm.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TCheckpointStore::TCheckpointStore(
    TVector<TCheckpointRequest> checkpointRequests,
    const TString& diskID)
    : DiskID(diskID)
{
    SortBy(
        checkpointRequests,
        [](const TCheckpointRequest& operand) { return operand.RequestId; });

    for (auto& checkpointRequest: checkpointRequests) {
        AddCheckpointRequest(std::move(checkpointRequest));
    }
}

const TCheckpointRequest& TCheckpointStore::CreateNew(
    TString checkpointId,
    TInstant timestamp,
    ECheckpointRequestType reqType)
{
    if (!CheckpointRequests.empty()) {
        const auto& lastReq = CheckpointRequests.rbegin()->second;
        timestamp =
            Max(timestamp, lastReq.Timestamp + TDuration::MicroSeconds(1));
    }

    return AddCheckpointRequest(TCheckpointRequest{
        ++LastCheckpointRequestId,
        std::move(checkpointId),
        timestamp,
        reqType,
        ECheckpointRequestState::Received});
}

void TCheckpointStore::SetCheckpointRequestSaved(ui64 requestId)
{
    auto& checkpointRequest = GetRequest(requestId);
    Y_VERIFY_DEBUG(
        checkpointRequest.State == ECheckpointRequestState::Received);
    checkpointRequest.State = ECheckpointRequestState::Saved;
}

void TCheckpointStore::SetCheckpointRequestInProgress(ui64 requestId)
{
    auto& checkpointRequest = GetRequest(requestId);
    Y_VERIFY_DEBUG(checkpointRequest.State == ECheckpointRequestState::Saved);

    CheckpointRequestInProgress = requestId;
    CheckpointBeingCreated =
        checkpointRequest.ReqType == ECheckpointRequestType::Create;
}

void TCheckpointStore::SetCheckpointRequestFinished(
    ui64 requestId,
    bool success)
{
    Y_VERIFY_DEBUG(CheckpointRequestInProgress == requestId);
    if (requestId == CheckpointRequestInProgress) {
        auto& checkpointRequest = GetRequest(requestId);
        Y_VERIFY_DEBUG(
            checkpointRequest.State == ECheckpointRequestState::Saved);
        checkpointRequest.State = success ? ECheckpointRequestState::Completed
                                          : ECheckpointRequestState::Rejected;
        Apply(checkpointRequest);
    }
    CheckpointRequestInProgress = 0;
    CheckpointBeingCreated = false;
}

bool TCheckpointStore::IsRequestInProgress() const
{
    return CheckpointRequestInProgress != 0;
}

bool TCheckpointStore::IsCheckpointBeingCreated() const
{
    return CheckpointBeingCreated;
}

bool TCheckpointStore::DoesCheckpointWithDataExist() const
{
    return CheckpointWithDataExists;
}

bool TCheckpointStore::DoesCheckpointHaveData(const TString& checkpointId) const
{
    if (const ECheckpointData* data = ActiveCheckpoints.FindPtr(checkpointId)) {
        return *data == ECheckpointData::DataPresent;
    }
    return false;
}

bool TCheckpointStore::HasRequestToExecute(ui64* requestId) const
{
    Y_VERIFY_DEBUG(requestId);

    for (const auto& [key, checkpointRequest]: CheckpointRequests) {
        if (checkpointRequest.State != ECheckpointRequestState::Saved) {
            continue;
        }
        Y_VERIFY_DEBUG(key == checkpointRequest.RequestId);
        if (requestId) {
            *requestId = checkpointRequest.RequestId;
        }
        return true;
    }
    return false;
}

TVector<TString> TCheckpointStore::GetCheckpointsWithData() const
{
    TVector<TString> checkpoints(Reserve(ActiveCheckpoints.size()));
    for (const auto& [checkpoint, checkpointData]: ActiveCheckpoints) {
        if (checkpointData == ECheckpointData::DataPresent) {
            checkpoints.push_back(checkpoint);
        }
    }
    return checkpoints;
}

const TMap<TString, ECheckpointData>& TCheckpointStore::GetActiveCheckpoints()
    const
{
    return ActiveCheckpoints;
}

const TCheckpointRequest& TCheckpointStore::GetRequestById(ui64 requestId) const
{
    if (const TCheckpointRequest* checkpointRequest =
            CheckpointRequests.FindPtr(requestId))
    {
        Y_VERIFY_DEBUG(requestId == checkpointRequest->RequestId);
        return *checkpointRequest;
    }
    STORAGE_VERIFY(0, TWellKnownEntityTypes::DISK, DiskID);
}

TVector<TCheckpointRequest> TCheckpointStore::GetCheckpointRequests() const
{
    TVector<TCheckpointRequest> result;
    result.reserve(CheckpointRequests.size());
    for (const auto& [key, checkpointRequest]: CheckpointRequests) {
        result.push_back(checkpointRequest);
    }
    return result;
}

TCheckpointRequest& TCheckpointStore::GetRequest(ui64 requestId)
{
    if (TCheckpointRequest* checkpointRequest =
            CheckpointRequests.FindPtr(requestId))
    {
        Y_VERIFY_DEBUG(requestId == checkpointRequest->RequestId);
        return *checkpointRequest;
    }
    STORAGE_VERIFY(0, TWellKnownEntityTypes::DISK, DiskID);
}

TCheckpointRequest& TCheckpointStore::AddCheckpointRequest(
    TCheckpointRequest checkpointRequest)
{
    LastCheckpointRequestId =
        Max(LastCheckpointRequestId, checkpointRequest.RequestId);
    auto requestId = checkpointRequest.RequestId;
    auto [it, inserted] =
        CheckpointRequests.insert({requestId, std::move(checkpointRequest)});
    Y_VERIFY_DEBUG(inserted);
    Apply(it->second);
    return it->second;
}

void TCheckpointStore::AddCheckpoint(const TString& checkpointId)
{
    ActiveCheckpoints.emplace(checkpointId, ECheckpointData::DataPresent);
    CalcDoesCheckpointWithDataExist();
}

void TCheckpointStore::DeleteCheckpointData(const TString& checkpointId)
{
    if (auto* checkpointData = ActiveCheckpoints.FindPtr(checkpointId)) {
        *checkpointData = ECheckpointData::DataDeleted;
    }
    CalcDoesCheckpointWithDataExist();
}

void TCheckpointStore::DeleteCheckpoint(const TString& checkpointId)
{
    ActiveCheckpoints.erase(checkpointId);
    CalcDoesCheckpointWithDataExist();
}

void TCheckpointStore::CalcDoesCheckpointWithDataExist()
{
    CheckpointWithDataExists = AnyOf(
        ActiveCheckpoints,
        [](const auto& it)
        { return it.second == ECheckpointData::DataPresent; });
}

void TCheckpointStore::Apply(const TCheckpointRequest& checkpointRequest)
{
    if (checkpointRequest.State != ECheckpointRequestState::Completed) {
        return;
    }
    switch (checkpointRequest.ReqType) {
        case ECheckpointRequestType::Create: {
            AddCheckpoint(checkpointRequest.CheckpointId);
            break;
        }
        case ECheckpointRequestType::Delete: {
            DeleteCheckpoint(checkpointRequest.CheckpointId);
            break;
        }
        case ECheckpointRequestType::DeleteData: {
            DeleteCheckpointData(checkpointRequest.CheckpointId);
            break;
        }
        default: {
            Y_VERIFY_DEBUG(0);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
