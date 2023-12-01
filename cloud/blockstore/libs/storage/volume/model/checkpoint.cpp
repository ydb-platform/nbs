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
    ECheckpointRequestType reqType,
    ECheckpointType type)
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
        ECheckpointRequestState::Received,
        type});
}

void TCheckpointStore::SetCheckpointRequestSaved(ui64 requestId)
{
    auto& checkpointRequest = GetRequest(requestId);
    Y_DEBUG_ABORT_UNLESS(
        checkpointRequest.State == ECheckpointRequestState::Received);
    checkpointRequest.State = ECheckpointRequestState::Saved;
}

void TCheckpointStore::SetCheckpointRequestInProgress(ui64 requestId)
{
    auto& checkpointRequest = GetRequest(requestId);
    Y_DEBUG_ABORT_UNLESS(checkpointRequest.State == ECheckpointRequestState::Saved);

    CheckpointRequestInProgress = requestId;
    CheckpointBeingCreated =
        checkpointRequest.ReqType == ECheckpointRequestType::Create ||
        checkpointRequest.ReqType == ECheckpointRequestType::CreateWithoutData;
}

void TCheckpointStore::SetCheckpointRequestFinished(
    ui64 requestId,
    bool success)
{
    Y_DEBUG_ABORT_UNLESS(CheckpointRequestInProgress == requestId);
    if (requestId == CheckpointRequestInProgress) {
        auto& checkpointRequest = GetRequest(requestId);
        Y_DEBUG_ABORT_UNLESS(
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
    if (const auto* data = ActiveCheckpoints.FindPtr(checkpointId)) {
        return data->Data == ECheckpointData::DataPresent;
    }
    return false;
}

bool TCheckpointStore::HasRequestToExecute(ui64* requestId) const
{
    Y_DEBUG_ABORT_UNLESS(requestId);

    for (const auto& [key, checkpointRequest]: CheckpointRequests) {
        if (checkpointRequest.State != ECheckpointRequestState::Saved) {
            continue;
        }
        Y_DEBUG_ABORT_UNLESS(key == checkpointRequest.RequestId);
        if (requestId) {
            *requestId = checkpointRequest.RequestId;
        }
        return true;
    }
    return false;
}

std::optional<ECheckpointType> TCheckpointStore::GetCheckpointType(
    const TString& checkpointId) const
{
    auto ptr = ActiveCheckpoints.FindPtr(checkpointId);
    return ptr ? ptr->Type : std::optional<ECheckpointType>{};
}

TVector<TString> TCheckpointStore::GetLightCheckpoints() const
{
    TVector<TString> checkpoints(Reserve(ActiveCheckpoints.size()));
    for (const auto& [checkpoint, checkpointData]: ActiveCheckpoints) {
        if (checkpointData.Type == ECheckpointType::Light) {
            checkpoints.push_back(checkpoint);
        }
    }
    return checkpoints;
}

TVector<TString> TCheckpointStore::GetCheckpointsWithData() const
{
    TVector<TString> checkpoints(Reserve(ActiveCheckpoints.size()));
    for (const auto& [checkpoint, checkpointData]: ActiveCheckpoints) {
        if (checkpointData.Data == ECheckpointData::DataPresent) {
            checkpoints.push_back(checkpoint);
        }
    }
    return checkpoints;
}

const TActiveCheckpointsMap& TCheckpointStore::GetActiveCheckpoints() const
{
    return ActiveCheckpoints;
}

const TCheckpointRequest& TCheckpointStore::GetRequestById(ui64 requestId) const
{
    if (const TCheckpointRequest* checkpointRequest =
            CheckpointRequests.FindPtr(requestId))
    {
        Y_DEBUG_ABORT_UNLESS(requestId == checkpointRequest->RequestId);
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
        Y_DEBUG_ABORT_UNLESS(requestId == checkpointRequest->RequestId);
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
    Y_DEBUG_ABORT_UNLESS(inserted);
    Apply(it->second);
    return it->second;
}

void TCheckpointStore::AddCheckpoint(
    const TCheckpointRequest& checkpointRequest,
    bool forceDataDeleted)
{
    ECheckpointData checkpointData =
        checkpointRequest.Type == ECheckpointType::Normal && !forceDataDeleted
            ? ECheckpointData::DataPresent
            : ECheckpointData::DataDeleted;

    ActiveCheckpoints.emplace(
        checkpointRequest.CheckpointId,
        TActiveCheckpointsType{checkpointRequest.Type, checkpointData});
    CalcDoesCheckpointWithDataExist();
}

void TCheckpointStore::DeleteCheckpointData(const TString& checkpointId)
{
    if (auto* checkpointData = ActiveCheckpoints.FindPtr(checkpointId)) {
        checkpointData->Data = ECheckpointData::DataDeleted;
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
        { return it.second.Data == ECheckpointData::DataPresent; });
}

void TCheckpointStore::Apply(const TCheckpointRequest& checkpointRequest)
{
    if (checkpointRequest.State != ECheckpointRequestState::Completed) {
        return;
    }
    switch (checkpointRequest.ReqType) {
        case ECheckpointRequestType::Create: {
            AddCheckpoint(checkpointRequest, false);
            break;
        }
        case ECheckpointRequestType::CreateWithoutData: {
            AddCheckpoint(checkpointRequest, true);
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
            Y_DEBUG_ABORT_UNLESS(0);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
