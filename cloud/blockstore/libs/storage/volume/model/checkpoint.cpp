#include "checkpoint.h"

#include <cloud/storage/core/libs/common/verify.h>

#include <util/generic/algorithm.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

bool TActiveCheckpointInfo::IsShadowDiskBased() const
{
    return ShadowDiskState != EShadowDiskState::None;
}

bool TActiveCheckpointInfo::ShouldBlockWrites() const
{
    if (Type == ECheckpointType::Light) {
        return false;
    }
    if (Data == ECheckpointData::DataDeleted) {
        return false;
    }
    return !IsShadowDiskBased();
}

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
    Y_DEBUG_ABORT_UNLESS(
        checkpointRequest.State == ECheckpointRequestState::Saved);

    CheckpointRequestInProgress = requestId;
    CheckpointBeingCreated =
        checkpointRequest.ReqType == ECheckpointRequestType::Create ||
        checkpointRequest.ReqType == ECheckpointRequestType::CreateWithoutData;
}

void TCheckpointStore::SetCheckpointRequestFinished(
    ui64 requestId,
    bool completed,
    TString shadowDiskId,
    EShadowDiskState shadowDiskState)
{
    Y_DEBUG_ABORT_UNLESS(CheckpointRequestInProgress == requestId);
    if (requestId == CheckpointRequestInProgress) {
        auto& checkpointRequest = GetRequest(requestId);
        Y_DEBUG_ABORT_UNLESS(
            checkpointRequest.State == ECheckpointRequestState::Saved);
        checkpointRequest.State = completed ? ECheckpointRequestState::Completed
                                            : ECheckpointRequestState::Rejected;
        checkpointRequest.ShadowDiskId = std::move(shadowDiskId);
        checkpointRequest.ShadowDiskState = shadowDiskState;
        Apply(checkpointRequest);
    }
    CheckpointRequestInProgress = 0;
    CheckpointBeingCreated = false;
}

void TCheckpointStore::SetShadowDiskState(
    const TString& checkpointId,
    EShadowDiskState shadowDiskState,
    ui64 processedBlockCount)
{
    Y_DEBUG_ABORT_UNLESS(shadowDiskState != EShadowDiskState::None);

    if (auto* checkpointData = ActiveCheckpoints.FindPtr(checkpointId)) {
        checkpointData->ShadowDiskState = shadowDiskState;
        checkpointData->ProcessedBlockCount = processedBlockCount;

        if (auto* checkpointRequest =
                CheckpointRequests.FindPtr(checkpointData->RequestId))
        {
            checkpointRequest->ShadowDiskState = shadowDiskState;
            checkpointRequest->ShadowDiskProcessedBlockCount =
                processedBlockCount;
        }
    }
}

void TCheckpointStore::ShadowActorCreated(const TString& checkpointId)
{
    if (auto* checkpointData = ActiveCheckpoints.FindPtr(checkpointId)) {
        checkpointData->HasShadowActor = true;
    }
}

void TCheckpointStore::ShadowActorDestroyed(const TString& checkpointId)
{
    if (auto* checkpointData = ActiveCheckpoints.FindPtr(checkpointId)) {
        checkpointData->HasShadowActor = false;
    }
}

bool TCheckpointStore::HasShadowActor(const TString& checkpointId) const
{
    if (const auto* checkpointData = ActiveCheckpoints.FindPtr(checkpointId)) {
        return checkpointData->HasShadowActor;
    }
    return false;
}

bool TCheckpointStore::IsRequestInProgress() const
{
    return CheckpointRequestInProgress != 0;
}

bool TCheckpointStore::IsCheckpointBeingCreated() const
{
    return CheckpointBeingCreated;
}

bool TCheckpointStore::DoesCheckpointBlockingWritesExist() const
{
    return CheckpointBlockingWritesExists;
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
    const auto* ptr = ActiveCheckpoints.FindPtr(checkpointId);
    return ptr ? ptr->Type : std::optional<ECheckpointType>{};
}

std::optional<TActiveCheckpointInfo> TCheckpointStore::GetCheckpoint(
    const TString& checkpointId) const
{
    const auto* ptr = ActiveCheckpoints.FindPtr(checkpointId);
    return ptr ? *ptr : std::optional<TActiveCheckpointInfo>{};
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
        TActiveCheckpointInfo{
            .RequestId = checkpointRequest.RequestId,
            .CheckpointId = checkpointRequest.CheckpointId,
            .Type = checkpointRequest.Type,
            .Data = checkpointData,
            .ShadowDiskId = checkpointRequest.ShadowDiskId,
            .ShadowDiskState = checkpointRequest.ShadowDiskState,
            .ProcessedBlockCount =
                checkpointRequest.ShadowDiskProcessedBlockCount});
    CalcCheckpointsState();
}

void TCheckpointStore::DeleteCheckpointData(const TString& checkpointId)
{
    if (auto* checkpointData = ActiveCheckpoints.FindPtr(checkpointId)) {
        checkpointData->Data = ECheckpointData::DataDeleted;
    }
    CalcCheckpointsState();
}

void TCheckpointStore::DeleteCheckpoint(const TString& checkpointId)
{
    ActiveCheckpoints.erase(checkpointId);
    CalcCheckpointsState();
}

void TCheckpointStore::CalcCheckpointsState()
{
    CheckpointBlockingWritesExists = AnyOf(
        ActiveCheckpoints,
        [](const auto& it) { return it.second.ShouldBlockWrites(); });
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
    }
}

}   // namespace NCloud::NBlockStore::NStorage
