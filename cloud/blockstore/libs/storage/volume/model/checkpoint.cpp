#include "checkpoint.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

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

const TCheckpointRequest& TCheckpointStore::MakeCreateCheckpointRequest(
    TString checkpointId,
    TInstant timestamp,
    ECheckpointRequestType reqType,
    ECheckpointType type,
    bool useShadowDisk)
{
    return AddCheckpointRequest(TCheckpointRequest(
        ++LastCheckpointRequestId,
        std::move(checkpointId),
        GetCorrectedTimestamp(timestamp),
        reqType,
        ECheckpointRequestState::Received,
        type,
        useShadowDisk));
}

const TCheckpointRequest& TCheckpointStore::MakeDeleteCheckpointRequest(
    const TString& checkpointId,
    TInstant timestamp)
{
    auto checkpointInfo = GetCheckpoint(checkpointId);

    return AddCheckpointRequest(TCheckpointRequest(
        ++LastCheckpointRequestId,
        checkpointId,
        GetCorrectedTimestamp(timestamp),
        ECheckpointRequestType::Delete,
        ECheckpointRequestState::Received,
        checkpointInfo ? checkpointInfo->Type : ECheckpointType::Normal,
        checkpointInfo ? checkpointInfo->IsShadowDiskBased() : false));
}

const TCheckpointRequest& TCheckpointStore::MakeDeleteCheckpointDataRequest(
    const TString& checkpointId,
    TInstant timestamp)
{
    auto checkpointInfo = GetCheckpoint(checkpointId);

    return AddCheckpointRequest(TCheckpointRequest(
        ++LastCheckpointRequestId,
        checkpointId,
        GetCorrectedTimestamp(timestamp),
        ECheckpointRequestType::DeleteData,
        ECheckpointRequestState::Received,
        checkpointInfo ? checkpointInfo->Type : ECheckpointType::Normal,
        checkpointInfo ? checkpointInfo->IsShadowDiskBased() : false));
}

void TCheckpointStore::RemoveCheckpointRequest(ui64 requestId)
{
    Y_DEBUG_ABORT_UNLESS(
        CheckpointRequests.FindPtr(requestId) &&
        CheckpointRequests.FindPtr(requestId)->State ==
            ECheckpointRequestState::Received);
    CheckpointRequests.erase(requestId);
}

void TCheckpointStore::SetCheckpointRequestInProgress(ui64 requestId)
{
    auto& checkpointRequest = GetRequest(requestId);
    Y_DEBUG_ABORT_UNLESS(
        checkpointRequest.State == ECheckpointRequestState::Received ||
        checkpointRequest.State == ECheckpointRequestState::Saved);

    CheckpointRequestInProgress = requestId;
    CheckpointBeingCreated =
        checkpointRequest.ReqType == ECheckpointRequestType::Create ||
        checkpointRequest.ReqType == ECheckpointRequestType::CreateWithoutData;

    CheckpointBlockingWritesBeingCreated =
        CheckpointBeingCreated &&
        checkpointRequest.ShadowDiskState == EShadowDiskState::None;
}

void TCheckpointStore::SetCheckpointRequestSaved(ui64 requestId)
{
    auto& checkpointRequest = GetRequest(requestId);
    Y_DEBUG_ABORT_UNLESS(
        checkpointRequest.State == ECheckpointRequestState::Received);
    checkpointRequest.State = ECheckpointRequestState::Saved;
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
        if (shadowDiskState != EShadowDiskState::None || shadowDiskId) {
            Y_DEBUG_ABORT_UNLESS(
                checkpointRequest.ShadowDiskState != EShadowDiskState::None);
        }
        checkpointRequest.State = completed ? ECheckpointRequestState::Completed
                                            : ECheckpointRequestState::Rejected;
        checkpointRequest.ShadowDiskId = std::move(shadowDiskId);
        checkpointRequest.ShadowDiskState = shadowDiskState;
        Apply(checkpointRequest);
    }
    CheckpointRequestInProgress = 0;
    CheckpointBeingCreated = false;
    CheckpointBlockingWritesBeingCreated = false;
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

bool TCheckpointStore::NeedShadowActor(const TString& checkpointId) const
{
    const auto* checkpointData = ActiveCheckpoints.FindPtr(checkpointId);
    if (!checkpointData || checkpointData->Type != ECheckpointType::Normal ||
        checkpointData->Data == ECheckpointData::DataDeleted ||
        !checkpointData->IsShadowDiskBased() ||
        checkpointData->ShadowDiskState == EShadowDiskState::Error)
    {
        return false;
    }

    // Do not need to create the shadow disk actor if the shadow disk is being
    // deleted.
    if (const TCheckpointRequest* checkpointRequest =
            CheckpointRequests.FindPtr(CheckpointRequestInProgress);
        checkpointRequest != nullptr &&
        checkpointRequest->CheckpointId == checkpointId)
    {
        if (checkpointRequest->ReqType == ECheckpointRequestType::Delete ||
            checkpointRequest->ReqType == ECheckpointRequestType::DeleteData)
        {
            return false;
        }
    }

    return true;
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
    return CheckpointBlockingWritesBeingCreated ||
           CheckpointBlockingWritesExists;
}

bool TCheckpointStore::IsCheckpointDeleted(const TString& checkpointId) const
{
    return DeletedCheckpoints.contains(checkpointId);
}

bool TCheckpointStore::DoesCheckpointHaveData(const TString& checkpointId) const
{
    if (const auto* data = ActiveCheckpoints.FindPtr(checkpointId)) {
        return data->Data == ECheckpointData::DataPresent;
    }
    return false;
}

TVector<ui64> TCheckpointStore::GetRequestIdsToProcess() const
{
    TVector<ui64> requestIds(Reserve(CheckpointRequests.size()));

    for (const auto& [key, checkpointRequest]: CheckpointRequests) {
        if (checkpointRequest.State == ECheckpointRequestState::Received ||
            checkpointRequest.State == ECheckpointRequestState::Saved)
        {
            Y_DEBUG_ABORT_UNLESS(key == checkpointRequest.RequestId);
            requestIds.push_back(checkpointRequest.RequestId);
        }
    }

    return requestIds;
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
    DeletedCheckpoints.insert(checkpointId);
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

TInstant TCheckpointStore::GetCorrectedTimestamp(TInstant timestamp) const
{
    if (!CheckpointRequests.empty()) {
        const auto& lastReq = CheckpointRequests.rbegin()->second;
        timestamp =
            Max(timestamp, lastReq.Timestamp + TDuration::MicroSeconds(1));
    }
    return timestamp;
}

std::optional<NProto::TError> TCheckpointStore::ValidateCheckpointRequest(
    const TString& checkpointId,
    ECheckpointRequestType requestType,
    ECheckpointType checkpointType) const
{
    auto makeErrorInvalid = [](TString message)
    {
        ui32 flags = 0;
        SetProtoFlag(flags, NProto::EF_SILENT);
        return MakeError(E_PRECONDITION_FAILED, std::move(message), flags);
    };

    auto makeErrorAlready = [](TString message)
    {
        return MakeError(S_ALREADY, std::move(message));
    };

    if (checkpointId.empty()) {
        return MakeError(E_ARGUMENT, "Checkpoint id should not be empty");
    }

    if (checkpointType != ECheckpointType::Normal &&
        (requestType == ECheckpointRequestType::DeleteData ||
         requestType == ECheckpointRequestType::CreateWithoutData))
    {
        TString message = TStringBuilder()
                          << requestType
                          << " request makes sense for normal checkpoints only";
        return MakeError(E_ARGUMENT, std::move(message));
    }

    const auto actualCheckpointType = GetCheckpointType(checkpointId);

    if (actualCheckpointType && actualCheckpointType != checkpointType) {
        TString message = TStringBuilder()
                          << "Checkpoint exists and has another type "
                          << *actualCheckpointType;
        return makeErrorInvalid(std::move(message));
    }

    const bool checkpointExists = actualCheckpointType.has_value();
    const bool checkpointDataPresent =
        checkpointExists && DoesCheckpointHaveData(checkpointId);
    const bool checkpointDeleted = IsCheckpointDeleted(checkpointId);

    if (!checkpointExists && !checkpointDeleted) {
        // Checkpoint never existed.
        switch (requestType) {
            case ECheckpointRequestType::Create:
            case ECheckpointRequestType::CreateWithoutData: {
                return std::nullopt;
            }
            case ECheckpointRequestType::DeleteData:
                return makeErrorInvalid("Checkpoint does not exist");
            case ECheckpointRequestType::Delete: {
                return makeErrorAlready("Checkpoint does not exist");
            }
        }
    }
    if (checkpointExists && checkpointDataPresent) {
        // Checkpoint exists and has data.
        switch (requestType) {
            case ECheckpointRequestType::Create: {
                return makeErrorAlready("Checkpoint exists");
            }
            case ECheckpointRequestType::DeleteData:
            case ECheckpointRequestType::Delete: {
                return std::nullopt;
            }
            case ECheckpointRequestType::CreateWithoutData: {
                return makeErrorInvalid("Checkpoint exists and has data");
            }
        }
    }
    if (checkpointExists && !checkpointDataPresent) {
        // Checkpoint exists and has no data.
        switch (requestType) {
            case ECheckpointRequestType::Create: {
                if (checkpointType == ECheckpointType::Normal) {
                    return makeErrorInvalid(
                        "Checkpoint exists and has no data");
                }
                return makeErrorAlready("Checkpoint exists");
            }
            case ECheckpointRequestType::DeleteData: {
                return makeErrorAlready("Data is already deleted");
            }
            case ECheckpointRequestType::Delete: {
                return std::nullopt;
            }
            case ECheckpointRequestType::CreateWithoutData: {
                return makeErrorAlready("Checkpoint exists");
            }
        }
    }
    if (!checkpointExists && checkpointDeleted) {
        // Checkpoint was deleted.
        switch (requestType) {
            case ECheckpointRequestType::Create:
            case ECheckpointRequestType::CreateWithoutData:
            case ECheckpointRequestType::DeleteData: {
                return makeErrorInvalid("Checkpoint is already deleted");
            }
            case ECheckpointRequestType::Delete: {
                return makeErrorAlready("Checkpoint is already deleted");
            }
        }
    }
    STORAGE_CHECK_PRECONDITION(false);
    return MakeError(
        E_INVALID_STATE,
        "Checkpoint validation should never reach this line");
}

}   // namespace NCloud::NBlockStore::NStorage
