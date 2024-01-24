#pragma once

#include <util/datetime/base.h>
#include <util/generic/map.h>
#include <util/generic/queue.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Stages: Received -> Saved -> Completed/Rejected
enum class ECheckpointRequestState
{
    Saved = 0,       // New request saved to database.
    Completed = 1,   // Request completed successfully.
    Rejected = 2,    // Request failed to execute.
    Received = 4,    // Request received from the client. Not saved to database.
};

// The type of checkpoint management request.
enum class ECheckpointRequestType
{
    Create = 0,
    Delete = 1,
    DeleteData = 2,
    CreateWithoutData = 3,
};

// Request status that determines either the request should be executed or return immediately
enum class ECheckpointRequestValidityStatus
{
    Ok = 0, // Request should be executed
    Already = 1, // Nothing to do, request should finish successfully without execution
    Invalid = 2, // Request is invalid, request should finish with error without execution
};

// The type of checkpoint. See documentation in doc/checkpoint.md.
enum class ECheckpointType
{
    Normal,
    Light,
};

// The logical state of the checkpoint data. It is not persistently saved.
enum class ECheckpointData
{
    DataPresent,     // The checkpoint has data and it can be read (after the
                     // shadow disk is prepared)
    DataDeleted,     // The checkpoint has no data
};

// State of the shadow disk. It is persistently saved.
enum class EShadowDiskState
{
    None = 0,
    New = 1,
    Preparing = 2,
    Ready = 3,
    Error = 4,
};

struct TCheckpointRequest
{
    ui64 RequestId;
    TString CheckpointId;
    TInstant Timestamp;
    ECheckpointRequestType ReqType;
    ECheckpointRequestState State;
    ECheckpointType Type;

    TString ShadowDiskId;
    EShadowDiskState ShadowDiskState = EShadowDiskState::None;
    ui64 ShadowDiskProcessedBlockCount = 0;

    TCheckpointRequest(
            ui64 requestId,
            TString checkpointId,
            TInstant timestamp,
            ECheckpointRequestType reqType,
            ECheckpointRequestState state,
            ECheckpointType type)
        : RequestId(requestId)
        , CheckpointId(std::move(checkpointId))
        , Timestamp(timestamp)
        , ReqType(reqType)
        , State(state)
        , Type(type)
    {}

    TCheckpointRequest(
            ui64 requestId,
            TString checkpointId,
            TInstant timestamp,
            ECheckpointRequestType reqType,
            ECheckpointRequestState state,
            ECheckpointType type,
            TString shadowDiskId,
            EShadowDiskState shadowDiskState,
            ui64 shadowDiskProcessedBlockCount)
        : RequestId(requestId)
        , CheckpointId(std::move(checkpointId))
        , Timestamp(timestamp)
        , ReqType(reqType)
        , State(state)
        , Type(type)
        , ShadowDiskId(std::move(shadowDiskId))
        , ShadowDiskState(shadowDiskState)
        , ShadowDiskProcessedBlockCount(shadowDiskProcessedBlockCount)
    {}
};

struct TActiveCheckpointInfo
{
    ui64 RequestId = 0;
    TString CheckpointId;
    ECheckpointType Type;
    ECheckpointData Data;
    TString ShadowDiskId;
    EShadowDiskState ShadowDiskState = EShadowDiskState::None;
    ui64 ProcessedBlockCount = 0;
    ui64 TotalBlockCount = 0;
    bool HasShadowActor = false;
};

using TActiveCheckpointsMap = TMap<TString, TActiveCheckpointInfo>;

class TCheckpointStore
{
private:
    TMap<ui64, TCheckpointRequest> CheckpointRequests;
    TActiveCheckpointsMap ActiveCheckpoints;
    TSet<TString> DeletedCheckpoints;
    TSet<TString> CheckpointsWithSavedRequest;
    ui64 LastCheckpointRequestId = 0;
    ui64 CheckpointRequestInProgress = 0;
    bool CheckpointBeingCreated = false;
    bool CheckpointBlockingWritesExists = false;
    const TString DiskID;

public:
    TCheckpointStore(
        TVector<TCheckpointRequest> checkpointRequests,
        const TString& diskID);
    ~TCheckpointStore() = default;

    const TCheckpointRequest& CreateNew(
        TString checkpointId,
        TInstant timestamp,
        ECheckpointRequestType reqType,
        ECheckpointType type);

    [[nodiscard]] ECheckpointRequestValidityStatus ValidateCheckpointRequest(
        const TString& checkpointId,
        ECheckpointRequestType requestType,
        ECheckpointType checkpointType,
        TString* message);

    void SetCheckpointRequestSaved(ui64 requestId);
    void SetCheckpointRequestInProgress(ui64 requestId);
    void SetCheckpointRequestFinished(
        ui64 requestId,
        bool success,
        TString shadowDiskId,
        EShadowDiskState shadowDiskState);

    void SetShadowDiskState(
        const TString& checkpointId,
        EShadowDiskState shadowDiskState,
        ui64 processedBlockCount,
        ui64 totalBlockCount);
    void ShadowActorCreated(const TString& checkpointId);
    void ShadowActorDestroyed(const TString& checkpointId);
    [[nodiscard]] bool HasShadowActor(const TString& checkpointId) const;

    [[nodiscard]] bool IsRequestInProgress() const;
    [[nodiscard]] bool IsCheckpointBeingCreated() const;
    [[nodiscard]] bool DoesCheckpointBlockingWritesExist() const;
    [[nodiscard]] bool DoesCheckpointHaveData(
        const TString& checkpointId) const;

    [[nodiscard]] std::optional<ECheckpointType> GetCheckpointType(
        const TString& checkpointId) const;

    [[nodiscard]] std::optional<TActiveCheckpointInfo> GetCheckpoint(
        const TString& checkpointId) const;

    [[nodiscard]] bool IsCheckpointDeleted(const TString& checkpointId) const;
    [[nodiscard]] bool IsCheckpointInSavedStatusExist(const TString& checkpointId) const;
    [[nodiscard]] TVector<TString> GetLightCheckpoints() const;
    [[nodiscard]] TVector<TString> GetCheckpointsWithData() const;
    [[nodiscard]] const TActiveCheckpointsMap& GetActiveCheckpoints() const;

    [[nodiscard]] bool HasRequestToExecute(ui64* requestId) const;

    [[nodiscard]] const TCheckpointRequest& GetRequestById(
        ui64 requestId) const;

    [[nodiscard]] TVector<TCheckpointRequest> GetCheckpointRequests() const;

    TSet<TString>& GetCheckpoitsWithSavedRequest();
    const TSet<TString>& GetCheckpoitsWithSavedRequest() const;

private:
    [[nodiscard]] TCheckpointRequest& GetRequest(ui64 requestId);
    TCheckpointRequest& AddCheckpointRequest(
        TCheckpointRequest checkpointRequest);
    void AddCheckpoint(
        const TCheckpointRequest& checkpointRequest,
        bool forceDataDeleted);
    void DeleteCheckpoint(const TString& checkpointId);
    void DeleteCheckpointData(const TString& checkpointId);
    void CalcCheckpointsState();
    void Apply(const TCheckpointRequest& checkpointRequest);
};

////////////////////////////////////////////////////////////////////////////////

template<typename TCheckpointRequestInfo>
struct TPostponedCheckpointRequest {
    ui64 RequestId;
    TCheckpointRequestInfo Info;

    TPostponedCheckpointRequest(
            ui64 requestId,
            TCheckpointRequestInfo info)
        : RequestId(requestId)
        , Info(std::move(info))
    {}
};

template<typename TCheckpointRequestInfo>
class TPostponedCheckpointRequestsQueue
{
private:
    using TRequest = TPostponedCheckpointRequest<TCheckpointRequestInfo>;
    TMap<TString, TQueue<TRequest>> Requests;

public:
    bool HasPostponedRequest(const TString& checkpointId) const
    {
        return Requests.contains(checkpointId);
    }

    void AddPostponedRequest(TString checkpointId, TRequest request)
    {
        Requests[checkpointId].push(std::move(request));
    }

    std::optional<TRequest> TakePostponedRequest(TString checkpointId)
    {
        auto queue = Requests.FindPtr(checkpointId);
        if (!queue) {
            return std::nullopt;
        }

        auto request = std::move(queue->front());
        queue->pop();
        if (queue->empty()) {
            Requests.erase(checkpointId);
        }

        return request;
    }
};

}   // namespace NCloud::NBlockStore::NStorage
