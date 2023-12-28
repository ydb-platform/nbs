#pragma once

#include <util/datetime/base.h>
#include <util/generic/map.h>
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

// The type of checkpoint. See documentation in doc/checkpoint.md.
enum class ECheckpointType
{
    Normal,
    Light,
};

// The logical state of the checkpoint data. It is not persistently saved.
enum class ECheckpointData
{
    DataPresent,     // The checkpoint has data and it can be read
    DataDeleted,     // The checkpoint has no data
    DataPreparing,   // The checkpoint has data, but it is not ready yet and
                     // cannot be read
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

    TCheckpointRequest(
            ui64 requestId,
            TString checkpointId,
            TInstant timestamp,
            ECheckpointRequestType reqType,
            ECheckpointRequestState state,
            ECheckpointType type,
            TString shadowDiskId)
        : RequestId(requestId)
        , CheckpointId(std::move(checkpointId))
        , Timestamp(timestamp)
        , ReqType(reqType)
        , State(state)
        , Type(type)
        , ShadowDiskId(std::move(shadowDiskId))
    {}
};

struct TActiveCheckpointsType
{
    ECheckpointType Type;
    ECheckpointData Data;
    TString ShadowDiskId;
};

using TActiveCheckpointsMap = TMap<TString, TActiveCheckpointsType>;

class TCheckpointStore
{
private:
    TMap<ui64, TCheckpointRequest> CheckpointRequests;
    TActiveCheckpointsMap ActiveCheckpoints;
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

    void SetCheckpointRequestSaved(ui64 requestId);
    void SetCheckpointRequestInProgress(ui64 requestId);
    void SetCheckpointRequestFinished(
        ui64 requestId,
        bool success,
        TString shadowDiskId);

    [[nodiscard]] bool IsRequestInProgress() const;
    [[nodiscard]] bool IsCheckpointBeingCreated() const;
    [[nodiscard]] bool DoesCheckpointBlockingWritesExist() const;
    [[nodiscard]] bool DoesCheckpointHaveData(
        const TString& checkpointId) const;

    [[nodiscard]] std::optional<ECheckpointType> GetCheckpointType(
        const TString& checkpointId) const;
    [[nodiscard]] TVector<TString> GetLightCheckpoints() const;
    [[nodiscard]] TVector<TString> GetCheckpointsWithData() const;
    [[nodiscard]] const TActiveCheckpointsMap& GetActiveCheckpoints() const;

    [[nodiscard]] bool HasRequestToExecute(ui64* requestId) const;

    [[nodiscard]] const TCheckpointRequest& GetRequestById(
        ui64 requestId) const;

    [[nodiscard]] TVector<TCheckpointRequest> GetCheckpointRequests() const;

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

}   // namespace NCloud::NBlockStore::NStorage
