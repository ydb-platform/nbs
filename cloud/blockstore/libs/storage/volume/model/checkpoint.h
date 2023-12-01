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

enum class ECheckpointRequestType
{
    Create = 0,
    Delete = 1,
    DeleteData = 2,
    CreateWithoutData = 3,
};

enum class ECheckpointType
{
    Normal,
    Light,
};

enum class ECheckpointData
{
    DataPresent,
    DataDeleted
};

struct TCheckpointRequest
{
    ui64 RequestId;
    TString CheckpointId;
    TInstant Timestamp;
    ECheckpointRequestType ReqType;
    ECheckpointRequestState State;
    ECheckpointType Type;

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
};

struct TActiveCheckpointsType
{
    ECheckpointType Type;
    ECheckpointData Data;
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
    bool CheckpointWithDataExists = false;
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
    void SetCheckpointRequestFinished(ui64 requestId, bool success);

    [[nodiscard]] bool IsRequestInProgress() const;
    [[nodiscard]] bool IsCheckpointBeingCreated() const;
    [[nodiscard]] bool DoesCheckpointWithDataExist() const;
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
    void CalcDoesCheckpointWithDataExist();
    void Apply(const TCheckpointRequest& checkpointRequest);
};

}   // namespace NCloud::NBlockStore::NStorage
