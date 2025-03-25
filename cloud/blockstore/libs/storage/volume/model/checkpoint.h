#pragma once

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/error.pb.h>

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>
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
    TString CheckpointError;

    TCheckpointRequest(
            ui64 requestId,
            TString checkpointId,
            TInstant timestamp,
            ECheckpointRequestType reqType,
            ECheckpointRequestState state,
            ECheckpointType type,
            bool useShadowDisk)
        : RequestId(requestId)
        , CheckpointId(std::move(checkpointId))
        , Timestamp(timestamp)
        , ReqType(reqType)
        , State(state)
        , Type(type)
        , ShadowDiskState(
              useShadowDisk ? EShadowDiskState::New : EShadowDiskState::None)
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
            ui64 shadowDiskProcessedBlockCount,
            TString checkpointError)
        : RequestId(requestId)
        , CheckpointId(std::move(checkpointId))
        , Timestamp(timestamp)
        , ReqType(reqType)
        , State(state)
        , Type(type)
        , ShadowDiskId(std::move(shadowDiskId))
        , ShadowDiskState(shadowDiskState)
        , ShadowDiskProcessedBlockCount(shadowDiskProcessedBlockCount)
        , CheckpointError(std::move(checkpointError))
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
    bool HasShadowActor = false;

    [[nodiscard]] bool IsShadowDiskBased() const;

    // Does the checkpoint block write/zero requests.
    [[nodiscard]] bool ShouldBlockWrites() const;
};

using TActiveCheckpointsMap = TMap<TString, TActiveCheckpointInfo>;

class TCheckpointStore
{
private:
    TMap<ui64, TCheckpointRequest> CheckpointRequests;
    TActiveCheckpointsMap ActiveCheckpoints;
    THashSet<TString> DeletedCheckpoints;
    ui64 LastCheckpointRequestId = 0;
    ui64 CheckpointRequestInProgress = 0;
    bool CheckpointBeingCreated = false;
    bool CheckpointBlockingWritesBeingCreated = false;
    bool CheckpointBlockingWritesExists = false;
    const TString DiskID;

public:
    TCheckpointStore(
        TVector<TCheckpointRequest> checkpointRequests,
        const TString& diskID);
    ~TCheckpointStore() = default;

    const TCheckpointRequest& MakeCreateCheckpointRequest(
        TString checkpointId,
        TInstant timestamp,
        ECheckpointRequestType reqType,
        ECheckpointType type,
        bool useShadowDisk);
    const TCheckpointRequest& MakeDeleteCheckpointRequest(
        const TString& checkpointId,
        TInstant timestamp);
    const TCheckpointRequest& MakeDeleteCheckpointDataRequest(
        const TString& checkpointId,
        TInstant timestamp);

    [[nodiscard]] std::optional<NProto::TError> ValidateCheckpointRequest(
        const TString& checkpointId,
        ECheckpointRequestType requestType,
        ECheckpointType checkpointType) const;

    void RemoveCheckpointRequest(ui64 requestId);

    void SetCheckpointRequestInProgress(ui64 requestId);
    void SetCheckpointRequestSaved(ui64 requestId);
    void SetCheckpointRequestFinished(
        ui64 requestId,
        bool completed,
        TString shadowDiskId,
        EShadowDiskState shadowDiskState);

    void SetShadowDiskState(
        const TString& checkpointId,
        EShadowDiskState shadowDiskState,
        ui64 processedBlockCount);
    void ShadowActorCreated(const TString& checkpointId);
    void ShadowActorDestroyed(const TString& checkpointId);
    [[nodiscard]] bool HasShadowActor(const TString& checkpointId) const;
    [[nodiscard]] bool NeedShadowActor(const TString& checkpointId) const;

    [[nodiscard]] bool IsRequestInProgress() const;
    [[nodiscard]] bool IsCheckpointBeingCreated() const;
    [[nodiscard]] bool DoesCheckpointBlockingWritesExist() const;

    [[nodiscard]] bool IsCheckpointDeleted(const TString& checkpointId) const;
    [[nodiscard]] bool DoesCheckpointHaveData(
        const TString& checkpointId) const;

    [[nodiscard]] std::optional<ECheckpointType> GetCheckpointType(
        const TString& checkpointId) const;

    [[nodiscard]] std::optional<TActiveCheckpointInfo> GetCheckpoint(
        const TString& checkpointId) const;

    [[nodiscard]] TVector<TString> GetLightCheckpoints() const;
    [[nodiscard]] TVector<TString> GetCheckpointsWithData() const;
    [[nodiscard]] const TActiveCheckpointsMap& GetActiveCheckpoints() const;

    [[nodiscard]] TVector<ui64> GetRequestIdsToProcess() const;

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
    [[nodiscard]] TInstant GetCorrectedTimestamp(TInstant timestamp) const;
};

}   // namespace NCloud::NBlockStore::NStorage
