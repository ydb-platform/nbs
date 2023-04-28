#pragma once

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class ECheckpointRequestState
{
    New = 0,
    Completed = 1,
    Rejected = 2,
};

enum class ECheckpointRequestType
{
    Create = 0,
    Delete = 1,
    DeleteData = 2,
};

struct TCheckpointRequest
{
    ui64 RequestId;
    TString CheckpointId;
    TInstant Timestamp;
    ECheckpointRequestType ReqType;
    ECheckpointRequestState State;

    TCheckpointRequest(
        ui64 requestId,
        TString checkpointId,
        TInstant timestamp,
        ECheckpointRequestType reqType,
        ECheckpointRequestState state)
        : RequestId(requestId)
        , CheckpointId(std::move(checkpointId))
        , Timestamp(timestamp)
        , ReqType(reqType)
        , State(state)
    {}
};

class TCheckpointStore
{
private:
    THashMap<TString, bool> Checkpoints;

public:
    void Add(const TString& checkpointId);

    void Delete(const TString& checkpointId);
    void DeleteData(const TString& checkpointId);

    bool Empty() const;
    size_t Size() const;

    TVector<TString> Get() const;
    TString GetLast() const;

    const THashMap<TString, bool>& GetAll() const;
};

}   // namespace NCloud::NBlockStore::NStorage
