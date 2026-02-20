#pragma once

#include "public.h"

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>

#include <optional>


namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TSessionPipeInfo
{
    NActors::TActorId Owner;
    NActors::TActorId PipeServer;
};

struct TSubSession
{
    ui64 SeqNo;
    bool ReadOnly;
    TSessionPipeInfo PipeInfo;
};

////////////////////////////////////////////////////////////////////////////////

class TSubSessions
{
    TVector<TSubSession> SubSessions;
    ui64 MaxSeenSeqNo = 0;
    ui64 MaxSeenRwSeqNo = 0;

public:
    explicit TSubSessions(ui64 maxSeenSeqNo, ui64 maxSeenRwSeqNo)
        : MaxSeenSeqNo(maxSeenSeqNo)
        , MaxSeenRwSeqNo(maxSeenRwSeqNo)
    {}

    std::optional<TSessionPipeInfo> AddSubSession(
        ui64 seqNo,
        bool readOnly,
        const NActors::TActorId& owner,
        const NActors::TActorId& pipeServer);

    std::optional<TSessionPipeInfo> UpdateSubSession(
        ui64 seqNo,
        bool readOnly,
        const NActors::TActorId& owner,
        const NActors::TActorId& pipeServer);

    ui32 DeleteSubSessionByPipeServer(const NActors::TActorId& pipeServer);
    std::optional<TSubSession> DeleteSubSession(ui64 sessionSeqNo);

    TVector<NActors::TActorId> GetSubSessionsOwner() const;
    TVector<NActors::TActorId> GetSubSessionsPipeServer() const;
    TVector<TSubSession> GetAllSubSessions() const;

    bool HasSeqNo(ui64 seqNo) const;

    bool IsValid() const;

    ui32 GetSize() const
    {
        return SubSessions.size();
    }

    ui64 GetMaxSeenSeqNo() const
    {
        return MaxSeenSeqNo;
    }

    ui64 GetMaxSeenRwSeqNo() const
    {
        return MaxSeenRwSeqNo;
    }

    std::optional<TSubSession> GetSubSessionBySeqNo(ui64 seqNo) const;

    bool ReadyToDestroy(ui64 seqNo) const;
};

}   // namespace NCloud::NFileStore::NStorage
