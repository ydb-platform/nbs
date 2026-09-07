#pragma once

#include "public.h"

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>

#include <functional>
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
    ui64 OwnerGeneration = 0;
};

struct TSubSessionUpdateResult
{
    std::optional<NActors::TActorId> StalePipeServer;
    std::optional<NActors::TActorId> StaleOwner;
};

struct TDeleteSubSessionResult
{
    // The subsession that was removed, if any matched.
    std::optional<TSubSession> Removed;

    // True if nothing else holds the session after this removal - the
    // caller should destroy the whole session instead of just this
    // subsession.
    bool SessionCanBeDestroyed = false;
};

////////////////////////////////////////////////////////////////////////////////

ui64 MakeSubSessionOwnerGeneration(
    ui32 tabletGeneration,
    ui32 ownerGeneration);

ui32 ExtractTabletGeneration(ui64 ownerGeneration);
ui32 ExtractSubSessionOwnerGeneration(ui64 ownerGeneration);

////////////////////////////////////////////////////////////////////////////////

class TSubSessions
{
    TVector<TSubSession> SubSessions;
    ui64 MaxSeenSeqNo = 0;
    ui64 MaxSeenRwSeqNo = 0;

    TDeleteSubSessionResult DeleteSubSessionIf(
        const std::function<bool(const TSubSession&)>& predicate);

public:
    explicit TSubSessions(ui64 maxSeenSeqNo, ui64 maxSeenRwSeqNo)
        : MaxSeenSeqNo(maxSeenSeqNo)
        , MaxSeenRwSeqNo(maxSeenRwSeqNo)
    {}

    TSubSessionUpdateResult AddSubSession(
        ui64 seqNo,
        bool readOnly,
        const NActors::TActorId& owner,
        const NActors::TActorId& pipeServer,
        ui32 tabletGeneration);

    TSubSessionUpdateResult UpdateSubSession(
        ui64 seqNo,
        bool readOnly,
        const NActors::TActorId& owner,
        const NActors::TActorId& pipeServer,
        ui32 tabletGeneration);

    TDeleteSubSessionResult DeleteSubSessionByPipeServer(
        const NActors::TActorId& pipeServer);
    TDeleteSubSessionResult DeleteSubSession(ui64 sessionSeqNo);

    TVector<NActors::TActorId> GetSubSessionOwnerIds() const;
    TVector<NActors::TActorId> GetSubSessionPipeServerIds() const;
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
