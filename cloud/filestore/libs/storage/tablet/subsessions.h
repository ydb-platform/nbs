#pragma once

#include "public.h"

#include <ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>

#include <optional>


namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TSubSession
{
    ui64 SeqNo;
    bool ReadOnly;
    NActors::TActorId Owner;
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

    NActors::TActorId AddSubSession(
        ui64 seqNo,
        bool readOnly,
        const NActors::TActorId& owner);

    NActors::TActorId UpdateSubSession(
        ui64 seqNo,
        bool readOnly,
        const NActors::TActorId& owner);

    ui32 DeleteSubSession(const NActors::TActorId& owner);
    ui32 DeleteSubSession(ui64 sessionSeqNo);

    TVector<NActors::TActorId> GetSubSessions() const;
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
