#pragma once

#include "public.h"

#include <contrib/ydb/library/actors/core/actorid.h>

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
    ui64 MaxRoSeqNo = 0; // ro sequence number (set during vm migration)
    ui64 MaxRwSeqNo = 0; // rw sequence number

public:
    explicit TSubSessions(ui64 maxRoSeqNo, ui64 maxRwSeqNo)
        : MaxRoSeqNo(maxRoSeqNo)
        , MaxRwSeqNo(maxRwSeqNo)
    {}

    NActors::TActorId AddSubSession(
        ui64 seqNo,
        bool readOnly,
        const NActors::TActorId& owner);

    NActors::TActorId UpdateSubSession(
        ui64 seqNo,
        bool readOnly,
        const NActors::TActorId& owner);

    bool DeleteSubSession(const NActors::TActorId& owner);
    bool DeleteSubSession(ui64 sessionSeqNo);
    void UpdateSeqNoAfterDelete(ui64 seqNo);

    TVector<NActors::TActorId> GetSubSessions() const;
    TVector<TSubSession> GetAllSubSessions() const;

    bool HasSeqNo(ui64 seqNo) const;

    bool IsValid() const;

    ui32 GetSize() const
    {
        return SubSessions.size();
    }

    ui64 GetMaxRoSeqNo() const
    {
        return MaxRoSeqNo;
    }

    ui64 GetMaxRwSeqNo() const
    {
        return MaxRwSeqNo;
    }

    std::optional<TSubSession> GetSubSessionBySeqNo(ui64 seqNo) const;
};

}   // namespace NCloud::NFileStore::NStorage
