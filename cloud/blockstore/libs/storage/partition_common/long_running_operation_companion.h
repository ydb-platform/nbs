#pragma once

#include "events_private.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TRunningActors
{
public:
    using EOperation =
        TEvPartitionCommonPrivate::TEvLongRunningOperation::EOperation;
    using TTimeoutsStat = std::array<size_t, EOperation::Count>;

private:
    struct TActorInfo
    {
        EOperation Operation = EOperation::DontCare;
        bool LongRunning = false;
    };

    THashMap<NActors::TActorId, TActorInfo> Actors;
    TTimeoutsStat StatForFinishedActors{};

public:
    void Insert(const NActors::TActorId& actorId);
    void Erase(const NActors::TActorId& actorId);
    void MarkLongRunning(
        const NActors::TActorId& actorId,
        EOperation operation);

    TVector<NActors::TActorId> GetActors() const;
    TTimeoutsStat ExtractLongRunningStat();
};

////////////////////////////////////////////////////////////////////////////////

class TLongRunningOperationCompanion
{
public:
    using EOperation =
        TEvPartitionCommonPrivate::TEvLongRunningOperation::EOperation;

private:
    const NActors::TActorId ParentActor;
    const TDuration LongRunningThreshold;
    const EOperation Operation;
    const ui32 GroupId;

    bool LongRunningDetected = false;

public:
    TLongRunningOperationCompanion(
        NActors::TActorId parentActor,
        TDuration longRunningThreshold,
        EOperation operation,
        ui32 groupId);

    void RequestStarted(const NActors::TActorContext& ctx);
    void RequestFinished(const NActors::TActorContext& ctx);

    void HandleTimeout(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
