#include "long_running_operation_companion.h"

#include <cloud/storage/core/libs/actors/helpers.h>

#include <util/string/cast.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TRunningActors::Insert(const TActorId& actorId)
{
    Actors.insert(std::make_pair(actorId, TActorInfo{}));
}

void TRunningActors::Erase(const TActorId& actorId)
{
    if (auto* actorInfo = Actors.FindPtr(actorId)) {
        if (actorInfo->LongRunning) {
            StatForFinishedActors[actorInfo->Operation]++;
        }
        Actors.erase(actorId);
    }
}

void TRunningActors::MarkLongRunning(
    const NActors::TActorId& actorId,
    TRunningActors::EOperation operation)
{
    if (auto* actorInfo = Actors.FindPtr(actorId)) {
        actorInfo->Operation = operation;
        actorInfo->LongRunning = true;
    }
}

TVector<NActors::TActorId> TRunningActors::GetActors() const
{
    TVector<NActors::TActorId> result;
    result.reserve(Actors.size());
    for (const auto& [actorId, actorInfo]: Actors) {
        result.push_back(actorId);
    }
    return result;
}

TRunningActors::TTimeoutsStat TRunningActors::ExtractLongRunningStat()
{
    TTimeoutsStat result{};
    result.swap(StatForFinishedActors);
    for (const auto& [actorId, actorInfo]: Actors) {
        if (actorInfo.LongRunning) {
            result[actorInfo.Operation]++;
        }
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TLongRunningOperationCompanion::TLongRunningOperationCompanion(
        NActors::TActorId parentActor,
        TDuration longRunningThreshold,
        TLongRunningOperationCompanion::EOperation operation,
        ui32 groupId)
    : ParentActor(parentActor)
    , LongRunningThreshold(longRunningThreshold)
    , Operation(operation)
    , GroupId(groupId)
{}

void TLongRunningOperationCompanion::RequestStarted(const TActorContext& ctx)
{
    if (LongRunningThreshold && LongRunningThreshold != TDuration::Max()) {
        ctx.Schedule(LongRunningThreshold, new TEvents::TEvWakeup());
    }
}

void TLongRunningOperationCompanion::RequestFinished(const TActorContext& ctx)
{
    if (!LongRunningDetected) {
        return;
    }

    NCloud::Send(
        ctx,
        ParentActor,
        std::make_unique<TEvPartitionCommonPrivate::TEvLongRunningOperation>(
            Operation,
            LongRunningThreshold,
            GroupId,
            true));
}

void TLongRunningOperationCompanion::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    NCloud::Send(
        ctx,
        ParentActor,
        std::make_unique<TEvPartitionCommonPrivate::TEvLongRunningOperation>(
            Operation,
            LongRunningThreshold,
            GroupId,
            false));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
