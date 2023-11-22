#include "long_running_operation_companion.h"

#include <cloud/storage/core/libs/actors/helpers.h>

#include <util/string/cast.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto LongRunningPingMaxDelay = TDuration::Minutes(5);

////////////////////////////////////////////////////////////////////////////////

}   // namespace

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
    , Operation(operation)
    , GroupId(groupId)
    , PingDelayProvider(
          longRunningThreshold,
          Max(longRunningThreshold, LongRunningPingMaxDelay))
{}

void TLongRunningOperationCompanion::RequestStarted(const TActorContext& ctx)
{
    StartAt = ctx.Now();

    auto longRunningThreshold = PingDelayProvider.GetDelay();
    if (longRunningThreshold && longRunningThreshold != TDuration::Max()) {
        ctx.Schedule(longRunningThreshold, new TEvents::TEvWakeup());
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
            true,
            ctx.Now() - StartAt,
            GroupId,
            true));
}

void TLongRunningOperationCompanion::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LongRunningDetected = true;
    PingCount++;

    NCloud::Send(
        ctx,
        ParentActor,
        std::make_unique<TEvPartitionCommonPrivate::TEvLongRunningOperation>(
            Operation,
            PingCount == 1,
            ctx.Now() - StartAt,
            GroupId,
            false));

    PingDelayProvider.IncreaseDelay();
    ctx.Schedule(PingDelayProvider.GetDelay(), new TEvents::TEvWakeup());
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
