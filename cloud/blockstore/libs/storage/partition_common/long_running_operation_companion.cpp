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
        NActors::TActorId partitionActor,
        NActors::TActorId volumeActor,
        TDuration longRunningThreshold,
        TLongRunningOperationCompanion::EOperation operation,
        ui32 groupId)
    : PartitionActor(partitionActor)
    , VolumeActor(volumeActor)
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

void TLongRunningOperationCompanion::RequestFinished(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    using TEvLongRunningOperation =
        TEvPartitionCommonPrivate::TEvLongRunningOperation;

    if (!LongRunningDetected && !HasError(error)) {
        return;
    }

    auto makeMessage = [&]()
    {
        return std::make_unique<TEvLongRunningOperation>(
            Operation,
            true,
            ctx.Now() - StartAt,
            GroupId,
            HasError(error) ? TEvLongRunningOperation::EReason::Cancelled
                            : TEvLongRunningOperation::EReason::FinishedOk,
            error);
    };
    NCloud::Send(ctx, PartitionActor, makeMessage());
    NCloud::Send(ctx, VolumeActor, makeMessage());
}

void TLongRunningOperationCompanion::HandleTimeout(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    using TEvLongRunningOperation =
        TEvPartitionCommonPrivate::TEvLongRunningOperation;
    Y_UNUSED(ev);

    LongRunningDetected = true;
    PingCount++;

    auto makeMessage = [&]()
    {
        return std::make_unique<TEvLongRunningOperation>(
            Operation,
            PingCount == 1,
            ctx.Now() - StartAt,
            GroupId,
            TEvLongRunningOperation::EReason::LongRunningDetected,
            MakeError(S_OK)   // doesn't matter, it won't be used
        );
    };
    NCloud::Send(ctx, PartitionActor, makeMessage());
    NCloud::Send(ctx, VolumeActor, makeMessage());

    PingDelayProvider.IncreaseDelay();
    ctx.Schedule(PingDelayProvider.GetDelay(), new TEvents::TEvWakeup());
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
