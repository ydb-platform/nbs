#include "partition_info.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TActorsStack::Push(NActors::TActorId actorId)
{
    if (!actorId || IsKnown(actorId)) {
        return;
    }

    Actors.push_front(actorId);
}

void TActorsStack::Clear()
{
    Actors.clear();
}

bool TActorsStack::IsKnown(NActors::TActorId actorId) const
{
    return AnyOf(
        Actors,
        [actorId](const NActors::TActorId& actor) { return actor == actorId; });
}

NActors::TActorId TActorsStack::GetTop() const
{
    return Actors.empty() ? NActors::TActorId() : Actors.front();
}

////////////////////////////////////////////////////////////////////////////////

TPartitionInfo::TPartitionInfo(
        ui64 tabletId,
        NProto::TPartitionConfig partitionConfig,
        ui32 partitionIndex,
        TDuration timeoutIncrement,
        TDuration timeoutMax)
    : TabletId(tabletId)
    , PartitionIndex(partitionIndex)
    , PartitionConfig(std::move(partitionConfig))
    , RetryPolicy(timeoutIncrement, timeoutMax)
{}

void TPartitionInfo::Init(const NActors::TActorId& bootstrapper)
{
    Bootstrapper = bootstrapper;
    State = UNKNOWN;
    Message = {};
}

void TPartitionInfo::SetStarted(TActorsStack actors)
{
    RelatedActors = std::move(actors);
    State = STARTED;
    Message = {};
}

void TPartitionInfo::SetReady()
{
    Y_ABORT_UNLESS(State == STARTED);
    State = READY;
}

void TPartitionInfo::SetStopped()
{
    RelatedActors.Clear();
    State = STOPPED;
    Message = {};
}

void TPartitionInfo::SetFailed(TString message)
{
    RelatedActors.Clear();
    State = FAILED;
    Message = std::move(message);
}

NActors::TActorId TPartitionInfo::GetTopActorId() const
{
    return RelatedActors.GetTop();
}

bool TPartitionInfo::IsKnownActorId(const NActors::TActorId actorId) const
{
    return RelatedActors.IsKnown(actorId);
}

TString TPartitionInfo::GetStatus() const
{
    TStringStream out;

    switch (State) {
        default:
        case UNKNOWN:
            out << "UNKNOWN";
            break;
        case STOPPED:
            out << "STOPPED";
            break;
        case STARTED:
            out << "STARTED";
            break;
        case FAILED:
            out << "FAILED";
            break;
        case READY:
            out << "READY";
            break;
    }

    if (Message) {
        out << ": " << Message;
    }

    return out.Str();
}

}   // namespace NCloud::NBlockStore::NStorage
