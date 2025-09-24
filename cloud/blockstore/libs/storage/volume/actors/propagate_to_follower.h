
#pragma once

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/volume/model/follower_disk.h>

#include <cloud/storage/core/libs/common/backoff_delay_provider.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

class TPropagateLinkToFollowerActor final
    : public NActors::TActorBootstrapped<TPropagateLinkToFollowerActor>
{
public:
    enum class EReason
    {
        Creation,      // Need to propagate to follower link creation
        Destruction,   // Need to propagate to follower link destruction
        Completion,    // Need to propagate to follower copy completion
    };

private:
    const TString LogPrefix;
    const TRequestInfoPtr RequestInfo;
    const TLeaderFollowerLink Link;
    const EReason Reason;

    TBackoffDelayProvider DelayProvider{
        TDuration::Seconds(1),
        TDuration::Seconds(15)};
    size_t TryCount = 0;

public:
    TPropagateLinkToFollowerActor(
        TString logPrefix,
        TRequestInfoPtr requestInfo,
        TLeaderFollowerLink link,
        EReason reason);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void PersistOnFollower(const NActors::TActorContext& ctx);

    void HandlePersistedOnFollower(
        const TEvVolume::TEvUpdateLinkOnFollowerResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void ReplyAndDie(
        const NActors::TActorContext& ctx,
        const NProto::TError& error);

private:
    STFUNC(StateWork);
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
