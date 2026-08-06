#pragma once

#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/model/log_title.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Periodically pings the base-disk volume through VolumeProxy with a
// TEvKeepAliveRequest, so the tablet pipe established for the base disk
// is not closed by VolumeProxy's inactivity timeout. The first ping is sent
// immediately on Bootstrap, the rest are driven by a self-sustaining timer
// that re-arms on every wakeup with KeepAliveInterval, independently of when
// the previous ping is answered.
//
// Pings are best-effort: errors are ignored except for logging. At most one
// ping is tracked as in flight at any time. If a wakeup fires while a ping is
// still outstanding, two cases are distinguished by how long it has been
// outstanding:
//   * shorter than KeepAliveInterval - the new ping is skipped (the response is
//     still expected shortly);
//   * at least KeepAliveInterval - the previous ping is considered lost (the
//     response was dropped, e.g. on a pipe reset) and a fresh ping is sent so
//     the keep-alive can never get permanently stuck waiting for a response
//     that will never arrive.
class TBaseDiskKeepAliveActor final
    : public NActors::TActorBootstrapped<TBaseDiskKeepAliveActor>
{
private:
    const TString BaseDiskId;
    const TDuration KeepAliveInterval;
    const TChildLogTitle LogTitle;

    bool PingInFlight = false;
    TInstant LastPingSentAt;

public:
    TBaseDiskKeepAliveActor(
        TString baseDiskId,
        TDuration keepAliveInterval,
        TChildLogTitle logTitle);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void SendPing(const NActors::TActorContext& ctx);
    void SchedulePing(const NActors::TActorContext& ctx);

    STFUNC(StateWork);

    void HandleKeepAliveResponse(
        const TEvVolumeProxy::TEvKeepAliveResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWakeup(
        const NActors::TEvents::TEvWakeup::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
