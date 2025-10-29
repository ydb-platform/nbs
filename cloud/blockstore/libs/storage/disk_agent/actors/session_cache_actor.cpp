#include "session_cache_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/storage/disk_agent/disk_agent_private.h>
#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/system/fs.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage::NDiskAgent {

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::TError SaveSessionCache(
    const TString& path,
    const TVector<NProto::TDiskAgentDeviceSession>& sessions,
    TInstant deadline)
{
    try {
        NProto::TDiskAgentDeviceSessionCache proto;
        proto.MutableSessions()->Reserve(static_cast<int>(sessions.size()));

        // saving only active sessions
        for (const auto& session: sessions) {
            if (session.GetLastActivityTs() > deadline.MicroSeconds()) {
                *proto.MutableSessions()->Add() = session;
            }
        }

        const TString tmpPath{path + ".tmp"};

        SerializeToTextFormat(proto, tmpPath);

        if (!NFs::Rename(tmpPath, path)) {
            char buf[64] = {};
            const auto ec = errno;

            return MakeError(
                MAKE_SYSTEM_ERROR(ec),
                strerror_r(ec, buf, sizeof(buf)));
        }
    } catch (...) {
        return MakeError(E_FAIL, CurrentExceptionMessage());
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TSessionCacheActor: public TActorBootstrapped<TSessionCacheActor>
{
private:
    const TString CachePath;
    const TDuration ReleaseInactiveSessionsTimeout;

public:
    TSessionCacheActor(
        TString cachePath,
        TDuration releaseInactiveSessionsTimeout)
        : CachePath{std::move(cachePath)}
        , ReleaseInactiveSessionsTimeout{releaseInactiveSessionsTimeout}
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        Become(&TThis::StateWork);

        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_AGENT_WORKER,
            "Session Cache Actor started");
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);

            HFunc(
                TEvDiskAgentPrivate::TEvUpdateSessionCacheRequest,
                HandleUpdateSessionCache);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::DISK_AGENT_WORKER,
                    __PRETTY_FUNCTION__);
                break;
        }
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        Die(ctx);
    }

    void HandleUpdateSessionCache(
        const TEvDiskAgentPrivate::TEvUpdateSessionCacheRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_AGENT_WORKER,
            "Update the session cache");

        auto* msg = ev->Get();

        const auto deadline = ctx.Now() - ReleaseInactiveSessionsTimeout;
        Y_DEBUG_ABORT_UNLESS(deadline);

        SaveSessionCache(CachePath, msg->Sessions, deadline);

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<
                TEvDiskAgentPrivate::TEvUpdateSessionCacheResponse>());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IActor> CreateSessionCacheActor(
    TString cachePath,
    TDuration releaseInactiveSessionsTimeout)
{
    return std::make_unique<TSessionCacheActor>(
        std::move(cachePath),
        releaseInactiveSessionsTimeout);
}

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
