#include "config_cache_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/storage/disk_agent/disk_agent_private.h>
#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/system/fs.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage::NDiskAgent {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TConfigCacheActor: public TActorBootstrapped<TConfigCacheActor>
{
private:
    const TString CachePath;

public:
    explicit TConfigCacheActor(TString cachePath)
        : CachePath(std::move(cachePath))
    {
        ActivityType = TBlockStoreComponents::DISK_AGENT_WORKER;
    }

    void Bootstrap(const TActorContext& ctx)
    {
        Become(&TThis::StateWork);

        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::DISK_AGENT_WORKER,
            "Config cache actor started; path to config: " << CachePath);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);
            HFunc(TEvDiskAgentPrivate::TEvUpdateConfigCacheRequest, HandleUpdateConfigCache);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::DISK_AGENT_WORKER);
                break;
        }
    }

    void HandleUpdateConfigCache(
        const TEvDiskAgentPrivate::TEvUpdateConfigCacheRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_AGENT_WORKER,
            "Trying to update the cached config");

        if (CachePath.empty()) {
            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<
                    TEvDiskAgentPrivate::TEvUpdateConfigCacheResponse>(
                    MakeError(S_FALSE, "Path is empty")));
            return;
        }

        auto* msg = ev->Get();
        const TString tmpPath{CachePath + ".tmp"};
        SerializeToTextFormat(msg->Config, tmpPath);

        if (!NFs::Rename(tmpPath, CachePath)) {
            const auto ec = errno;
            NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<
                    TEvDiskAgentPrivate::TEvUpdateConfigCacheResponse>(
                    MakeError(
                        MAKE_SYSTEM_ERROR(ec),
                        TStringBuilder() << strerror(ec))));
            return;
        }

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<
                TEvDiskAgentPrivate::TEvUpdateConfigCacheResponse>());
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        Die(ctx);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IActor> CreateConfigCacheActor(
    TString cachePath)
{
    Y_DEBUG_ABORT_UNLESS(!cachePath.empty());

    return std::make_unique<TConfigCacheActor>(
        std::move(cachePath));
}

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
