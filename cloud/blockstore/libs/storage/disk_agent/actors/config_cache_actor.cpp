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
    TRequestInfoPtr RequestInfo;

public:
    TConfigCacheActor(TString cachePath, TRequestInfoPtr requestInfo)
        : CachePath(std::move(cachePath))
        , RequestInfo(std::move(requestInfo))
    {
        ActivityType = TBlockStoreComponents::DISK_AGENT_WORKER;
    }

    void Bootstrap(const TActorContext& ctx)
    {
        Become(&TThis::StateWork);

        LOG_INFO(
            ctx,
            TBlockStoreComponents::DISK_AGENT_WORKER,
            "Config cache actor started");

        bool success = NFs::Rename(CachePath, CachePath + ".old");
        auto error = MakeError(success ? S_OK : E_FAIL, "blablabla");

        NCloud::Reply(
            ctx,
            RequestInfo,
            std::make_unique<TEvDiskAgentPrivate::TEvConfigCacheDeletionResult>(
                error));

        Die(ctx);
    }

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(NActors::TEvents::TEvPoisonPill, HandlePoisonPill);

            default:
                HandleUnexpectedEvent(
                    ev,
                    TBlockStoreComponents::DISK_AGENT_WORKER);
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
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IActor> CreateConfigCacheActor(
    TString cachePath,
    TRequestInfoPtr requestInfo)
{
    return std::make_unique<TConfigCacheActor>(
        std::move(cachePath),
        std::move(requestInfo));
}

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
