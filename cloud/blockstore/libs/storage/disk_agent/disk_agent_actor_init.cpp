#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>

#include <util/generic/vector.h>
#include <util/stream/str.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::InitAgent(const TActorContext& ctx)
{
    State = std::make_unique<TDiskAgentState>(
        AgentConfig,
        Spdk,
        Allocator,
        StorageProvider,
        ProfileLog,
        BlockDigestGenerator,
        Logging,
        RdmaServer,
        NvmeManager);

    auto result = State->Initialize();

    auto* actorSystem = ctx.ActorSystem();
    auto replyTo = ctx.SelfID;

    result.Subscribe([=] (auto future) {
        using TCompletionEvent = TEvDiskAgentPrivate::TEvInitAgentCompleted;

        NProto::TError error;

        try {
            TDiskAgentState::TInitializeResult r = future.ExtractValue();

            auto response = std::make_unique<TCompletionEvent>(
                std::move(r.Configs),
                std::move(r.Errors));

            actorSystem->Send(
                new IEventHandle(
                    replyTo,
                    replyTo,
                    response.release()));
        } catch (const TServiceError& e) {
            error = MakeError(e.GetCode(), TString(e.GetMessage()));
        } catch (...) {
            error = MakeError(E_FAIL, CurrentExceptionMessage());
        }

        if (error.GetCode()) {
            auto response = std::make_unique<TCompletionEvent>(error);

            actorSystem->Send(
                new IEventHandle(
                    replyTo,
                    replyTo,
                    response.release()));
        }
    });
}

void TDiskAgentActor::HandleInitAgentCompleted(
    const TEvDiskAgentPrivate::TEvInitAgentCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    for (const auto& error: msg->Errors) {
        LOG_WARN_S(ctx, TBlockStoreComponents::DISK_AGENT, error);
    }

    if (const auto& error = msg->GetError(); HasError(error)) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::DISK_AGENT,
            "DiskAgent initialization failed. Error: " << FormatError(error).data());
    } else {
        TStringStream out;
        for (const auto& config: msg->Configs) {
            out << config.GetDeviceName()
                << "(" << FormatByteSize(config.GetBlocksCount() * config.GetBlockSize())
                << "); ";
        }

        LOG_INFO_S(ctx, TBlockStoreComponents::DISK_AGENT,
            "Initialization completed. Devices found: " << out.Str());
    }

    // resend pending requests
    SendPendingRequests(ctx, PendingRequests);

    Become(&TThis::StateWork);

    SendRegisterRequest(ctx);
    ScheduleUpdateStats(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
