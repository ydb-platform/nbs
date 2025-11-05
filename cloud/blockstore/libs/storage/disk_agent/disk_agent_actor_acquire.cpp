#include "disk_agent_actor.h"

#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <contrib/ydb/core/base/appdata.h>

#include <util/string/join.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::HandleAcquireDevices(
    const TEvDiskAgent::TEvAcquireDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_AGENT_COUNTER(AcquireDevices);

    auto* actorSystem = ctx.ActorSystem();
    auto replyFrom = ctx.SelfID;

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        ev->Get()->CallContext);

    auto reply = [=] (auto error) {
        auto response = std::make_unique<TEvDiskAgent::TEvAcquireDevicesResponse>(
            std::move(error));

        actorSystem->Send(
            new IEventHandle(
                requestInfo->Sender,
                replyFrom,
                response.release(),
                0,          // flags
                requestInfo->Cookie));
    };

    const auto* msg = ev->Get();
    const auto& record = msg->Record;
    const TString clientId = record.GetHeaders().GetClientId();

    TVector<TString> uuids {
        record.GetDeviceUUIDs().begin(),
        record.GetDeviceUUIDs().end()
    };

    try {
        LOG_DEBUG_S(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "AcquireDevices: clientId=" << clientId
                << ", uuids=" << JoinSeq(",", uuids)
        );

        const bool updated = State->AcquireDevices(
            uuids,
            clientId,
            ctx.Now(),
            record.GetAccessMode(),
            record.GetMountSeqNumber(),
            record.GetDiskId(),
            record.GetVolumeGeneration());

        if (updated) {
            UpdateSessionCache(ctx);
        }

        if (!Spdk || !record.HasRateLimits()) {
            reply(NProto::TError());
            return;
        }

        const NSpdk::TDeviceRateLimits limits {
            record.GetRateLimits().GetIopsLimit(),
            record.GetRateLimits().GetBandwidthLimit(),
            record.GetRateLimits().GetReadBandwidthLimit(),
            record.GetRateLimits().GetWriteBandwidthLimit()
        };

        TVector<TFuture<void>> futures(Reserve(uuids.size()));

        for (const auto& uuid: uuids) {
            const auto& deviceName = State->GetDeviceName(uuid);

            futures.push_back(Spdk->SetRateLimits(deviceName, limits));
        }

        WaitExceptionOrAll(futures).Subscribe(
            [
                =, this,
                uuids = std::move(uuids),
                diskId = record.GetDiskId(),
                volumeGeneration = record.GetVolumeGeneration()
            ] (const auto& future) {
                try {
                    future.GetValue();
                    reply(NProto::TError());
                } catch (...) {
                    State->ReleaseDevices(
                        uuids,
                        clientId,
                        diskId,
                        volumeGeneration);
                    reply(MakeError(E_FAIL, CurrentExceptionMessage()));
                }
            });
    } catch (const TServiceError& e) {
        LOG_ERROR_S(
            ctx,
            TBlockStoreComponents::DISK_AGENT,
            "AcquireDevices failed: " << e.what()
                << ", uuids=" << JoinSeq(",", uuids)
        );

        reply(MakeError(e.GetCode(), e.what()));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
