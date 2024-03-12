#include "disk_agent_actor.h"

#include <ydb/core/base/appdata.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TDiskAgentActor::HandleReleaseDevices(
    const TEvDiskAgent::TEvReleaseDevicesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    BLOCKSTORE_DISK_AGENT_COUNTER(ReleaseDevices);

    auto response = std::make_unique<TEvDiskAgent::TEvReleaseDevicesResponse>();

    try {
        const auto& record = ev->Get()->Record;
        const auto& uuids = record.GetDeviceUUIDs();

        const TVector<TString> tmp(uuids.begin(), uuids.end());

        State->ReleaseDevices(
            tmp,
            record.GetHeaders().GetClientId(),
            record.GetDiskId(),
            record.GetVolumeGeneration());

        // We should update the session cache (if it was configured) with every
        // release request.
        if (GetCachedSessionsPath()) {
            UpdateSessionCacheAndRespond(
                ctx,
                CreateRequestInfo(
                    ev->Sender,
                    ev->Cookie,
                    ev->Get()->CallContext),
                std::move(response));

            return;
        }
    } catch (const TServiceError& e) {
        *response->Record.MutableError() = MakeError(e.GetCode(), e.what());
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
