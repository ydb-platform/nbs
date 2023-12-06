#include "volume_session_actor.h"

#include "service_actor.h"

#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/mount_token.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <ydb/core/tablet/tablet_setup.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/deque.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

using EChangeBindingOp =
    TEvService::TEvChangeVolumeBindingRequest::EChangeBindingOp;

}  // namespace

////////////////////////////////////////////////////////////////////////////////

void TVolumeSessionActor::PostponeChangeVolumeBindingRequest(
    const TEvService::TEvChangeVolumeBindingRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    MountUnmountRequests.emplace(ev.Release());
}

void TVolumeSessionActor::HandleChangeVolumeBindingRequest(
    const TEvService::TEvChangeVolumeBindingRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto replyError = [&] (NProto::TError result)
    {
        using TResponse = TEvService::TEvChangeVolumeBindingResponse;
        auto response = std::make_unique<TResponse>(std::move(result));
        NCloud::Reply(ctx, *ev, std::move(response));
    };

    const auto* localMounter = VolumeInfo->GetLocalMountClientInfo();
    if (!localMounter) {
        replyError(MakeError(E_ARGUMENT, "No local mounter found"));
        return;
    }

    NProto::EVolumeBinding bindingType = NProto::BINDING_NOT_SET;
    if (msg->Action == EChangeBindingOp::RELEASE_TO_HIVE) {
        if (VolumeInfo->BindingType == NProto::BINDING_REMOTE) {
            replyError(
                MakeError(S_ALREADY, "Volume is already moved from host"));
            return;
        }
        bindingType = NProto::BINDING_REMOTE;
    } else {
        if (VolumeInfo->BindingType == NProto::BINDING_LOCAL) {
            replyError(
                MakeError(S_ALREADY, "Volume is already running at host"));
        }
        bindingType = NProto::BINDING_LOCAL;
    }

    // simulate mount request
    auto request =
        std::make_unique<TEvServicePrivate::TEvInternalMountVolumeRequest>();
    request->Record.SetDiskId(VolumeInfo->DiskId);
    request->Record.SetVolumeAccessMode(localMounter->VolumeAccessMode);
    request->Record.SetVolumeMountMode(localMounter->VolumeMountMode);
    request->Record.MutableHeaders()->SetClientId(localMounter->ClientId);
    if (VolumeInfo->VolumeInfo) {
        const auto& desc = VolumeInfo->VolumeInfo->GetEncryptionDesc();
        auto& spec = *request->Record.MutableEncryptionSpec();
        spec.SetMode(desc.GetMode());
        spec.SetKeyHash(desc.GetKeyHash());
    }
    request->BindingType = bindingType;
    request->PreemptionSource = msg->Source;
    request->CallContext = std::move(msg->CallContext);

    using TEventType =
        TEvServicePrivate::TEvInternalMountVolumeRequest::TPtr;
    auto event =
        static_cast<TEventType::TValueType*>(new IEventHandle(
            SelfId(),
            ev->Sender,
            request.release()));

    HandleInternalMountVolume(event, ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
