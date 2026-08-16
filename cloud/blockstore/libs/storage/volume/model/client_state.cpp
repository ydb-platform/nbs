#include "client_state.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/public/api/protos/mount.pb.h>

#include <contrib/ydb/library/actors/core/actor.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TVolumeClientState::SetLastActivityTimestamp(TInstant ts)
{
    VolumeClientInfo.SetLastActivityTimestamp(ts.MicroSeconds());
}

void TVolumeClientState::SetDisconnectTimestamp(TInstant ts)
{
    VolumeClientInfo.SetDisconnectTimestamp(ts.MicroSeconds());
}

void TVolumeClientState::UpdateClientInfo(
    const NProto::TVolumeClientInfo& info)
{
    const auto disconnectTimestamp =
        VolumeClientInfo.GetDisconnectTimestamp();
    const auto lastActivityTimestamp =
        VolumeClientInfo.GetLastActivityTimestamp();

    VolumeClientInfo = info;
    VolumeClientInfo.SetDisconnectTimestamp(disconnectTimestamp);
    VolumeClientInfo.SetLastActivityTimestamp(lastActivityTimestamp);

    UpdateState();
}

bool TVolumeClientState::IsPreempted(ui64 hostNodeId) const
{
    return AnyOf(
        Pipes,
        [&](const auto& pipe)
        {
            return pipe.second.State ==
                       TVolumeClientState::EPipeState::DEACTIVATED &&
                   pipe.second.MountMode == NProto::VOLUME_MOUNT_LOCAL &&
                   hostNodeId != pipe.second.SenderNodeId;
        });
}

void TVolumeClientState::RemovePipe(
    NActors::TActorId serverId,
    TInstant ts)
{
    if (!serverId) {
        Pipes.clear();
    } else {
        Pipes.erase(serverId);
    }

    UpdateState();

    if (!AnyPipeAlive() && ts) {
        VolumeClientInfo.SetDisconnectTimestamp(ts.MicroSeconds());
    }
}

TAddPipeResult TVolumeClientState::AddPipe(
    TActorId serverId,
    ui32 senderNodeId,
    NProto::EVolumeAccessMode accessMode,
    NProto::EVolumeMountMode mountMode,
    ui32 mountFlags)
{
    VolumeClientInfo.SetDisconnectTimestamp(0);

    TPipeInfo* pipe = Pipes.FindPtr(serverId);

    if (!pipe) {
        Pipes.emplace(
            serverId,
            TPipeInfo{
                .MountMode = mountMode,
                .State = EPipeState::WAIT_START,
                .SenderNodeId = senderNodeId});

        VolumeClientInfo.SetVolumeAccessMode(accessMode);
        VolumeClientInfo.SetMountFlags(mountFlags);

        UpdateState();

        return TAddPipeResult(true);
    }

    if (pipe->State == EPipeState::DEACTIVATED) {
        return TAddPipeResult(
            MakeError(E_REJECTED, "Pipe is already deactivated"));
    }

    const bool isNew = pipe->MountMode != mountMode ||
                       VolumeClientInfo.GetVolumeAccessMode() != accessMode ||
                       VolumeClientInfo.GetMountFlags() != mountFlags;

    pipe->MountMode = mountMode;
    pipe->SenderNodeId = senderNodeId;

    VolumeClientInfo.SetVolumeAccessMode(accessMode);
    VolumeClientInfo.SetMountFlags(mountFlags);

    UpdateState();

    return TAddPipeResult(isNew);
}

bool TVolumeClientState::AnyPipeAlive() const
{
    return AnyOf(
        Pipes,
        [&](const auto& p)
        {
            return p.second.State !=
                   TVolumeClientState::EPipeState::DEACTIVATED;
        });
}

const TVolumeClientState::TPipes& TVolumeClientState::GetPipes() const
{
    return Pipes;
}

std::optional<TVolumeClientState::TPipeInfo> TVolumeClientState::GetPipeInfo(
    NActors::TActorId serverId) const
{
    auto it = Pipes.find(serverId);
    return it == Pipes.end() ? std::nullopt : std::make_optional(it->second);
}

////////////////////////////////////////////////////////////////////////////////

bool TVolumeClientState::IsLocalPipeActive() const
{
    return ActivePipe &&
           ActivePipe->MountMode == NProto::VOLUME_MOUNT_LOCAL;
}

void TVolumeClientState::UpdateState()
{
    // Update ActivePipe
    const auto it = FindIf(
        Pipes,
        [](const auto& p)
        { return p.second.State == TVolumeClientState::EPipeState::ACTIVE; });
    ActivePipe = it == Pipes.end() ? nullptr : &it->second;

    // Update VolumeClientInfo
    bool hasAliveLocalPipe = AnyOf(
        Pipes,
        [](const auto& p)
        {
            return p.second.State !=
                       TVolumeClientState::EPipeState::DEACTIVATED &&
                   p.second.MountMode == NProto::VOLUME_MOUNT_LOCAL;
        });
    VolumeClientInfo.SetVolumeMountMode(
        hasAliveLocalPipe ? NProto::VOLUME_MOUNT_LOCAL
                          : NProto::VOLUME_MOUNT_REMOTE);
}

void TVolumeClientState::ActivatePipe(TPipeInfo* pipe)
{
    Y_DEBUG_ABORT_UNLESS(pipe->State == EPipeState::WAIT_START);

    if (ActivePipe) {
        ActivePipe->State = EPipeState::DEACTIVATED;
    }

    pipe->State = EPipeState::ACTIVE;

    UpdateState();
}

bool TVolumeClientState::CanWrite() const
{
    const auto accessMode = VolumeClientInfo.GetVolumeAccessMode();
    const auto mountFlags = VolumeClientInfo.GetMountFlags();
    return IsReadWriteMode(accessMode) ||
           HasProtoFlag(mountFlags, NProto::MF_FORCE_WRITE);
}

NProto::TError TVolumeClientState::CheckWritePermission(
    bool isWrite,
    const TString& methodName,
    const TString& diskId) const
{
    if (!isWrite || CanWrite()) {
        return MakeError(S_OK);
    }

    ui32 flags = 0;
    ui32 code = E_ARGUMENT;
    const auto accessMode = VolumeClientInfo.GetVolumeAccessMode();
    if (accessMode == NProto::VOLUME_ACCESS_USER_READ_ONLY) {
        SetProtoFlag(flags, NProto::EF_SILENT);
        // for legacy clients
        code = E_IO_SILENT;
    }

    // Keep in sync with TAlignedDeviceHandler::ReportCriticalError()
    return MakeError(
        code,
        TStringBuilder()
            << "Request " << methodName << " is not allowed for client "
            << VolumeClientInfo.GetClientId().Quote() << " and volume "
            << diskId.Quote(),
        flags);
}

NProto::TError TVolumeClientState::CheckPipeRequest(
    TActorId serverId,
    bool isWrite,
    const TString& methodName,
    const TString& diskId)
{
    // Don't check if there is not a single pipe.
    if (!Pipes.empty()) {
        TPipeInfo* pipe = Pipes.FindPtr(serverId);
        if (!pipe || pipe->State == EPipeState::DEACTIVATED) {
            return MakeError(
                E_BS_INVALID_SESSION,
                TStringBuilder() << "No mounter found "
                                 << (pipe ? "(deactivated)" : "(nullptr)"));
        }
        if (IsLocalPipeActive() &&
            pipe->MountMode == NProto::VOLUME_MOUNT_REMOTE)
        {
            // When the local pipe is ACTIVE response with retriable error
            // E_REJECTED because the local pipe may disconnect and then the
            // request from remote pipe can be executed later.
            return MakeError(
                E_REJECTED,
                TStringBuilder() << "Local mounter is active");
        }
        if (ActivePipe != pipe) {
            ActivatePipe(pipe);
        }
    }

    return CheckWritePermission(isWrite, methodName, diskId);
}

NProto::TError TVolumeClientState::CheckLocalRequest(
    ui32 nodeId,
    bool isWrite,
    const TString& methodName,
    const TString& diskId)
{
    if (IsLocalPipeActive() && ActivePipe->SenderNodeId == nodeId) {
        return CheckWritePermission(isWrite, methodName, diskId);
    }

    // Don't check if there is not a single pipe.
    if (!Pipes.empty()) {
        // Find alive local pipe to activate.
        const auto it = FindIf(
            Pipes,
            [&](const auto& p)
            {
                return p.second.SenderNodeId == nodeId &&
                       p.second.MountMode == NProto::VOLUME_MOUNT_LOCAL &&
                       p.second.State != EPipeState::DEACTIVATED;
            });

        if (it == Pipes.end()) {
            return MakeError(
                E_BS_INVALID_SESSION,
                TStringBuilder() << "No local mounter found");
        }

        if (ActivePipe != &it->second) {
            ActivatePipe(&it->second);
        }
    }

    return CheckWritePermission(isWrite, methodName, diskId);
}

}   // namespace NCloud::NBlockStore::NStorage
