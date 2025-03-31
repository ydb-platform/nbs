#include "client_state.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/public/api/protos/mount.pb.h>

#include <contrib/ydb/library/actors/core/actor.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

NProto::EVolumeMountMode AdvanceMountMode(
    NProto::EVolumeMountMode curMode,
    NProto::EVolumeMountMode newMode)
{
    switch (curMode) {
        case NProto::VOLUME_MOUNT_REMOTE: {
            if (newMode == NProto::VOLUME_MOUNT_LOCAL) {
                curMode = NProto::VOLUME_MOUNT_LOCAL;
            }
        }
        case NProto::VOLUME_MOUNT_LOCAL: {
            break;
        }
        default: {
            Y_ABORT_UNLESS(0);
        }
    }
    return curMode;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TVolumeClientState::UpdateClientInfo()
{
    NProto::EVolumeMountMode mountMode = NProto::VOLUME_MOUNT_REMOTE;

    for (const auto& pipe: Pipes) {
        if (pipe.second.State != TVolumeClientState::EPipeState::DEACTIVATED) {
            mountMode = AdvanceMountMode(mountMode, pipe.second.MountMode);
        }
    }

    VolumeClientInfo.SetVolumeMountMode(mountMode);
}

void TVolumeClientState::SetLastActivityTimestamp(TInstant ts)
{
    VolumeClientInfo.SetLastActivityTimestamp(ts.MicroSeconds());
}

void TVolumeClientState::SetDisconnectTimestamp(TInstant ts)
{
    VolumeClientInfo.SetDisconnectTimestamp(ts.MicroSeconds());
}

bool TVolumeClientState::IsPreempted(ui64 hostNodeId) const
{
    for (const auto& pipe: Pipes) {
        if (pipe.second.State == TVolumeClientState::EPipeState::DEACTIVATED
            && pipe.second.MountMode == NProto::VOLUME_MOUNT_LOCAL
            && hostNodeId != pipe.second.SenderNodeId)
        {
            return true;
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeClientState::RemovePipe(
    NActors::TActorId serverId,
    TInstant ts)
{
    if (!serverId) {
        Pipes.clear();
        LocalPipeInfo = Pipes.end();
    } else if (auto it = Pipes.find(serverId); it != Pipes.end()) {
        if (LocalPipeInfo == it) {
            LocalPipeInfo = Pipes.end();
        }
        Pipes.erase(it);
    }

    UpdateClientInfo();

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
    auto it = Pipes.find(serverId);

    if (it == Pipes.end()) {
        Pipes.emplace(
            serverId,
            TPipeInfo{
                mountMode,
                EPipeState::WAIT_START,
                senderNodeId});

        VolumeClientInfo.SetVolumeAccessMode(accessMode);
        VolumeClientInfo.SetMountFlags(mountFlags);

        UpdateClientInfo();

        return true;
    } else {
        if (it->second.State == EPipeState::DEACTIVATED) {
            return TAddPipeResult(MakeError(
                E_REJECTED,
                "Pipe is already deactivated"));
        }

        auto oldMountMode = it->second.MountMode;
        auto oldAccessMode = VolumeClientInfo.GetVolumeAccessMode();
        ui32 oldMountFlags = VolumeClientInfo.GetMountFlags();
        it->second =
            TPipeInfo{mountMode, it->second.State, senderNodeId};

        VolumeClientInfo.SetVolumeAccessMode(accessMode);
        VolumeClientInfo.SetMountFlags(mountFlags);

        UpdateClientInfo();

        bool isNew = (oldMountMode != mountMode
            || oldAccessMode != accessMode
            || oldMountFlags != mountFlags);

        return isNew;
    }
}

bool TVolumeClientState::AnyPipeAlive() const
{
    for (const auto& p: Pipes) {
        if (p.second.State != TVolumeClientState::EPipeState::DEACTIVATED) {
            return true;
        }
    }
    return false;
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

void TVolumeClientState::ActivatePipe(
    TVolumeClientState::TPipes::iterator active,
    bool isLocal)
{
    Y_ABORT_UNLESS(active != Pipes.end());

    for (auto it = Pipes.begin(); it != Pipes.end(); ++it) {
        if (it != active && it->second.State == EPipeState::ACTIVE) {
            it->second.State = EPipeState::DEACTIVATED;
        }
    }

    UpdateClientInfo();

    active->second.State = EPipeState::ACTIVE;
    LocalPipeInfo = isLocal ? active : Pipes.end();
}

NProto::TError TVolumeClientState::GetWriteError(
    NProto::EVolumeAccessMode accessMode,
    const TString& methodName,
    const TString& diskId) const
{
    ui32 flags = 0;
    ui32 code = E_ARGUMENT;
    if (accessMode == NProto::VOLUME_ACCESS_USER_READ_ONLY) {
        SetProtoFlag(flags, NProto::EF_SILENT);
        // for legacy clients
        code = E_IO_SILENT;
    }
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
    auto accessMode = VolumeClientInfo.GetVolumeAccessMode();
    auto mountFlags = VolumeClientInfo.GetMountFlags();
    if (Pipes.size()) {
        bool checkOk = false;
        auto it = Pipes.find(serverId);
        if (it != Pipes.end() && it->second.State != EPipeState::DEACTIVATED) {
            if (it->second.State == EPipeState::WAIT_START) {
                ActivatePipe(it, false);
            }
            checkOk = true;
        }

        if (!checkOk) {
            return MakeError(
                E_BS_INVALID_SESSION,
                TStringBuilder() << "No mounter found");
        }
    }

    if (isWrite
            && !IsReadWriteMode(accessMode)
            && !HasProtoFlag(mountFlags, NProto::MF_FORCE_WRITE))
    {
        return GetWriteError(accessMode, methodName, diskId);
    }

    return {};
}

NProto::TError TVolumeClientState::CheckLocalRequest(
    ui32 nodeId,
    bool isWrite,
    const TString& methodName,
    const TString& diskId)
{
    auto accessMode = VolumeClientInfo.GetVolumeAccessMode();
    auto mountFlags = VolumeClientInfo.GetMountFlags();
    bool checkOk = false;
    if (LocalPipeInfo == Pipes.end()) {
        if (Pipes.size()) {
            auto it = std::find_if(
                Pipes.begin(),
                Pipes.end(),
                [&] (const auto& p) {
                    return p.second.SenderNodeId == nodeId
                        && p.second.State != EPipeState::DEACTIVATED;
                });

            if (it != Pipes.end()) {
                if (it->second.State == EPipeState::WAIT_START) {
                    ActivatePipe(it, true);
                } else {
                    LocalPipeInfo = it;
                }

                checkOk = true;
            }
            if (!checkOk) {
                return MakeError(
                    E_BS_INVALID_SESSION,
                    TStringBuilder() << "No local mounter found");
            }
        }
    }

    if (isWrite
            && !IsReadWriteMode(accessMode)
            && !HasProtoFlag(mountFlags, NProto::MF_FORCE_WRITE))
    {
        return GetWriteError(accessMode, methodName, diskId);
    }

    return {};
}

}   // namespace NCloud::NBlockStore::NStorage
