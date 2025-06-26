#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/protos/volume.pb.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/generic/set.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TAddPipeResult
{
    NProto::TError Error;
    bool IsNew = false;

    TAddPipeResult(NProto::TError error)
        : Error(std::move(error))
    {}

    TAddPipeResult(bool isNew)
        : IsNew(isNew)
    {}
};

////////////////////////////////////////////////////////////////////////////////

// The TVolumeClientState helps to manage the access of a single client requests
// through multiple pipes.
// Only one pipe can be active at a time. When a new pipe is activated, the
// previous active switched to the DEACTIVATED state. Requests received through
// DEACTIVATED pipes are rejected with E_BS_INVALID_SESSION. State of new
// created pipe is WAIT_START. Requests received through pipe checked with
// CheckPipeRequest() or CheckLocalRequest(). First request from pipe with
// WAIT_START state, permitted and change state of pipe to ACTIVE. Requests
// received through pipe with the ACTIVE state are also allowed. The local pipes
// has priority, when local pipe is active, requests received through the remote
// pipe will be rejected with E_REJECT error and can not switch local pipe to
// the DEACTIVATED state. Requests from the remote pipe will be rejected until
// the local pipe is deleted.

class TVolumeClientState
{
public:
    enum class EPipeState : ui32
    {
        WAIT_START,
        ACTIVE,
        DEACTIVATED
    };

    struct TPipeInfo
    {
        NProto::EVolumeMountMode MountMode = NProto::VOLUME_MOUNT_REMOTE;
        EPipeState State = EPipeState::WAIT_START;
        ui32 SenderNodeId = 0;
        bool IsLocal = false;
    };

    using TPipes = THashMap<NActors::TActorId, TPipeInfo>;

private:
    NProto::TVolumeClientInfo VolumeClientInfo;
    TPipes Pipes;
    TPipeInfo* ActivePipe = nullptr;

public:
    explicit TVolumeClientState(NProto::TVolumeClientInfo info)
        : VolumeClientInfo(std::move(info))
    {}

    void SetLastActivityTimestamp(TInstant ts);
    void SetDisconnectTimestamp(TInstant ts);

    void RemovePipe(NActors::TActorId serverId, TInstant ts);

    TAddPipeResult AddPipe(
        NActors::TActorId serverId,
        ui32 senderNodeId,
        NProto::EVolumeAccessMode accessMode,
        NProto::EVolumeMountMode mountMode,
        ui32 mountFlags);

    NProto::TError CheckPipeRequest(
        NActors::TActorId serverId,
        bool isWrite,
        const TString& methodName,
        const TString& diskId);

    NProto::TError CheckLocalRequest(
        ui32 nodeId,
        bool isWrite,
        const TString& methodName,
        const TString& diskId);

    bool AnyPipeAlive() const;

    const TPipes& GetPipes() const;
    std::optional<TPipeInfo> GetPipeInfo(NActors::TActorId serverId) const;

    const NProto::TVolumeClientInfo& GetVolumeClientInfo() const
    {
        return VolumeClientInfo;
    }

    bool IsPreempted(ui64 hostNodeId) const;

private:
    bool IsLocalPipeActive() const;

    void UpdateState();

    void ActivatePipe(TPipeInfo* pipe, bool isLocal);

    bool CanWrite() const;

    NProto::TError CheckWritePermission(
        bool isWrite,
        const TString& methodName,
        const TString& diskId) const;
};

}   // namespace NCloud::NBlockStore::NStorage
