#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/storage.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

struct ISession
    : public IStorage
{
    virtual ~ISession() = default;

    virtual ui32 GetMaxTransfer() const = 0;

    virtual NThreading::TFuture<NProto::TMountVolumeResponse> MountVolume(
        NProto::EVolumeAccessMode accessMode,
        NProto::EVolumeMountMode mountMode,
        ui64 mountSeqNumber,
        TCallContextPtr callContext = MakeIntrusive<TCallContext>(),
        const NProto::THeaders& headers = {}) = 0;

    virtual NThreading::TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr callContext = MakeIntrusive<TCallContext>(),
        const NProto::THeaders& headers = {}) = 0;

    virtual NThreading::TFuture<NProto::TUnmountVolumeResponse> UnmountVolume(
        TCallContextPtr callContext = MakeIntrusive<TCallContext>(),
        const NProto::THeaders& headers = {}) = 0;

    virtual NThreading::TFuture<NProto::TMountVolumeResponse> EnsureVolumeMounted() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TSessionConfig
{
    TString DiskId;
    TString MountToken;

    TString InstanceId;

    NProto::EVolumeAccessMode AccessMode = NProto::VOLUME_ACCESS_READ_WRITE;
    NProto::EVolumeMountMode MountMode = NProto::VOLUME_MOUNT_LOCAL;
    ui32 MountFlags = 0;

    NProto::EClientIpcType IpcType = NProto::IPC_GRPC;

    TString ClientVersionInfo;

    ui64 MountSeqNumber = 0;

    NProto::TEncryptionSpec EncryptionSpec;

    bool RetryFailed = false;
    TDuration RetryTimeout = TDuration::Zero();
    TDuration BackoffIncrement = TDuration::Zero();
};

////////////////////////////////////////////////////////////////////////////////

ISessionPtr CreateSession(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats,
    IBlockStorePtr client,
    TClientAppConfigPtr config,
    const TSessionConfig& sessionConfig);

}   // namespace NCloud::NBlockStore::NClient
