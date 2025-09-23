#pragma once

#include "public.h"

#include "session.h"

#include <cloud/blockstore/libs/service/context.h>

#include <library/cpp/threading/future/future.h>

#include <functional>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

struct TTestSession final
    : public ISession
{
    ui32 MaxTransfer = 8*1024*1024;

    ui32 GetMaxTransfer() const override
    {
        return MaxTransfer;
    }

    using TMountVolumeHandler = std::function<
        NThreading::TFuture<NProto::TMountVolumeResponse>(
            const NProto::THeaders& headers)
        >;

    using TUnmountVolumeHandler = std::function<
        NThreading::TFuture<NProto::TUnmountVolumeResponse>(
            const NProto::THeaders& headers)
        >;

    using TEnsureVolumeMountedHandler = std::function<
        NThreading::TFuture<NProto::TMountVolumeResponse>()>;

    using TReadBlocksHandler = std::function<
        NThreading::TFuture<NProto::TReadBlocksLocalResponse>(
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
        >;

    using TWriteBlocksHandler = std::function<
        NThreading::TFuture<NProto::TWriteBlocksLocalResponse>(
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        >;

    using TZeroBlocksHandler = std::function<
        NThreading::TFuture<NProto::TZeroBlocksResponse>(
            TCallContextPtr callContext,
            std::shared_ptr<NProto::TZeroBlocksRequest> request)
        >;

    using TEraseDeviceHandler = std::function<
        NThreading::TFuture<NProto::TError>(
            NProto::EDeviceEraseMethod method)
        >;

    TMountVolumeHandler MountVolumeHandler;
    TUnmountVolumeHandler UnmountVolumeHandler;
    TEnsureVolumeMountedHandler EnsureVolumeMountedHandler;
    TReadBlocksHandler ReadBlocksHandler;
    TWriteBlocksHandler WriteBlocksHandler;
    TZeroBlocksHandler ZeroBlocksHandler;
    TEraseDeviceHandler EraseDeviceHandler;

    NThreading::TFuture<NProto::TMountVolumeResponse> MountVolume(
        NProto::EVolumeAccessMode accessMode,
        NProto::EVolumeMountMode mountMode,
        ui64 mountSeqNumber,
        TCallContextPtr callContext,
        const NProto::THeaders& headers = {}) override
    {
        Y_UNUSED(accessMode);
        Y_UNUSED(mountMode);
        Y_UNUSED(mountSeqNumber);
        Y_UNUSED(callContext);
        return MountVolumeHandler(headers);
    }

    NThreading::TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr callContext,
        const NProto::THeaders& headers = {}) override
    {
        Y_UNUSED(callContext);
        return MountVolumeHandler(headers);
    }

    NThreading::TFuture<NProto::TUnmountVolumeResponse> UnmountVolume(
        TCallContextPtr callContext,
        const NProto::THeaders& headers = {}) override
    {
        Y_UNUSED(callContext);
        return UnmountVolumeHandler(headers);
    }

    NThreading::TFuture<NProto::TMountVolumeResponse> EnsureVolumeMounted() override
    {
        return EnsureVolumeMountedHandler();
    }

    NThreading::TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        return ReadBlocksHandler(std::move(callContext), std::move(request));
    }

    NThreading::TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        return WriteBlocksHandler(std::move(callContext), std::move(request));
    }

    NThreading::TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        return ZeroBlocksHandler(std::move(callContext), std::move(request));
    }

    NThreading::TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        return EraseDeviceHandler(method);
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        Y_UNUSED(bytesCount);
        return nullptr;
    }

    void OnMountChange(TMountChangeListener fn) override
    {
        Y_UNUSED(fn);
    }

    void ReportIOError() override
    {}
};

}   // namespace NCloud::NBlockStore::NClient
