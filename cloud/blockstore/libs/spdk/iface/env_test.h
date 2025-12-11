#pragma once

#include "public.h"

#include "device.h"
#include "env.h"
#include "target.h"

namespace NCloud::NBlockStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

struct TTestSpdkDevice: public ISpdkDevice
{
    std::function<NThreading::TFuture<void>()> StartHandler;
    std::function<NThreading::TFuture<void>()> StopHandler;

    using TBufferRequestHandler = std::function<NThreading::TFuture<
        NProto::TError>(void* buf, ui64 fileOffset, ui32 bytesCount)>;
    TBufferRequestHandler ReadBufferHandler;
    TBufferRequestHandler WriteBufferHandler;

    using TSgListRequestHandler = std::function<NThreading::TFuture<
        NProto::TError>(TSgList sglist, ui64 fileOffset, ui32 bytesCount)>;
    TSgListRequestHandler ReadSgListHandler;
    TSgListRequestHandler WriteSgListHandler;

    using TZeroRequestHandler = std::function<
        NThreading::TFuture<NProto::TError>(ui64 fileOffset, ui32 bytesCount)>;
    TZeroRequestHandler WriteZeroesHandler;

    using TEraseRequestHandler =
        std::function<NThreading::TFuture<NProto::TError>(
            NProto::EDeviceEraseMethod method)>;
    TEraseRequestHandler EraseHandler;

    void Start() override
    {
        StartAsync().GetValue(StartTimeout);
    }

    void Stop() override
    {
        StopAsync().GetValue(StartTimeout);
    }

    NThreading::TFuture<void> StartAsync() override
    {
        return StartHandler();
    }

    NThreading::TFuture<void> StopAsync() override
    {
        return StopHandler();
    }

    NThreading::TFuture<NProto::TError>
    Read(void* buf, ui64 fileOffset, ui32 bytesCount) override
    {
        return ReadBufferHandler(buf, fileOffset, bytesCount);
    }

    NThreading::TFuture<NProto::TError>
    Write(void* buf, ui64 fileOffset, ui32 bytesCount) override
    {
        return WriteBufferHandler(buf, fileOffset, bytesCount);
    }

    NThreading::TFuture<NProto::TError>
    Read(TSgList sglist, ui64 fileOffset, ui32 bytesCount) override
    {
        return ReadSgListHandler(std::move(sglist), fileOffset, bytesCount);
    }

    NThreading::TFuture<NProto::TError>
    Write(TSgList sglist, ui64 fileOffset, ui32 bytesCount) override
    {
        return WriteSgListHandler(std::move(sglist), fileOffset, bytesCount);
    }

    NThreading::TFuture<NProto::TError> WriteZeroes(
        ui64 fileOffset,
        ui32 bytesCount) override
    {
        return WriteZeroesHandler(fileOffset, bytesCount);
    }

    NThreading::TFuture<NProto::TError> Erase(
        NProto::EDeviceEraseMethod method) override
    {
        return EraseHandler(method);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestSpdkTarget: public ISpdkTarget
{
    void Start() override
    {}

    void Stop() override
    {}

    NThreading::TFuture<void> StartAsync() override
    {
        return NThreading::MakeFuture();
    }

    NThreading::TFuture<void> StopAsync() override
    {
        return NThreading::MakeFuture();
    }

    ISpdkDevicePtr GetDevice(const TString& name) override
    {
        Y_UNUSED(name);

        Y_ABORT("Not implemented");
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestSpdkEnv: public ISpdkEnv
{
    void Start() override
    {}

    void Stop() override
    {}

    NThreading::TFuture<void> StartAsync() override
    {
        return NThreading::MakeFuture();
    }

    NThreading::TFuture<void> StopAsync() override
    {
        return NThreading::MakeFuture();
    }

    IAllocator* GetAllocator() override
    {
        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<TString> RegisterNullDevice(
        const TString& name,
        ui64 blocksCount,
        ui32 blockSize) override
    {
        Y_UNUSED(name);
        Y_UNUSED(blocksCount);
        Y_UNUSED(blockSize);

        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<TString> RegisterMemoryDevice(
        const TString& name,
        ui64 blocksCount,
        ui32 blockSize) override
    {
        Y_UNUSED(name);
        Y_UNUSED(blocksCount);
        Y_UNUSED(blockSize);

        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<TString> RegisterFileDevice(
        const TString& name,
        const TString& path,
        ui32 blockSize) override
    {
        Y_UNUSED(name);
        Y_UNUSED(path);
        Y_UNUSED(blockSize);

        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<TString> RegisterSCSIDevice(
        const TString& name,
        const TString& targetUrl,
        const TString& initiatorIqn) override
    {
        Y_UNUSED(name);
        Y_UNUSED(targetUrl);
        Y_UNUSED(initiatorIqn);

        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<TVector<TString>> RegisterNVMeDevices(
        const TString& baseName,
        const TString& transportId) override
    {
        Y_UNUSED(baseName);
        Y_UNUSED(transportId);

        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<void> UnregisterDevice(const TString& name) override
    {
        Y_UNUSED(name);

        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<ISpdkDevicePtr> OpenDevice(
        const TString& name,
        bool write) override
    {
        Y_UNUSED(name);
        Y_UNUSED(write);

        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<TDeviceStats> QueryDeviceStats(
        const TString& name) override
    {
        Y_UNUSED(name);

        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<TString> RegisterDeviceProxy(
        const TString& deviceName,
        const TString& proxyName) override
    {
        Y_UNUSED(deviceName);
        Y_UNUSED(proxyName);

        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<TString> RegisterDeviceWrapper(
        ISpdkDevicePtr device,
        const TString& wrapperName,
        ui64 blocksCount,
        ui32 blockSize) override
    {
        Y_UNUSED(device);
        Y_UNUSED(wrapperName);
        Y_UNUSED(blocksCount);
        Y_UNUSED(blockSize);

        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<void> SetRateLimits(
        const TString& deviceName,
        TDeviceRateLimits limits) override
    {
        Y_UNUSED(deviceName);
        Y_UNUSED(limits);

        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<TDeviceRateLimits> GetRateLimits(
        const TString& deviceName) override
    {
        Y_UNUSED(deviceName);

        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<TDeviceIoStats> GetDeviceIoStats(
        const TString& deviceName) override
    {
        Y_UNUSED(deviceName);

        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<void> EnableHistogram(
        const TString& deviceName,
        bool enable) override
    {
        Y_UNUSED(deviceName);
        Y_UNUSED(enable);

        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<TVector<TBucketInfo>> GetHistogramBuckets(
        const TString& deviceName) override
    {
        Y_UNUSED(deviceName);

        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<void> AddTransport(const TString& transportId) override
    {
        Y_UNUSED(transportId);

        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<void> StartListen(const TString& transportId) override
    {
        Y_UNUSED(transportId);

        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<ISpdkTargetPtr> CreateNVMeTarget(
        const TString& nqn,
        const TVector<TString>& devices,
        const TVector<TString>& transportIds) override
    {
        Y_UNUSED(nqn);
        Y_UNUSED(devices);
        Y_UNUSED(transportIds);

        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<void> CreatePortalGroup(
        ui32 tag,
        const TVector<TPortal>& portals) override
    {
        Y_UNUSED(tag);
        Y_UNUSED(portals);

        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<void> CreateInitiatorGroup(
        ui32 tag,
        const TVector<TInitiator>& initiators) override
    {
        Y_UNUSED(tag);
        Y_UNUSED(initiators);

        Y_ABORT("Not implemented");
    }

    NThreading::TFuture<ISpdkTargetPtr> CreateSCSITarget(
        const TString& name,
        const TVector<TDevice>& devices,
        const TVector<TGroupMapping>& groups) override
    {
        Y_UNUSED(name);
        Y_UNUSED(devices);
        Y_UNUSED(groups);

        Y_ABORT("Not implemented");
    }
};

}   // namespace NCloud::NBlockStore::NSpdk
