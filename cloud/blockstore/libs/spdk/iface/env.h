#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/memory/alloc.h>

namespace NCloud::NBlockStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration StartTimeout = TDuration::Seconds(30);

////////////////////////////////////////////////////////////////////////////////

struct ISpdkEnv: public IStartable
{
    virtual ~ISpdkEnv() = default;

    //
    // Initialization
    //

    virtual NThreading::TFuture<void> StartAsync() = 0;
    virtual NThreading::TFuture<void> StopAsync() = 0;

    //
    // Memory
    //

    virtual IAllocator* GetAllocator() = 0;

    //
    // Device
    //

    virtual NThreading::TFuture<TString> RegisterNullDevice(
        const TString& name,
        ui64 blocksCount,
        ui32 blockSize) = 0;

    virtual NThreading::TFuture<TString> RegisterMemoryDevice(
        const TString& name,
        ui64 blocksCount,
        ui32 blockSize) = 0;

    virtual NThreading::TFuture<TString> RegisterFileDevice(
        const TString& name,
        const TString& path,
        ui32 blockSize) = 0;

    virtual NThreading::TFuture<TString> RegisterSCSIDevice(
        const TString& name,
        const TString& targetUrl,
        const TString& initiatorIqn) = 0;

    virtual NThreading::TFuture<TVector<TString>> RegisterNVMeDevices(
        const TString& baseName,
        const TString& transportId) = 0;

    virtual NThreading::TFuture<void> UnregisterDevice(const TString& name) = 0;

    virtual NThreading::TFuture<ISpdkDevicePtr> OpenDevice(
        const TString& name,
        bool write) = 0;

    virtual NThreading::TFuture<TDeviceStats> QueryDeviceStats(
        const TString& name) = 0;

    virtual NThreading::TFuture<TString> RegisterDeviceProxy(
        const TString& deviceName,
        const TString& proxyName) = 0;

    virtual NThreading::TFuture<TString> RegisterDeviceWrapper(
        ISpdkDevicePtr device,
        const TString& wrapperName,
        ui64 blocksCount,
        ui32 blockSize) = 0;

    virtual NThreading::TFuture<void> SetRateLimits(
        const TString& deviceName,
        TDeviceRateLimits limits) = 0;

    virtual NThreading::TFuture<TDeviceRateLimits> GetRateLimits(
        const TString& deviceName) = 0;

    virtual NThreading::TFuture<TDeviceIoStats> GetDeviceIoStats(
        const TString& deviceName) = 0;

    virtual NThreading::TFuture<void> EnableHistogram(
        const TString& deviceName,
        bool enable) = 0;

    virtual NThreading::TFuture<TVector<TBucketInfo>> GetHistogramBuckets(
        const TString& deviceName) = 0;

    //
    // NVMe-oF target
    //

    virtual NThreading::TFuture<void> AddTransport(
        const TString& transportId) = 0;

    virtual NThreading::TFuture<void> StartListen(
        const TString& transportId) = 0;

    virtual NThreading::TFuture<ISpdkTargetPtr> CreateNVMeTarget(
        const TString& nqn,
        const TVector<TString>& devices,
        const TVector<TString>& transportIds) = 0;

    //
    // iSCSI target
    //

    using TPortal = std::pair<TString, ui32>;   // portal address:port

    virtual NThreading::TFuture<void> CreatePortalGroup(
        ui32 tag,
        const TVector<TPortal>& portals) = 0;

    using TInitiator = std::pair<TString, TString>;   // initiator name:netmask

    virtual NThreading::TFuture<void> CreateInitiatorGroup(
        ui32 tag,
        const TVector<TInitiator>& initiators) = 0;

    using TDevice = std::pair<TString, ui32>;   // device name:lun
    using TGroupMapping =
        std::pair<ui32, ui32>;   // initiator group to portal group

    virtual NThreading::TFuture<ISpdkTargetPtr> CreateSCSITarget(
        const TString& name,
        const TVector<TDevice>& devices,
        const TVector<TGroupMapping>& groups) = 0;
};

}   // namespace NCloud::NBlockStore::NSpdk
