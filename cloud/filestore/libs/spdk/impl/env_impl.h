#pragma once

#include "public.h"

#include "alloc.h"
#include "env.h"
#include "spdk.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/common/task_queue.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NFileStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

class TSpdkEnv final
    : public ISpdkEnv
    , public ITaskQueue
    , public std::enable_shared_from_this<TSpdkEnv>
{
    friend class TSpdkDevice;

private:
    class TMainThread;
    std::unique_ptr<TMainThread> MainThread;

public:
    explicit TSpdkEnv(TSpdkEnvConfigPtr config);
    ~TSpdkEnv();

    //
    // IStartable
    //

    void Start() override;
    void Stop() override;

    NThreading::TFuture<void> StartAsync() override;
    NThreading::TFuture<void> StopAsync() override;

    //
    // ITaskQueue
    //

    void Enqueue(ITaskPtr task) override;

    //
    // Memory
    //

    IAllocator* GetAllocator() override
    {
        return GetHugePageAllocator();
    }

    //
    // Device
    //

    NThreading::TFuture<TString> RegisterNullDevice(
        const TString& name,
        ui64 blocksCount,
        ui32 blockSize) override;

    NThreading::TFuture<TString> RegisterMemoryDevice(
        const TString& name,
        ui64 blocksCount,
        ui32 blockSize) override;

    NThreading::TFuture<TString> RegisterFileDevice(
        const TString& name,
        const TString& path,
        ui32 blockSize) override;

    NThreading::TFuture<TString> RegisterSCSIDevice(
        const TString& name,
        const TString& targetUrl,
        const TString& initiatorIqn) override;

    NThreading::TFuture<TVector<TString>> RegisterNVMeDevices(
        const TString& baseName,
        const TString& transportId) override;

    NThreading::TFuture<void> UnregisterDevice(
        const TString& name) override;

    NThreading::TFuture<ISpdkDevicePtr> OpenDevice(
        const TString& name,
        bool write) override;

    NThreading::TFuture<TDeviceStats> QueryDeviceStats(
        const TString& name) override;

    NThreading::TFuture<TString> RegisterDeviceProxy(
        const TString& deviceName,
        const TString& proxyName) override;

    NThreading::TFuture<TString> RegisterDeviceWrapper(
        ISpdkDevicePtr device,
        const TString& wrapperName,
        ui64 blocksCount,
        ui32 blockSize) override;

    NThreading::TFuture<void> SetRateLimits(
        const TString& deviceName,
        TDeviceRateLimits limits) override;

    NThreading::TFuture<TDeviceRateLimits> GetRateLimits(
        const TString& deviceName) override;

    NThreading::TFuture<TDeviceIoStats> GetDeviceIoStats(
        const TString& deviceName) override;

    NThreading::TFuture<void> EnableHistogram(
        const TString& deviceName,
        bool enable) override;

    NThreading::TFuture<TVector<TBucketInfo>> GetHistogramBuckets(
        const TString& deviceName) override;

    //
    // NVMe-oF target
    //

    NThreading::TFuture<void> AddTransport(
        const TString& transportId) override;

    NThreading::TFuture<void> StartListen(
        const TString& transportId) override;

    NThreading::TFuture<ISpdkTargetPtr> CreateNVMeTarget(
        const TString& nqn,
        const TVector<TString>& devices,
        const TVector<TString>& transportIds) override;

    //
    // iSCSI target
    //

    NThreading::TFuture<void> CreatePortalGroup(
        ui32 tag,
        const TVector<TPortal>& portals) override;

    NThreading::TFuture<void> CreateInitiatorGroup(
        ui32 tag,
        const TVector<TInitiator>& initiators) override;

    NThreading::TFuture<ISpdkTargetPtr> CreateSCSITarget(
        const TString& name,
        const TVector<TDevice>& devices,
        const TVector<TGroupMapping>& groups) override;

private:
    int PickCore();
    ITaskQueue* GetTaskQueue(int index = -1);

    ISpdkDevicePtr OpenDevice(spdk_bdev* bdev, bool write);

    NThreading::TFuture<TVector<TString>> CreateNvmeController(
        const TString& baseName,
        const TString& transportId);
};

////////////////////////////////////////////////////////////////////////////////

using TSpdkEnvPtr = std::shared_ptr<TSpdkEnv>;

}   // namespace NCloud::NFileStore::NSpdk
