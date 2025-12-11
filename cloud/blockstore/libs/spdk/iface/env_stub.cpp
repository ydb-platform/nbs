#include "env_stub.h"

#include "device.h"
#include "env.h"
#include "target.h"

#include <util/generic/hash.h>
#include <util/string/cast.h>

namespace NCloud::NBlockStore::NSpdk {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DefaultBlocksCount = 1024 * 1024;

////////////////////////////////////////////////////////////////////////////////

class TSpdkEnvStub final: public ISpdkEnv
{
private:
    size_t DeviceId = 0;

    THashMap<TString, TDeviceStats> Devices;

public:
    void Start() override
    {}

    void Stop() override
    {}

    TFuture<void> StartAsync() override
    {
        return MakeFuture();
    }

    TFuture<void> StopAsync() override
    {
        return MakeFuture();
    }

    IAllocator* GetAllocator() override
    {
        return TDefaultAllocator::Instance();
    }

    TFuture<TString> RegisterNullDevice(
        const TString& name,
        ui64 blocksCount,
        ui32 blockSize) override
    {
        Y_UNUSED(name);

        auto deviceId = "NullDevice" + ToString(++DeviceId);
        RegisterDevice(deviceId, blocksCount, blockSize);
        return MakeFuture(deviceId);
    }

    TFuture<TString> RegisterMemoryDevice(
        const TString& name,
        ui64 blocksCount,
        ui32 blockSize) override
    {
        Y_UNUSED(name);

        auto deviceId = "MemoryDevice" + ToString(++DeviceId);
        RegisterDevice(deviceId, blocksCount, blockSize);
        return MakeFuture(deviceId);
    }

    TFuture<TString> RegisterFileDevice(
        const TString& name,
        const TString& path,
        ui32 blockSize) override
    {
        Y_UNUSED(name);
        Y_UNUSED(path);

        auto deviceId = "FileDevice" + ToString(++DeviceId);
        RegisterDevice(deviceId, DefaultBlocksCount, blockSize);
        return MakeFuture(deviceId);
    }

    TFuture<TString> RegisterSCSIDevice(
        const TString& name,
        const TString& targetUrl,
        const TString& initiatorIqn) override
    {
        Y_UNUSED(name);
        Y_UNUSED(targetUrl);
        Y_UNUSED(initiatorIqn);

        auto deviceId = "SCSIDevice" + ToString(++DeviceId);
        RegisterDevice(deviceId, DefaultBlocksCount, DefaultBlockSize);
        return MakeFuture(deviceId);
    }

    TFuture<TVector<TString>> RegisterNVMeDevices(
        const TString& baseName,
        const TString& transportId) override
    {
        Y_UNUSED(baseName);
        Y_UNUSED(transportId);

        auto deviceId = "NVMeDevice" + ToString(++DeviceId);
        RegisterDevice(deviceId, DefaultBlocksCount, DefaultBlockSize);
        return MakeFuture(TVector<TString>{deviceId});
    }

    TFuture<void> UnregisterDevice(const TString& name) override
    {
        Y_UNUSED(name);

        return MakeFuture();
    }

    TFuture<ISpdkDevicePtr> OpenDevice(const TString& name, bool write) override
    {
        Y_UNUSED(name);
        Y_UNUSED(write);

        return MakeFuture(CreateDeviceStub());
    }

    TFuture<TDeviceStats> QueryDeviceStats(const TString& name) override
    {
        return MakeFuture(Devices[name]);
    }

    TFuture<TString> RegisterDeviceProxy(
        const TString& deviceName,
        const TString& proxyName) override
    {
        Y_UNUSED(deviceName);

        return MakeFuture(proxyName);
    }

    TFuture<TString> RegisterDeviceWrapper(
        ISpdkDevicePtr device,
        const TString& wrapperName,
        ui64 blocksCount,
        ui32 blockSize) override
    {
        Y_UNUSED(device);
        Y_UNUSED(blocksCount);
        Y_UNUSED(blockSize);

        return MakeFuture(wrapperName);
    }

    TFuture<void> SetRateLimits(
        const TString& deviceName,
        TDeviceRateLimits limits) override
    {
        Y_UNUSED(deviceName);
        Y_UNUSED(limits);

        return MakeFuture();
    }

    TFuture<TDeviceRateLimits> GetRateLimits(const TString& deviceName) override
    {
        Y_UNUSED(deviceName);

        return MakeFuture(TDeviceRateLimits());
    }

    TFuture<TDeviceIoStats> GetDeviceIoStats(const TString& deviceName) override
    {
        Y_UNUSED(deviceName);

        return MakeFuture(TDeviceIoStats());
    }

    TFuture<void> EnableHistogram(
        const TString& deviceName,
        bool enable) override
    {
        Y_UNUSED(deviceName);
        Y_UNUSED(enable);

        return MakeFuture();
    }

    TFuture<TVector<TBucketInfo>> GetHistogramBuckets(
        const TString& deviceName) override
    {
        Y_UNUSED(deviceName);

        return MakeFuture(TVector<TBucketInfo>());
    }

    TFuture<void> AddTransport(const TString& transportId) override
    {
        Y_UNUSED(transportId);

        return MakeFuture();
    }

    TFuture<void> StartListen(const TString& transportId) override
    {
        Y_UNUSED(transportId);

        return MakeFuture();
    }

    TFuture<ISpdkTargetPtr> CreateNVMeTarget(
        const TString& nqn,
        const TVector<TString>& devices,
        const TVector<TString>& transportIds) override
    {
        Y_UNUSED(nqn);
        Y_UNUSED(devices);
        Y_UNUSED(transportIds);

        return MakeFuture(CreateTargetStub());
    }

    TFuture<void> CreatePortalGroup(
        ui32 tag,
        const TVector<TPortal>& portals) override
    {
        Y_UNUSED(tag);
        Y_UNUSED(portals);

        return MakeFuture();
    }

    TFuture<void> CreateInitiatorGroup(
        ui32 tag,
        const TVector<TInitiator>& initiators) override
    {
        Y_UNUSED(tag);
        Y_UNUSED(initiators);

        return MakeFuture();
    }

    TFuture<ISpdkTargetPtr> CreateSCSITarget(
        const TString& name,
        const TVector<TDevice>& devices,
        const TVector<TGroupMapping>& groups) override
    {
        Y_UNUSED(name);
        Y_UNUSED(devices);
        Y_UNUSED(groups);

        return MakeFuture(CreateTargetStub());
    }

private:
    void
    RegisterDevice(const TString& deviceId, ui64 blocksCount, ui32 blockSize)
    {
        auto& stats = Devices[deviceId];
        Zero(stats);
        stats.BlocksCount = blocksCount;
        stats.BlockSize = blockSize;
        CreateGuid(&stats.DeviceUUID);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSpdkDeviceStub final: public ISpdkDevice
{
public:
    void Start() override
    {}

    void Stop() override
    {}

    TFuture<void> StartAsync() override
    {
        return MakeFuture();
    }

    TFuture<void> StopAsync() override
    {
        return MakeFuture();
    }

    TFuture<NProto::TError>
    Read(void* buf, ui64 fileOffset, ui32 bytesCount) override
    {
        Y_UNUSED(buf);
        Y_UNUSED(fileOffset);
        Y_UNUSED(bytesCount);

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError>
    Write(void* buf, ui64 fileOffset, ui32 bytesCount) override
    {
        Y_UNUSED(buf);
        Y_UNUSED(fileOffset);
        Y_UNUSED(bytesCount);

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError>
    Read(TSgList sglist, ui64 fileOffset, ui32 bytesCount) override
    {
        Y_UNUSED(sglist);
        Y_UNUSED(fileOffset);
        Y_UNUSED(bytesCount);

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError>
    Write(TSgList sglist, ui64 fileOffset, ui32 bytesCount) override
    {
        Y_UNUSED(sglist);
        Y_UNUSED(fileOffset);
        Y_UNUSED(bytesCount);

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> WriteZeroes(
        ui64 fileOffset,
        ui32 bytesCount) override
    {
        Y_UNUSED(fileOffset);
        Y_UNUSED(bytesCount);

        return MakeFuture<NProto::TError>();
    }

    NThreading::TFuture<NProto::TError> Erase(
        NProto::EDeviceEraseMethod method) override
    {
        Y_UNUSED(method);

        return MakeFuture<NProto::TError>();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSpdkTargetStub final: public ISpdkTarget
{
public:
    void Start() override
    {}

    void Stop() override
    {}

    TFuture<void> StartAsync() override
    {
        return MakeFuture();
    }

    TFuture<void> StopAsync() override
    {
        return MakeFuture();
    }

    ISpdkDevicePtr GetDevice(const TString& name) override
    {
        Y_UNUSED(name);

        return CreateDeviceStub();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ISpdkEnvPtr CreateEnvStub()
{
    return std::make_shared<TSpdkEnvStub>();
}

ISpdkDevicePtr CreateDeviceStub()
{
    return std::make_shared<TSpdkDeviceStub>();
}

ISpdkTargetPtr CreateTargetStub()
{
    return std::make_shared<TSpdkTargetStub>();
}

}   // namespace NCloud::NBlockStore::NSpdk
