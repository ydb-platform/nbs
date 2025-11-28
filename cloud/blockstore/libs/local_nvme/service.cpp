#include "service.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/fs.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] TResultOrError<TVector<NProto::TNVMeDevice>> LoadNVMeDevices(
    const TString& path)
{
    return SafeExecute<TResultOrError<TVector<NProto::TNVMeDevice>>>(
        [&]
        {
            if (!NFs::Exists(path)) {
                return TVector<NProto::TNVMeDevice>{};
            }

            NProto::TNVMeDeviceList proto;
            ParseProtoTextFromFileRobust(path, proto);

            return TVector<NProto::TNVMeDevice>(
                std::make_move_iterator(proto.MutableDevices()->begin()),
                std::make_move_iterator(proto.MutableDevices()->end()));
        });
}

////////////////////////////////////////////////////////////////////////////////

class TLocalNVMeService final: public ILocalNVMeService
{
private:
    const TString DeviceListFilePath;
    const ILoggingServicePtr Logging;

    TLog Log;

    TVector<NProto::TNVMeDevice> Devices;

public:
    TLocalNVMeService(TString deviceListFilePath, ILoggingServicePtr logging)
        : DeviceListFilePath(std::move(deviceListFilePath))
        , Logging(std::move(logging))
    {}

    void Start() final
    {
        Log = Logging->CreateLog("BLOCKSTORE_LOCAL_NVME");

        DiscoverNVMeDevices();
    }

    void Stop() final
    {}

    [[nodiscard]] TVector<NProto::TNVMeDevice> GetNVMeDevices() const final
    {
        return Devices;
    }

    [[nodiscard]] TFuture<NCloud::NProto::TError> ResetNVMeDevice(
        const TString& serialNumber) const final
    {
        auto* device = FindIfPtr(
            Devices,
            [&](const auto& d) { return d.GetSerialNumber() == serialNumber; });

        if (!device) {
            return MakeFuture(MakeError(
                E_NOT_FOUND,
                TStringBuilder()
                    << "Device " << serialNumber.Quote() << " not found"));
        }

        STORAGE_INFO("Reset NVMe device " << serialNumber.Quote() << " ...");

        // TODO: ...

        return MakeFuture(NProto::TError());
    }

private:
    void DiscoverNVMeDevices()
    {
        if (DeviceListFilePath.empty()) {
            return;
        }

        auto [devices, error] = LoadNVMeDevices(DeviceListFilePath);
        if (HasError(error)) {
            STORAGE_ERROR(
                "Can't load NVMe devices from the cache: "
                << FormatError(error));
            return;
        }

        Devices = std::move(devices);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TLocalNVMeServiceStub final: public ILocalNVMeService
{
    void Start() final
    {}

    void Stop() final
    {}

    [[nodiscard]] TVector<NProto::TNVMeDevice> GetNVMeDevices() const final
    {
        return {};
    }

    [[nodiscard]] TFuture<NCloud::NProto::TError> ResetNVMeDevice(
        const TString& serialNumber) const final
    {
        Y_UNUSED(serialNumber);

        return MakeFuture(NProto::TError());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ILocalNVMeServicePtr CreateLocalNVMeService(
    TString deviceListFilePath,
    ILoggingServicePtr logging)
{
    return std::make_shared<TLocalNVMeService>(
        std::move(deviceListFilePath),
        std::move(logging));
}

ILocalNVMeServicePtr CreateLocalNVMeServiceStub()
{
    return std::make_shared<TLocalNVMeServiceStub>();
}

}   // namespace NCloud::NBlockStore::NStorage
