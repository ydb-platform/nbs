#include "service.h"

#include "config.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/fs.h>

namespace NCloud::NBlockStore {

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
    const TLocalNVMeConfigPtr Config;
    const ILoggingServicePtr Logging;

    TLog Log;

    TVector<NProto::TNVMeDevice> Devices;

public:
    TLocalNVMeService(TLocalNVMeConfigPtr config, ILoggingServicePtr logging)
        : Config(std::move(config))
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

    [[nodiscard]] TFuture<NCloud::NProto::TError> AcquireNVMeDevice(
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

        STORAGE_INFO("Acquire NVMe device " << serialNumber.Quote());

        return MakeFuture(NProto::TError());
    }

    [[nodiscard]] TFuture<NCloud::NProto::TError> ReleaseNVMeDevice(
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

        STORAGE_INFO("Release NVMe device " << serialNumber.Quote());

        return MakeFuture(NProto::TError());
    }

private:
    void DiscoverNVMeDevices()
    {
        if (Config->GetNVMeDevicesCacheFile().empty()) {
            return;
        }

        auto [devices, error] =
            LoadNVMeDevices(Config->GetNVMeDevicesCacheFile());
        if (HasError(error)) {
            STORAGE_ERROR(
                "Can't load NVMe devices from the cache: "
                << FormatError(error));
            return;
        }

        Devices = std::move(devices);

        STORAGE_INFO(
            "Discovered: " <<
            [&]
            {
                TStringStream ss;
                for (size_t i = 0; i != Devices.size(); ++i) {
                    if (i) {
                        ss << ", ";
                    }
                    ss << Devices[i];
                }
                return ss.Str();
            }() << " ...");
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLocalNVMeServiceStub final: public ILocalNVMeService
{
private:
    const ILoggingServicePtr Logging;

    TLog Log;

public:
    explicit TLocalNVMeServiceStub(ILoggingServicePtr logging)
        : Logging(std::move(logging))
    {}

    void Start() final
    {
        Log = Logging->CreateLog("BLOCKSTORE_LOCAL_NVME");
    }

    void Stop() final
    {}

    [[nodiscard]] TVector<NProto::TNVMeDevice> GetNVMeDevices() const final
    {
        return {};
    }

    [[nodiscard]] TFuture<NCloud::NProto::TError> AcquireNVMeDevice(
        const TString& serialNumber) const final
    {
        STORAGE_INFO("Acquire NVMe device " << serialNumber.Quote());

        return MakeFuture(MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "Device " << serialNumber.Quote() << " not found"));
    }

    [[nodiscard]] TFuture<NCloud::NProto::TError> ReleaseNVMeDevice(
        const TString& serialNumber) const final
    {
        STORAGE_INFO("Release NVMe device " << serialNumber.Quote());

        return MakeFuture(MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "Device " << serialNumber.Quote() << " not found"));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ILocalNVMeServicePtr CreateLocalNVMeService(
    TLocalNVMeConfigPtr config,
    ILoggingServicePtr logging)
{
    return std::make_shared<TLocalNVMeService>(
        std::move(config),
        std::move(logging));
}

ILocalNVMeServicePtr CreateLocalNVMeServiceStub(ILoggingServicePtr logging)
{
    return std::make_shared<TLocalNVMeServiceStub>(std::move(logging));
}

}   // namespace NCloud::NBlockStore
