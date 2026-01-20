#include "local_nvme_mock.h"

#include <cloud/blockstore/libs/local_nvme/service.h>

#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TLocalNVMeServiceMock final
    : public ILocalNVMeService
{
private:
    const TVector<NProto::TNVMeDevice> Devices;

public:
    explicit TLocalNVMeServiceMock(TVector<NProto::TNVMeDevice> devices)
        : Devices(std::move(devices))
    {}

    void Start() final
    {}

    void Stop() final
    {}

    [[nodiscard]] auto ListNVMeDevices() const
        -> TResultOrError<TVector<NProto::TNVMeDevice>> final
    {
        return Devices;
    }

    [[nodiscard]] auto AcquireNVMeDevice(const TString& serialNumber) const
        -> TFuture<NProto::TError> final
    {
        const auto* disk = FindIfPtr(
            Devices,
            [&](const auto& disk)
            { return disk.GetSerialNumber() == serialNumber; });

        if (disk) {
            return MakeFuture(NProto::TError());
        }

        return MakeFuture(MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "Disk " << serialNumber.Quote() << " not found"));
    }

    [[nodiscard]] auto ReleaseNVMeDevice(const TString& serialNumber) const
        -> TFuture<NProto::TError> final
    {
        const auto* disk = FindIfPtr(
            Devices,
            [&](const auto& disk)
            { return disk.GetSerialNumber() == serialNumber; });

        if (disk) {
            return MakeFuture(NProto::TError());
        }

        return MakeFuture(MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "Disk " << serialNumber.Quote() << " not found"));
    }
};

}   //namespace

////////////////////////////////////////////////////////////////////////////////

ILocalNVMeServicePtr CreateLocalNVMeServiceMock(
    TVector<NProto::TNVMeDevice> devices)
{
    return std::make_shared<TLocalNVMeServiceMock>(std::move(devices));
}

}   // namespace NCloud::NBlockStore
