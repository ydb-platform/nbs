#include "service.h"

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore {

namespace {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

class TLocalNVMeServiceStub final: public ILocalNVMeService
{
public:
    void Start() final
    {}

    void Stop() final
    {}

    [[nodiscard]] auto ListNVMeDevices()
        -> TFuture<TResultOrError<TVector<NProto::TNVMeDevice>>> final
    {
        return MakeFuture<TResultOrError<TVector<NProto::TNVMeDevice>>>(
            TVector<NProto::TNVMeDevice>{});
    }

    [[nodiscard]] auto AcquireNVMeDevice(const TString& serialNumber)
        -> TFuture<NProto::TError> final
    {
        if (!serialNumber) {
            return MakeFuture(MakeError(E_ARGUMENT, "Serial number is empty"));
        }

        return MakeFuture(MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "Device " << serialNumber.Quote() << " not found"));
    }

    [[nodiscard]] auto ReleaseNVMeDevice(const TString& serialNumber)
        -> TFuture<NProto::TError> final
    {
        if (!serialNumber) {
            return MakeFuture(MakeError(E_ARGUMENT, "Serial number is empty"));
        }

        return MakeFuture(MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "Device " << serialNumber.Quote() << " not found"));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ILocalNVMeServicePtr CreateLocalNVMeServiceStub()
{
    return std::make_shared<TLocalNVMeServiceStub>();
}

}   // namespace NCloud::NBlockStore
