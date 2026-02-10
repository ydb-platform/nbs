#include "device_provider.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TFileNVMeDeviceProvider final: public ILocalNVMeDeviceProvider
{
private:
    const TString Path;

public:
    explicit TFileNVMeDeviceProvider(TString path)
        : Path(std::move(path))
    {}

    [[nodiscard]] auto ListNVMeDevices()
        -> TFuture<TVector<NProto::TNVMeDevice>> final
    {
        NProto::TNVMeDeviceList list;
        ParseProtoTextFromFile(Path, list);

        return MakeFuture(
            TVector<NProto::TNVMeDevice>{
                std::make_move_iterator(list.MutableDevices()->begin()),
                std::make_move_iterator(list.MutableDevices()->end())});
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLocalNVMeDeviceProviderStub final: public ILocalNVMeDeviceProvider
{
public:
    [[nodiscard]] auto ListNVMeDevices()
        -> TFuture<TVector<NProto::TNVMeDevice>> final
    {
        return MakeFuture(TVector<NProto::TNVMeDevice>{});
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ILocalNVMeDeviceProviderPtr CreateFileNVMeDeviceProvider(const TString& path)
{
    return std::make_shared<TFileNVMeDeviceProvider>(path);
}

ILocalNVMeDeviceProviderPtr CreateLocalNVMeDeviceProviderStub()
{
    return std::make_shared<TLocalNVMeDeviceProviderStub>();
}

}   // namespace NCloud::NBlockStore
