#include "nvme.h"

namespace NCloud::NBlockStore::NNvme {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TNvmeManagerStub final
    : public INvmeManager
{
private:
    bool IsDeviceSsd;

public:
    TNvmeManagerStub(bool isDeviceSsd)
        : IsDeviceSsd(isDeviceSsd)
    {}

    TFuture<NProto::TError> Format(
        const TString& path,
        nvme_secure_erase_setting ses) override
    {
        Y_UNUSED(path);
        Y_UNUSED(ses);

        return MakeFuture<NProto::TError>();
    }

    TFuture<NProto::TError> Deallocate(
        const TString& path,
        ui64 offsetBytes,
        ui64 sizeBytes) override
    {
        Y_UNUSED(path);
        Y_UNUSED(offsetBytes);
        Y_UNUSED(sizeBytes);

        return MakeFuture<NProto::TError>();
    }

    TResultOrError<TString> GetSerialNumber(const TString& path) override
    {
        Y_UNUSED(path);

        return TString {};
    }

    TResultOrError<bool> IsSsd(const TString& path) override
    {
        Y_UNUSED(path);

        return IsDeviceSsd;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

INvmeManagerPtr CreateNvmeManagerStub(bool isDeviceSsd)
{
    return std::make_shared<TNvmeManagerStub>(isDeviceSsd);
}

}   // namespace NCloud::NBlockStore::NNvme
