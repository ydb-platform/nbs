#include "nvme.h"

namespace NCloud::NBlockStore::NNvme {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TNvmeManagerStub final
    : public INvmeManager
{
public:
    TFuture<NProto::TError> Format(
        const TString& path,
        nvme_secure_erase_setting ses) override
    {
        Y_UNUSED(path);
        Y_UNUSED(ses);

        return MakeFuture<NProto::TError>();
    }

    TResultOrError<TString> GetSerialNumber(const TString& path) override
    {
        Y_UNUSED(path);

        return TString {};
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

INvmeManagerPtr CreateNvmeManagerStub()
{
    return std::make_shared<TNvmeManagerStub>();
}

}   // namespace NCloud::NBlockStore::NNvme
