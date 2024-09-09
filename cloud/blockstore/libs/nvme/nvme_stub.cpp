#include "nvme_stub.h"

namespace NCloud::NBlockStore::NNvme {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TNvmeManagerStub final
    : public INvmeManager
{
private:
    bool IsDeviceSsd;
    TNvmeDeallocateHistoryPtr DeallocateHistory;

public:
    TNvmeManagerStub(
            bool isDeviceSsd,
            TNvmeDeallocateHistoryPtr deallocateHistory)
        : IsDeviceSsd(isDeviceSsd)
        , DeallocateHistory(std::move(deallocateHistory))
    {}

    TFuture<NProto::TError> Format(
        const TString& path,
        nvme_secure_erase_setting ses) override
    {
        Y_UNUSED(path);
        Y_UNUSED(ses);

        return MakeFuture(MakeError(S_OK));
    }

    TFuture<NProto::TError> Deallocate(
        const TString& path,
        ui64 offsetBytes,
        ui64 sizeBytes) override
    {
        Y_UNUSED(path);

        if (DeallocateHistory) {
            DeallocateHistory->emplace_back(offsetBytes, sizeBytes);
        }

        return MakeFuture(MakeError(S_OK));
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

INvmeManagerPtr CreateNvmeManagerStub(
    bool isDeviceSsd,
    TNvmeDeallocateHistoryPtr deallocateHistory)
{
    return std::make_shared<TNvmeManagerStub>(
        isDeviceSsd,
        std::move(deallocateHistory));
}

}   // namespace NCloud::NBlockStore::NNvme
