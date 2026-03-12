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

    void Start() final
    {}

    void Stop() final
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

    NProto::TError Sanitize(const TString& ctrlPath) override
    {
        Y_UNUSED(ctrlPath);

        return {};
    }

    TResultOrError<TSanitizeStatus> GetSanitizeStatus(
        const TString& ctrlPath) override
    {
        Y_UNUSED(ctrlPath);

        return TSanitizeStatus{};
    }

    NProto::TError ResetToSingleNamespace(const TString& ctrlPath) final
    {
        Y_UNUSED(ctrlPath);

        return {};
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
