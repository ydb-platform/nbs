#pragma once

#include "nvme.h"

namespace NCloud::NBlockStore::NNvme {

using namespace NThreading;

class TNvmeManager: public INvmeManager
{
private:
    ITaskQueuePtr Executor;
    TDuration Timeout;   // admin command timeout

    void FormatImpl(const TString& path, nvme_secure_erase_setting ses);

    void DeallocateImpl(const TString& path, ui64 offsetBytes, ui64 sizeBytes);

public:
    TNvmeManager(ITaskQueuePtr executor, TDuration timeout);
    ~TNvmeManager() override;

    TFuture<NProto::TError> Format(
        const TString& path,
        nvme_secure_erase_setting ses) override;
    TFuture<NProto::TError>
    Deallocate(const TString& path, ui64 offsetBytes, ui64 sizeBytes) override;
    TResultOrError<TString> GetSerialNumber(const TString& path) override;
    TResultOrError<bool> IsSsd(const TString& path) override;
};

}   // namespace NCloud::NBlockStore::NNvme
