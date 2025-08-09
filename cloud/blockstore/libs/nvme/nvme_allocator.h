#pragma once

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <util/folder/path.h>
#include <util/generic/string.h>

#include <memory>

namespace NCloud::NBlockStore::NNvme {

////////////////////////////////////////////////////////////////////////////////

class TNvmeAllocator
{
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TNvmeAllocator(
        ILoggingServicePtr logging,
        TFsPath devicesFolder,
        TFsPath locksFolder);

    TNvmeAllocator(TNvmeAllocator&&);
    TNvmeAllocator(const TNvmeAllocator&) = delete;

    TNvmeAllocator& operator=(TNvmeAllocator&&) = delete;
    TNvmeAllocator& operator=(const TNvmeAllocator&) = delete;

    ~TNvmeAllocator();

    [[nodiscard]] size_t AvailableDevicesCount() const;

    TResultOrError<TFsPath> AcquireNvme();
    TResultOrError<TFsPath> AcquireNvme(TDuration timeout, TDuration delay);
    NProto::TError ReleaseNvme(const TFsPath& path);
};

}   // namespace NCloud::NBlockStore::NNvme
