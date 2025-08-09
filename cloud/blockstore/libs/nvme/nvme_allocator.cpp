#include "nvme_allocator.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/folder/iterator.h>
#include <util/generic/hash.h>
#include <util/random/random.h>
#include <util/system/file_lock.h>

#include <mutex>

namespace NCloud::NBlockStore::NNvme {

namespace {

////////////////////////////////////////////////////////////////////////////////

TVector<TString> CollectDevices(const TFsPath& folder)
{
    TVector<TString> devices;

    for (const auto& entry: TDirIterator {folder}) {
        if (!entry.fts_namelen) {
            continue;
        }

        TStringBuf name {entry.fts_name, entry.fts_namelen};
        if (!name.starts_with("nvme")) {
            continue;
        }

        devices.push_back(ToString(name));
    }

    return devices;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TNvmeAllocator::TImpl
{
private:
    const ILoggingServicePtr Logging;
    const TFsPath DevicesFolder;
    const TFsPath LocksFolder;

    TLog Log;

    std::mutex Mutex;

    TVector<TString> AvailableDevices;
    THashMap<TString, TFileLock> AcquiredDevices;

public:
    TImpl(
            ILoggingServicePtr logging,
            TFsPath devicesFolder,
            TFsPath locksFolder)
        : Logging(std::move(logging))
        , DevicesFolder(std::move(devicesFolder))
        , LocksFolder(std::move(locksFolder))
        , Log(Logging->CreateLog("NVME"))
        , AvailableDevices(CollectDevices(DevicesFolder))
    {}

    ~TImpl()
    {
        ReleaseAll();
    }

    size_t AvailableDevicesCount() const
    {
        return AvailableDevices.size();
    }

    TResultOrError<TFsPath> AcquireNvme()
    {
        std::unique_lock lock{Mutex};

        return AcquireNvmeNoLock();
    }

    TResultOrError<TFsPath> AcquireNvme(TDuration timeout, TDuration delay)
    {
        TInstant deadline = timeout.ToDeadLine();
        while (Now() < deadline) {
            auto r = AcquireNvme();
            if (!HasError(r)) {
                return r;
            }
            STORAGE_DEBUG("Wait...");
            Sleep(delay);
        }

        return AcquireNvme();
    }

    TResultOrError<TFsPath> AcquireNvmeNoLock()
    {
        if (AcquiredDevices.size() >= AvailableDevices.size()) {
            return MakeError(E_TRY_AGAIN, "All NVMe devices are taken");
        }

        const ui64 salt = RandomNumber<ui64>();
        for (ui64 attempt = 0; attempt != AvailableDevices.size(); ++attempt) {
            const ui64 i = (salt + attempt) % AvailableDevices.size();
            const auto& name = AvailableDevices[i];

            TFileLock filelock{LocksFolder / name, EFileLockType::Exclusive};
            if (!filelock.TryAcquire()) {
                continue;
            }

            STORAGE_INFO("NVMe device " << name.Quote() << " acquired");

            TFsPath path = DevicesFolder / name;
            AcquiredDevices.emplace(path, std::move(filelock));
            return path;
        }

        return MakeError(E_TRY_AGAIN, "Failed to find available NVMe device");
    }

    NProto::TError ReleaseNvme(const TFsPath& path)
    {
        std::unique_lock lock{Mutex};

        auto it = AcquiredDevices.find(static_cast<const TString&>(path));
        if (it == AcquiredDevices.end()) {
            return MakeError(E_ARGUMENT, "Unknown device");
        }

        STORAGE_INFO("Release NVMe device: " << path.Basename().Quote());

        it->second.Release();
        AcquiredDevices.erase(it);

        return {};
    }

    void ReleaseAll()
    {
        STORAGE_INFO("Release all NVMe devices");

        std::unique_lock lock{Mutex};
        for (auto [path, filelock]: AcquiredDevices) {
            STORAGE_INFO(
                "Release NVMe device: " << TFsPath{path}.Basename().Quote());
            filelock.Release();
        }
        AcquiredDevices.clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

TNvmeAllocator::TNvmeAllocator(
        ILoggingServicePtr logging,
        TFsPath devicesFolder,
        TFsPath locksFolder)
    : Impl(std::make_unique<TImpl>(
        std::move(logging),
        std::move(devicesFolder),
        std::move(locksFolder)))
{}

TNvmeAllocator::~TNvmeAllocator() = default;
TNvmeAllocator::TNvmeAllocator(TNvmeAllocator&&) = default;

TResultOrError<TFsPath> TNvmeAllocator::AcquireNvme()
{
    return Impl->AcquireNvme();
}

TResultOrError<TFsPath> TNvmeAllocator::AcquireNvme(
    TDuration timeout,
    TDuration delay)
{
    return Impl->AcquireNvme(timeout, delay);
}

NProto::TError TNvmeAllocator::ReleaseNvme(const TFsPath& path)
{
    return Impl->ReleaseNvme(path);
}

size_t TNvmeAllocator::AvailableDevicesCount() const
{
    return Impl->AvailableDevicesCount();
}

}   // namespace NCloud::NBlockStore::NNvme
