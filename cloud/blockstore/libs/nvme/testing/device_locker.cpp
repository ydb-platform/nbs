#include "device_locker.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/hash.h>
#include <util/random/random.h>
#include <util/system/env.h>
#include <util/system/file_lock.h>

#include <filesystem>
#include <mutex>
#include <regex>

namespace NCloud::NBlockStore::NNvme {

namespace {

////////////////////////////////////////////////////////////////////////////////

TVector<TString> CollectDevices(const TFsPath& folder, TStringBuf nameMask)
{
    const std::regex re{nameMask.data(), nameMask.size()};

    TVector<TString> devices;

    std::filesystem::directory_iterator it{folder.c_str()};
    std::filesystem::directory_iterator end;

    for (; it != end; ++it) {
        const auto& filename = it->path().filename().string();
        if (std::regex_match(filename, re)) {
            devices.emplace_back(filename);
        }
    }

    return devices;
}

////////////////////////////////////////////////////////////////////////////////

struct TRetryState
{
    TDuration SleepDuration;
    ui32 Attempts = 0;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

class TDeviceLocker::TImpl
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
            TFsPath locksFolder,
            TStringBuf nameMask)
        : Logging(std::move(logging))
        , DevicesFolder(std::move(devicesFolder))
        , LocksFolder(std::move(locksFolder))
        , Log(Logging->CreateLog("DEVICE_LOCKER"))
        , AvailableDevices(CollectDevices(DevicesFolder, nameMask))
    {}

    ~TImpl()
    {
        ReleaseAll();
    }

    size_t AvailableDevicesCount() const
    {
        return AvailableDevices.size();
    }

    TResultOrError<TFsPath> AcquireDevice(const TRetryOptions& retryOptions)
    {
        STORAGE_INFO("Acquiring device...");

        TRetryState retryState{.SleepDuration = retryOptions.SleepDuration};

        for (;;) {
            auto r = AcquireDevice();
            if (!HasError(r) || retryState.Attempts >= retryOptions.RetryCount)
            {
                return r;
            }

            ++retryState.Attempts;
            STORAGE_DEBUG("Wait...");
            Sleep(retryState.SleepDuration);

            retryState.SleepDuration += retryOptions.SleepIncrement;
        }
    }

    TResultOrError<TFsPath> AcquireDevice()
    {
        std::unique_lock lock{Mutex};

        if (AcquiredDevices.size() >= AvailableDevices.size()) {
            return MakeError(E_TRY_AGAIN, "All devices are taken");
        }

        const ui64 salt = RandomNumber<ui64>();
        for (ui64 attempt = 0; attempt != AvailableDevices.size(); ++attempt) {
            const ui64 i = (salt + attempt) % AvailableDevices.size();
            const auto& name = AvailableDevices[i];

            TFileLock filelock{LocksFolder / name, EFileLockType::Exclusive};
            if (!filelock.TryAcquire()) {
                continue;
            }

            TFsPath path = DevicesFolder / name;

            STORAGE_INFO("Device " << path.GetPath().Quote() << " acquired");

            AcquiredDevices.emplace(path, std::move(filelock));
            return std::move(path);
        }

        return MakeError(E_TRY_AGAIN, "Failed to find available device");
    }

    NProto::TError ReleaseDevice(const TFsPath& path)
    {
        std::unique_lock lock{Mutex};

        auto it = AcquiredDevices.find(path.GetPath());
        if (it == AcquiredDevices.end()) {
            return MakeError(E_ARGUMENT, "Unknown device");
        }

        STORAGE_INFO("Release device: " << path.GetPath().Quote());

        it->second.Release();
        AcquiredDevices.erase(it);

        return {};
    }

    void ReleaseAll()
    {
        STORAGE_INFO("Release all devices");

        std::unique_lock lock{Mutex};

        for (auto& [path, filelock]: AcquiredDevices) {
            STORAGE_INFO("Release device: " << path.Quote());
            filelock.Release();
        }
        AcquiredDevices.clear();
    }
};

////////////////////////////////////////////////////////////////////////////////

TDeviceLocker::TDeviceLocker() = default;

TDeviceLocker::TDeviceLocker(
        ILoggingServicePtr logging,
        TFsPath devicesFolder,
        TFsPath locksFolder,
        TStringBuf nameMask)
    : Impl(std::make_unique<TImpl>(
        std::move(logging),
        std::move(devicesFolder),
        std::move(locksFolder),
        nameMask))
{}

TDeviceLocker::~TDeviceLocker() = default;
TDeviceLocker::TDeviceLocker(TDeviceLocker&&) noexcept = default;
TDeviceLocker& TDeviceLocker::operator=(
    TDeviceLocker&&) noexcept = default;

TResultOrError<TFsPath> TDeviceLocker::AcquireDevice(
    const TRetryOptions& retryOptions)
{
    if (!Impl) {
        return MakeError(E_INVALID_STATE);
    }

    return Impl->AcquireDevice(retryOptions);
}

NProto::TError TDeviceLocker::ReleaseDevice(const TFsPath& path)
{
    if (!Impl) {
        return MakeError(E_INVALID_STATE);
    }

    return Impl->ReleaseDevice(path);
}

size_t TDeviceLocker::AvailableDevicesCount() const
{
    if (!Impl) {
        return 0;
    }

    return Impl->AvailableDevicesCount();
}

TDeviceLocker TDeviceLocker::CreateFromEnv(
    ILoggingServicePtr logging,
    TStringBuf nameMask)
{
    auto devicesFolder = GetEnv("NVME_LOOP_DEVICES_FOLDER");
    Y_ENSURE(
        devicesFolder,
        "Environment variable 'NVME_LOOP_DEVICES_FOLDER' is not set");

    auto locksFolder = GetEnv("NVME_LOOP_LOCKS_FOLDER");
    Y_ENSURE(
        locksFolder,
        "Environment variable 'NVME_LOOP_LOCKS_FOLDER' is not set");

    return {std::move(logging), devicesFolder, locksFolder, nameMask};
}

}   // namespace NCloud::NBlockStore::NNvme
