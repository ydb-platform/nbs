#pragma once

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <util/datetime/base.h>
#include <util/folder/path.h>
#include <util/generic/string.h>

#include <memory>

namespace NCloud::NBlockStore::NNvme {

////////////////////////////////////////////////////////////////////////////////

/**
 * Thread-safe device locker for managing exclusive access to block devices in
 * tests.
 *
 * Provides exclusive locking mechanism for block devices (typically NVMe) using
 * file locking. The class scans a specified directory for devices matching a
 * given pattern and allows acquiring/releasing exclusive locks on them.
 * Supports automatic retry logic with configurable backoff for handling
 * contention scenarios.
 *
 */
class TDeviceLocker
{
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    struct TRetryOptions
    {
        ui32 RetryCount = 0;
        TDuration SleepDuration;
        TDuration SleepIncrement;
    };

    static inline const TRetryOptions DefaultRetryOptions = TRetryOptions{
        .RetryCount = 15,
        .SleepDuration = TDuration::Seconds(1),
        .SleepIncrement = {}};

    static constexpr TStringBuf DefaultNameMask = "nvme[0-9]n[0-9]+";

    TDeviceLocker();

    TDeviceLocker(
        ILoggingServicePtr logging,
        TFsPath devicesFolder,
        TFsPath locksFolder,
        TStringBuf nameMask = DefaultNameMask);

    TDeviceLocker(TDeviceLocker&&) noexcept;
    TDeviceLocker(const TDeviceLocker&) = delete;

    TDeviceLocker& operator=(TDeviceLocker&&) noexcept;
    TDeviceLocker& operator=(const TDeviceLocker&) = delete;

    ~TDeviceLocker();

    static TDeviceLocker CreateFromEnv(
        ILoggingServicePtr logging,
        TStringBuf nameMask = DefaultNameMask);

    [[nodiscard]] size_t AvailableDevicesCount() const;

    [[nodiscard]] TResultOrError<TFsPath> AcquireDevice(
        const TRetryOptions& retryOptions = DefaultRetryOptions);

    NProto::TError ReleaseDevice(const TFsPath& path);
};

}   // namespace NCloud::NBlockStore::NNvme
