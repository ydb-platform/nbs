#pragma once

#include <util/system/types.h>

#include <memory>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct IFileMapMemoryLimiter
{
    virtual ~IFileMapMemoryLimiter() = default;

    // The check is advisory, not a reservation. Callers account memory only
    // after a file mapping actually grows, so concurrent growth can temporarily
    // overshoot the limit; the limit gates future growth attempts.
    [[nodiscard]] virtual bool CanIncrease(ui64 increaseSize) const = 0;
    virtual void Increase(ui64 diffSize) = 0;
    virtual void Decrease(ui64 diffSize) = 0;
};

using IFileMapMemoryLimiterPtr = std::shared_ptr<IFileMapMemoryLimiter>;

////////////////////////////////////////////////////////////////////////////////

struct TFileMapMemoryLimiterConfig
{
    ui64 FileMapMemoryLimit = 0;
};

IFileMapMemoryLimiterPtr CreateFileMapMemoryLimiter(
    TFileMapMemoryLimiterConfig config);

IFileMapMemoryLimiterPtr CreateFileMapMemoryLimiterStub();

}   // namespace NCloud
