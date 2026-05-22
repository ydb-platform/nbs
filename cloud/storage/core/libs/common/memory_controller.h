#pragma once

#include <util/system/types.h>

#include <memory>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct IMemoryController
{
    virtual ~IMemoryController() = default;

    // The check is advisory, not a reservation. Callers account memory only
    // after a file mapping actually grows, so concurrent growth can temporarily
    // overshoot the limit; the limit gates future growth attempts.
    [[nodiscard]] virtual bool CanIncreaseFileMapUsage(
        ui64 increaseSize) const = 0;
    virtual void IncreaseFileMapUsage(ui64 diffSize) = 0;
    virtual void DecreaseFileMapUsage(ui64 diffSize) = 0;
};

using IMemoryControllerPtr = std::shared_ptr<IMemoryController>;

////////////////////////////////////////////////////////////////////////////////

struct TMemoryControllerConfig
{
    ui64 TmpfsMemoryLimit = 0;
};

// Returns nullptr when memory limiting is disabled.
IMemoryControllerPtr CreateMemoryController(
    TMemoryControllerConfig config = {});

}   // namespace NCloud
