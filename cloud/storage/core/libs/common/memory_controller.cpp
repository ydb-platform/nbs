#include "memory_controller.h"

#include "numeric.h"
#include "verify.h"

#include <util/generic/strbuf.h>

#include <atomic>
#include <memory>
#include <utility>

namespace NCloud {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf MemoryControllerEntityType = "MemoryController";
constexpr TStringBuf MemoryControllerEntityId = "file_map";

////////////////////////////////////////////////////////////////////////////////

class TMemoryController final: public IMemoryController
{
private:
    const TMemoryControllerConfig Config;
    const ui64 FileMapLimit;
    std::atomic<ui64> FileMapMemoryUsage = 0;

public:
    explicit TMemoryController(TMemoryControllerConfig config)
        : Config(std::move(config))
        , FileMapLimit(CalcFileMapLimit(Config))
    {}

    [[nodiscard]] bool CanIncreaseFileMapUsage(ui64 increaseSize) const override
    {
        const ui64 current = FileMapMemoryUsage.load(std::memory_order_acquire);
        return current <= FileMapLimit &&
               increaseSize <= FileMapLimit - current;
    }

    void IncreaseFileMapUsage(ui64 diffSize) override
    {
        if (!diffSize) {
            return;
        }

        FileMapMemoryUsage.fetch_add(diffSize, std::memory_order_release);
    }

    void DecreaseFileMapUsage(ui64 diffSize) override
    {
        if (!diffSize) {
            return;
        }

        const ui64 previous =
            FileMapMemoryUsage.fetch_sub(diffSize, std::memory_order_release);
        STORAGE_VERIFY(
            previous >= diffSize,
            MemoryControllerEntityType,
            MemoryControllerEntityId);
    }

private:
    static ui64 CalcFileMapLimit(const TMemoryControllerConfig& config)
    {
        STORAGE_VERIFY(
            config.MemoryLimit,
            MemoryControllerEntityType,
            MemoryControllerEntityId);

        return PercentOf(config.MemoryLimit, config.TmpfsMemoryLimitPercent);
    }
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IMemoryControllerPtr CreateMemoryController(TMemoryControllerConfig config)
{
    if (!config.MemoryLimit) {
        return nullptr;
    }

    return std::make_shared<TMemoryController>(std::move(config));
}

}   // namespace NCloud
