#include "file_map_memory_limiter.h"

#include "verify.h"

#include <util/generic/strbuf.h>
#include <util/system/yassert.h>

#include <atomic>
#include <memory>

namespace NCloud {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf FileMapMemoryLimiterEntityType = "FileMapMemoryLimiter";

////////////////////////////////////////////////////////////////////////////////

class TFileMapMemoryLimiter final: public IFileMapMemoryLimiter
{
private:
    const ui64 FileMapMemoryLimit;
    std::atomic<ui64> FileMapMemoryUsage = 0;

public:
    explicit TFileMapMemoryLimiter(TFileMapMemoryLimiterConfig config)
        : FileMapMemoryLimit(config.FileMapMemoryLimit)
    {
        STORAGE_VERIFY(
            FileMapMemoryLimit,
            FileMapMemoryLimiterEntityType,
            FileMapMemoryLimiterEntityType);
    }

    [[nodiscard]] bool CanIncrease(ui64 increaseSize) const override
    {
        const ui64 current = FileMapMemoryUsage.load(std::memory_order_acquire);
        return current <= FileMapMemoryLimit &&
               increaseSize <= FileMapMemoryLimit - current;
    }

    void Increase(ui64 diffSize) override
    {
        if (!diffSize) {
            return;
        }

        FileMapMemoryUsage.fetch_add(diffSize, std::memory_order_release);
    }

    void Decrease(ui64 diffSize) override
    {
        if (!diffSize) {
            return;
        }

        const ui64 previous =
            FileMapMemoryUsage.fetch_sub(diffSize, std::memory_order_release);
        STORAGE_VERIFY(
            previous >= diffSize,
            FileMapMemoryLimiterEntityType,
            FileMapMemoryLimiterEntityType);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFileMapMemoryLimiterStub final: public IFileMapMemoryLimiter
{
public:
    [[nodiscard]] bool CanIncrease(ui64 increaseSize) const override
    {
        Y_UNUSED(increaseSize);
        return true;
    }

    void Increase(ui64 diffSize) override
    {
        Y_UNUSED(diffSize);
    }

    void Decrease(ui64 diffSize) override
    {
        Y_UNUSED(diffSize);
    }
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileMapMemoryLimiterPtr CreateFileMapMemoryLimiter(
    TFileMapMemoryLimiterConfig config)
{
    if (!config.FileMapMemoryLimit) {
        return CreateFileMapMemoryLimiterStub();
    }

    return std::make_shared<TFileMapMemoryLimiter>(config);
}

IFileMapMemoryLimiterPtr CreateFileMapMemoryLimiterStub()
{
    return std::make_shared<TFileMapMemoryLimiterStub>();
}

}   // namespace NCloud
