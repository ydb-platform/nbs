#pragma once

#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <util/system/types.h>

#include <atomic>
#include <memory>
#include <optional>

namespace NCloud::NBlockStore::NStorage {

class TCounter
{
private:
    std::atomic<ui32> Value = 0;

public:

    explicit TCounter(ui32 value)
        : Value(value)
    {}

    ui32 GetValue() const
    {
        return Value.load();
    }

    std::optional<ui32> SafeIncrement()
    {
        auto lastValue = Value.load();
        while (lastValue != Max<ui32>()) {
            if (Value.compare_exchange_strong(lastValue, lastValue + 1)) {
                return lastValue + 1;
            }
        }

        return {};
    }
};

class TCommitIdGenerator
{
private:
    const ui32 Generation;
    TCounter Counter;

public:
    explicit TCommitIdGenerator(ui32 generation, ui32 value)
        : Generation(generation)
        , Counter(value)
    {}

    ui64 GetLastCommitId() const
    {
        return MakeCommitId(Generation, Counter.GetValue());
    }

    ui64 GenerateCommitId()
    {
        if (auto commitId = Counter.SafeIncrement()) {
            return MakeCommitId(Generation, *commitId);
        }
        return InvalidCommitId;
    }
};

using TCommitIdGeneratorPtr = std::shared_ptr<TCommitIdGenerator>;

}   // namespace NCloud::NBlockStore::NStorage
