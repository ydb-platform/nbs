#pragma once

#include <util/system/defaults.h>

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TBlockData
{
    ui64 RequestNumber = 0;
    ui64 PartNumber = 0;
    ui64 BlockIndex = 0;
    ui64 RangeIdx = 0;
    ui64 RequestTimestamp = 0;
    ui64 TestTimestamp = 0;
    ui64 TestId = 0;
    ui64 Checksum = 0;

    bool operator<(const TBlockData& r) const
    {
        return memcmp(this, &r, sizeof(TBlockData)) < 0;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct IRequestGenerator;
using IRequestGeneratorPtr = std::shared_ptr<IRequestGenerator>;

struct ITestExecutor;
using ITestExecutorPtr = std::shared_ptr<ITestExecutor>;

struct IConfigHolder;
using IConfigHolderPtr = std::shared_ptr<IConfigHolder>;

}   // namespace NCloud::NBlockStore
