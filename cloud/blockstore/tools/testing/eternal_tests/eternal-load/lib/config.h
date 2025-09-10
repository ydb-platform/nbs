#pragma once

#include "public.h"

#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config/config.pb.h>

#include <util/stream/file.h>

namespace NCloud::NBlockStore {


////////////////////////////////////////////////////////////////////////////////

struct IConfigHolder
{
    virtual TTestConfig& GetConfig() = 0;

    virtual void DumpConfig(const TString& filePath) = 0;

    virtual ~IConfigHolder() = default;
};


////////////////////////////////////////////////////////////////////////////////

IConfigHolderPtr CreateTestConfig(
    const TString& filePath,
    ui64 fileSize,
    ui16 ioDepth,
    ui64 blockSize,
    ui16 writeRate,
    ui64 requestBlockCount,
    ui64 writeParts,
    TString alternatingPhase,
    ui64 maxWriteRequestCount,
    ui64 minReadSize,
    ui64 maxReadSize,
    ui64 minWriteSize,
    ui64 maxWriteSize,
    ui64 minRegionSize,
    ui64 maxRegionSize
);

IConfigHolderPtr CreateTestConfig(const TString& filePath);

}   // namespace NCloud::NBlockStore
