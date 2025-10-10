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

struct TCreateTestConfigArguments
{
    TString FilePath;
    ui64 FileSize = 0;
    ui16 IoDepth = 0;
    ui64 BlockSize = 0;
    ui16 WriteRate = 0;
    ui64 RequestBlockCount = 0;
    ui64 WriteParts = 0;
    TString AlternatingPhase = "";
    ui64 MaxWriteRequestCount = 0;

    // Arguments for unaligned test scenario
    ui64 MinReadByteCount = 0;
    ui64 MaxReadByteCount = 0;
    ui64 MinWriteByteCount = 0;
    ui64 MaxWriteByteCount = 0;
    ui64 MinRegionByteCount = 0;
    ui64 MaxRegionByteCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

IConfigHolderPtr CreateTestConfig(const TCreateTestConfigArguments& args);
IConfigHolderPtr LoadTestConfig(const TString& filePath);

}   // namespace NCloud::NBlockStore
