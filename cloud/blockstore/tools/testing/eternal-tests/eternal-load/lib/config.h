#pragma once

#include "public.h"

#include <cloud/blockstore/tools/testing/eternal-tests/eternal-load/lib/config.sc.h>

#include <library/cpp/config/config.h>
#include <library/cpp/json/domscheme_traits.h>

#include <util/stream/file.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

using TTestConfig = TTestConfigDesc<TJsonTraits>;

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
    ui64 writeParts
);

IConfigHolderPtr CreateTestConfig(const TString& filePath);

}   // namespace NCloud::NBlockStore

