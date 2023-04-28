#pragma once

#include "public.h"

#include <cloud/blockstore/tools/testing/eternal-tests/eternal-load/lib/config.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/config/config.h>
#include <library/cpp/config/domscheme.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/event.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct ITestExecutor
{
    virtual ~ITestExecutor() = default;

    virtual bool Run() = 0;

    virtual void Stop() = 0;
};

////////////////////////////////////////////////////////////////////////////////

ITestExecutorPtr CreateTestExecutor(
    IConfigHolderPtr configHolder,
    const TLog& log);

}   // namespace NCloud::NBlockStore
