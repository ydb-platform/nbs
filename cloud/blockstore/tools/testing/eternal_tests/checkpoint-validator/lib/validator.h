#pragma once

#include "public.h"

#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config.h>
#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/test_executor.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct ICheckpointValidator: public IOutputStream
{
    virtual bool GetResult() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

TValidatorPtr CreateValidator(const TTestConfig& config, const TLog& log);

}   // namespace NCloud::NBlockStore
