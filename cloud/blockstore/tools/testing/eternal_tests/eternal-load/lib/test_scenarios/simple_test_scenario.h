#pragma once

#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/public.h>
#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/test_executor.h>

namespace NCloud::NBlockStore::NTesting {

////////////////////////////////////////////////////////////////////////////////

enum class ESimpleTestScenarioMode
{
    Sequential,
    Random
};

////////////////////////////////////////////////////////////////////////////////

ITestScenarioPtr CreateSimpleTestScenario(
    ESimpleTestScenarioMode mode,
    IConfigHolderPtr configHolder,
    const TLog& log);

}   // namespace NCloud::NBlockStore::NTesting
