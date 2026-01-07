#pragma once

#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/public.h>
#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/test_executor.h>

namespace NCloud::NBlockStore::NTesting {

////////////////////////////////////////////////////////////////////////////////

ITestScenarioPtr CreateAlignedTestScenario(
    IConfigHolderPtr configHolder,
    const TLog& log);

}   // namespace NCloud::NBlockStore::NTesting
