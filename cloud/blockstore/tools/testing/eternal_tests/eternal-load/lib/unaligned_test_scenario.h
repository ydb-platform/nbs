#pragma once

#include "public.h"
#include "test_executor.h"

namespace NCloud::NBlockStore::NTesting {

////////////////////////////////////////////////////////////////////////////////

ITestScenarioPtr CreateUnalignedTestScenario(
    IConfigHolderPtr configHolder,
    const TLog& log);

}   // namespace NCloud::NBlockStore::NTesting
