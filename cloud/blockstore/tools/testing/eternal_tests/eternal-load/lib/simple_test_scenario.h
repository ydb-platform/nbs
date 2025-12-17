#pragma once

#include "public.h"
#include "test_executor.h"

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
