#pragma once

#include <contrib/ydb/library/yql/core/cbo/cbo_optimizer_new.h>

namespace NYql {

IOptimizerFactory::TPtr MakeSimpleCBOOptimizerFactory();

} // namespace NYql
