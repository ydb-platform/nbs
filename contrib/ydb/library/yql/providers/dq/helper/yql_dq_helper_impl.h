#pragma once

#include <util/generic/ptr.h>
#include <util/generic/map.h>

#include <contrib/ydb/library/yql/core/dq_integration/yql_dq_helper.h>

namespace NYql {

IDqHelper::TPtr MakeDqHelper();

} // namespace NYql
