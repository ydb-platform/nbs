#pragma once

#include <contrib/ydb/library/yql/providers/common/mkql/yql_provider_mkql.h>

namespace NYql {

void RegisterDqSolomonMkqlCompilers(NCommon::TMkqlCallableCompilerBase& compiler);

}
