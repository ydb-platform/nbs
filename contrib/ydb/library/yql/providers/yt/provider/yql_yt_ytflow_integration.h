#pragma once

#include "yql_yt_provider.h"

#include <contrib/ydb/library/yql/providers/ytflow/integration/interface/yql_ytflow_integration.h>

#include <util/generic/ptr.h>


namespace NYql {

THolder<IYtflowIntegration> CreateYtYtflowIntegration(TYtState::TWeakPtr state);

} // namespace NYql
