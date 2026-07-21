#pragma once

#include "yql_clickhouse_provider.h"

#include <contrib/ydb/library/yql/core/dq_integration/yql_dq_integration.h>

#include <util/generic/ptr.h>

namespace NYql {

THolder<IDqIntegration> CreateClickHouseDqIntegration(TClickHouseState::TPtr state);

}
