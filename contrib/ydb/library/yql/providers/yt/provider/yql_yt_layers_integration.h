#pragma once

#include <contrib/ydb/library/yql/core/layers/layers_integration.h>

namespace NYql {
NLayers::ILayersIntegrationPtr CreateYtLayersIntegration();
}
