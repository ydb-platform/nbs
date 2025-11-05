#pragma once

#include <contrib/ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <contrib/ydb/library/yql/providers/generic/connector/api/common/data_source.pb.h>

namespace NYql {
    TString DumpGenericClusterConfig(const TGenericClusterConfig& clusterConfig);
}
