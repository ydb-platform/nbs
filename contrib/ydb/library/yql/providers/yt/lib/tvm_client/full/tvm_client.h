#pragma once

#include <contrib/ydb/library/yql/providers/yt/lib/tvm_client/tvm_client.h>
#include <contrib/ydb/library/yql/providers/yt/lib/tvm_client/proto/tvm_client.pb.h>

namespace NYql {

ITvmClient::TPtr CreateTvmClient(const TYtTvmConfig& config);

}; // namespace NYql
