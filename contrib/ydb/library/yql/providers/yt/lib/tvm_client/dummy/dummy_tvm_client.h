#pragma once

#include <contrib/ydb/library/yql/providers/yt/lib/tvm_client/tvm_client.h>

namespace NYql {

ITvmClient::TPtr CreateDummyTvmClient();

} // namespace NYql
