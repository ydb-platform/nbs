#pragma once

#include <contrib/ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <contrib/ydb/library/yql/public/issue/yql_issue_id.h>

#include <util/generic/fwd.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

TString JoinPath(const TString& basePath, const TString& path);

} // namespace NFq
