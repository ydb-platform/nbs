#pragma once
#include <contrib/ydb/library/yql/public/issue/yql_issue.h>
#include <contrib/ydb/library/yql/public/langver/yql_langver.h>

namespace NYql {

bool CheckLangVersion(TLangVersion ver, TLangVersion max, TMaybe<TIssue>& issue);

} // namespace NYql
