#pragma once
#include <contrib/ydb/library/yql/core/issue/yql_issue.h>
#include <util/generic/string.h>

namespace NSQLTranslationV1 {

bool CheckLexers(NYql::TPosition pos, const TString& query, NYql::TIssues& issues);

} // namespace NSQLTranslationV1
