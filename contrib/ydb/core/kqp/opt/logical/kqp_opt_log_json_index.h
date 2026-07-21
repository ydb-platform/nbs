#pragma once

#include <contrib/ydb/core/kqp/common/kqp_yql.h>

#include <util/generic/hash.h>
#include <contrib/ydb/library/yql/ast/yql_expr.h>
#include <contrib/ydb/library/yql/public/issue/yql_issue.h>

#include <expected>

namespace NKikimr::NKqp::NOpt {

struct TJsonIndexSettings {
    TString ColumnName;
    NYql::TKqpReadTableFullTextIndexSettings Settings;
};

std::expected<TJsonIndexSettings, NYql::TIssue> CollectJsonIndexPredicate(const NYql::NNodes::TExprBase& body,
    const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx, const THashSet<TString>& jsonIndexedColumns);

}   // namespace NKikimr::NKqp::NOpt
