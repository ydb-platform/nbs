#pragma once
#include <contrib/ydb/library/yql/ast/yql_expr.h>

namespace NYql {
    TVector<TVector<TString>> ExtractLayersFromExpr(const TExprNode::TPtr& node);
}
