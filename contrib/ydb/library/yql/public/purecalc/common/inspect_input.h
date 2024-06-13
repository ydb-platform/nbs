#pragma once

#include <contrib/ydb/library/yql/ast/yql_expr.h>

namespace NYql::NPureCalc {
    bool TryFetchInputIndexFromSelf(const TExprNode&, TExprContext&, ui32, ui32&);
}
