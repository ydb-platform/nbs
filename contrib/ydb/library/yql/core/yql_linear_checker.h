#pragma once

#include <contrib/ydb/library/yql/ast/yql_expr.h>

namespace NYql {

bool ValidateLinearTypes(const TExprNode& root, TExprContext& ctx);

}
