#pragma once

#include <contrib/ydb/library/yql/ast/yql_expr.h>
#include <contrib/ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

namespace NYql {

bool CheckBlockIOSupportedTypes(
    const TTypeAnnotationNode& containerType,
    const TSet<TString>& supportedTypes,
    const TSet<NUdf::EDataSlot>& supportedDataTypes,
    std::function<void(const TString&)> unsupportedTypeHandler,
    size_t wideFlowLimit,
    bool allowNestedOptionals = true
);

NNodes::TCoLambda WrapLambdaWithBlockInput(NNodes::TCoLambda lambda, TExprContext& ctx);

}
