#pragma once

#include <contrib/ydb/library/yql/ast/yql_expr.h>
#include <contrib/ydb/library/yql/core/yql_graph_transformer.h>

namespace NYql {

struct TTypeAnnotationContext;

IGraphTransformer::TStatus OptimizeBlocks(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx,
    TTypeAnnotationContext& typeCtx);

} // NYql
