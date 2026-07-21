#pragma once

#include <contrib/ydb/library/yql/ast/yql_expr.h>
#include <contrib/ydb/library/yql/ast/yql_pos_handle.h>
#include <contrib/ydb/library/yql/core/expr_nodes_gen/yql_expr_nodes_gen.h>
#include <contrib/ydb/library/yql/core/yql_graph_transformer.h>

namespace NYql::NPushdown {

IGraphTransformer::TStatus AnnotateFilterPredicate(
    const TExprNode::TPtr& input,
    size_t childIndex,
    const TStructExprType* itemType,
    TExprContext& ctx);


} // namespace NYql::NPushdown
