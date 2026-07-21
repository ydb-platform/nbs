#pragma once

#include <contrib/ydb/library/yql/ast/yql_expr.h>
#include <contrib/ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <contrib/ydb/library/yql/core/yql_graph_transformer.h>

namespace NKikimr::NMetadata::NModifications {

class IOptimizationManager {
public:
    using TPtr = std::shared_ptr<IOptimizationManager>;

    virtual ~IOptimizationManager() = default;

    // Remove all features with world type annotation and merge it into single node
    virtual NYql::TExprNode::TPtr ExtractWorldFeatures(NYql::NNodes::TCoNameValueTupleList& features, NYql::TExprContext& ctx) const = 0;

    virtual NYql::IGraphTransformer::TStatus ValidateObjectNodeAnnotation(NYql::TExprNode::TPtr node, NYql::TExprContext& ctx) const = 0;
};

}  // namespace NKikimr::NMetadata::NModifications
