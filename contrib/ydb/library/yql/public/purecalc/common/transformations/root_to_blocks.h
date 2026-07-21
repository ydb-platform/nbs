#pragma once

#include <contrib/ydb/library/yql/public/purecalc/common/processor_mode.h>

#include <contrib/ydb/library/yql/core/yql_graph_transformer.h>

namespace NYql::NPureCalc {
/**
 * A transformer which rewrite the root to respect block types.
 *
 * @param acceptsBlock allows using this transformer in pipeline and
 *        skip this phase if no block output is required.
 * @param processorMode specifies the top-most container of the result.
 * @return a graph transformer for rewriting the root node.
 */
TAutoPtr<IGraphTransformer> MakeRootToBlocks(
    bool acceptsBlocks,
    EProcessorMode processorMode);
} // namespace NYql::NPureCalc
