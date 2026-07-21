#pragma once

#include <contrib/ydb/library/yql/core/yql_graph_transformer.h>
#include <contrib/ydb/library/yql/core/yql_type_annotation.h>

#include <util/generic/ptr.h>

namespace NYql {

THolder<IGraphTransformer> CreateNormalizeDependsOnTransformer(const TTypeAnnotationContext& types);

}
