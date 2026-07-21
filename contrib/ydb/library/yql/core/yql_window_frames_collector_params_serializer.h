#pragma once

#include <contrib/ydb/library/yql/core/yql_window_frame_settings.h>
#include <contrib/ydb/library/yql/core/sql_types/window_frames_collector_params.h>
#include <contrib/ydb/library/yql/ast/yql_expr.h>

namespace NYql::NWindow {

TExprNode::TPtr SerializeWindowAggregatorParamsToExpr(
    const TExprNodeCoreWinFrameCollectorParams& params,
    TPositionHandle pos,
    TExprContext& ctx);

} // namespace NYql::NWindow
