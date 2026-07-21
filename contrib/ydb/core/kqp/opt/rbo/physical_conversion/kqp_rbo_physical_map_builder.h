#pragma once
#include "kqp_rbo_physical_op_builder.h"
#include "kqp_rbo_physical_convertion_utils.h"
#include <contrib/ydb/library/yql/core/yql_opt_utils.h>
#include <contrib/ydb/library/yql/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalMapBuilder: public TPhysicalUnaryOpBuilder {
public:
    TPhysicalMapBuilder(TIntrusivePtr<TOpMap> map, TExprContext& ctx, TPositionHandle pos)
        : TPhysicalUnaryOpBuilder(ctx, pos)
        , Map(map) {
    }

    TExprNode::TPtr BuildPhysicalOp(TExprNode::TPtr input) override;

private:
    TIntrusivePtr<TOpMap> Map;
};
