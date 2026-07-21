#pragma once

#include <contrib/ydb/library/yql/providers/common/mkql/yql_type_mkql.h>
#include <contrib/ydb/library/yql/public/purecalc/common/interface.h>
#include <contrib/ydb/library/yql/minikql/mkql_node.h>
#include <contrib/ydb/library/yql/ast/yql_expr.h>
#include <contrib/ydb/library/yql/core/yql_user_data.h>

namespace NYql::NPureCalc {

/**
 * Compile expr to mkql byte-code
 */

NKikimr::NMiniKQL::TRuntimeNode CompileMkql(const TExprNode::TPtr& exprRoot, TExprContext& exprCtx,
                                            const NKikimr::NMiniKQL::IFunctionRegistry& funcRegistry, const NKikimr::NMiniKQL::TTypeEnvironment& env, const TUserDataTable& userData,
                                            NCommon::TMemoizedTypesMap* typeMemoization = nullptr);

} // namespace NYql::NPureCalc
