#include "dq_compute.h"

#include <contrib/ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <contrib/ydb/library/yql/minikql/mkql_node.h>
#include <contrib/ydb/library/yql/parser/pg_wrapper/interface/pack.h>
#include <contrib/ydb/library/yql/parser/pg_wrapper/interface/comp_factory.h>
#include "contrib/ydb/library/yql/utils/yql_panic.h"

namespace NYql::NDq {

using namespace NKikimr;
using namespace NMiniKQL;

TComputationNodeFactory GetDqBaseComputeFactory(const TDqComputeContextBase* computeCtx) {
    YQL_ENSURE(computeCtx);
    auto builtinFactory = GetCompositeWithBuiltinFactory({
        NYql::GetPgFactory()
    });

    return [builtinFactory]
        (TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
            if (auto builtin = builtinFactory(callable, ctx)) {
                return builtin;
            }

            return nullptr;
        };
}

}
