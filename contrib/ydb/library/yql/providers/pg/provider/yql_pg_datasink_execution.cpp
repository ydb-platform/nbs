#include "yql_pg_provider_impl.h"

#include <contrib/ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <contrib/ydb/library/yql/providers/pg/expr_nodes/yql_pg_expr_nodes.h>

#include <contrib/ydb/library/yql/providers/common/provider/yql_provider.h>
#include <contrib/ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <contrib/ydb/library/yql/providers/common/provider/yql_data_provider_impl.h>

#include <contrib/ydb/library/yql/utils/log/log.h>

#include <utility>

namespace NYql {

using namespace NNodes;

class TPgDataSinkExecTransformer: public TExecTransformerBase {
public:
    explicit TPgDataSinkExecTransformer(TPgState::TPtr state)
        : State_(std::move(state))
    {
        AddHandler({TCoCommit::CallableName()}, RequireFirst(), Pass());
    }

private:
    TPgState::TPtr State_;
};

THolder<TExecTransformerBase> CreatePgDataSinkExecTransformer(TPgState::TPtr state) {
    return THolder(new TPgDataSinkExecTransformer(state));
}

} // namespace NYql
