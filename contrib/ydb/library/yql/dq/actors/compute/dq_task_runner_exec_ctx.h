#pragma once

#include <contrib/ydb/library/yql/dq/runtime/dq_tasks_runner.h>
#include <contrib/ydb/library/yql/dq/common/dq_common.h>
#include <contrib/ydb/library/actors/core/actor.h>

namespace NYql {
namespace NDq {

class TDqTaskRunnerExecutionContext : public TDqTaskRunnerExecutionContextBase {
public:
    TDqTaskRunnerExecutionContext(TTxId txId, IDqChannelStorage::TWakeUpCallback&& wakeUp);

    IDqChannelStorage::TPtr CreateChannelStorage(ui64 channelId, bool withSpilling) const override;
    IDqChannelStorage::TPtr CreateChannelStorage(ui64 channelId, bool withSpilling, NActors::TActorSystem* actorSystem, bool isConcurrent) const override;

private:
    const TTxId TxId_;
    const IDqChannelStorage::TWakeUpCallback WakeUp_;
};

} // namespace NDq
} // namespace NYql
