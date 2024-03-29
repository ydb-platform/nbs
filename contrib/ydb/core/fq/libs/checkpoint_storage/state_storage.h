#pragma once

#include <contrib/ydb/core/fq/libs/checkpointing_common/defs.h>

#include <contrib/ydb/library/yql/dq/proto/dq_checkpoint.pb.h>
#include <contrib/ydb/library/yql/public/issue/yql_issue.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/ptr.h>

namespace NFq {

////////////////////////////////////////////////////////////////////////////////

class IStateStorage : public virtual TThrRefBase {
public:
    using TGetStateResult = std::pair<std::vector<NYql::NDqProto::TComputeActorState>, NYql::TIssues>;
    using TCountStatesResult = std::pair<size_t, NYql::TIssues>;

    virtual NThreading::TFuture<NYql::TIssues> Init() = 0;

    virtual NThreading::TFuture<NYql::TIssues> SaveState(
        ui64 taskId,
        const TString& graphId,
        const TCheckpointId& checkpointId,
        const NYql::NDqProto::TComputeActorState& state) = 0;

    virtual NThreading::TFuture<TGetStateResult> GetState(
        const std::vector<ui64>& taskIds,
        const TString& graphId,
        const TCheckpointId& checkpointId) = 0;

    virtual NThreading::TFuture<TCountStatesResult> CountStates(
        const TString& graphId,
        const TCheckpointId& checkpointId) = 0;

    // GC interface

    virtual NThreading::TFuture<NYql::TIssues> DeleteGraph(
        const TString& graphId) = 0;

    virtual NThreading::TFuture<NYql::TIssues> DeleteCheckpoints(
        const TString& graphId,
        const TCheckpointId& checkpointId) = 0;
};

using TStateStoragePtr = TIntrusivePtr<IStateStorage>;

} // namespace NFq
