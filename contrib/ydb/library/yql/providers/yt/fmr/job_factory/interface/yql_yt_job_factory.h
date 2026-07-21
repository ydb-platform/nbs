#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <contrib/ydb/library/yql/providers/yt/fmr/request_options/yql_yt_request_options.h>
#include <contrib/ydb/library/yql/utils/runnable.h>

namespace NYql::NFmr {

class IFmrJobFactory: public IRunnable {
public:
    using TPtr = TIntrusivePtr<IFmrJobFactory>;

    virtual ~IFmrJobFactory() = default;

    virtual NThreading::TFuture<TTaskState::TPtr> StartJob(TTask::TPtr task, std::shared_ptr<std::atomic<bool>> cancelFlag) = 0;

    virtual ui64 GetMaxParallelJobCount() const = 0;
};

} // namespace NYql::NFmr
