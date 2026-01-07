#include "trace_processor.h"

#include "trace_reader.h"

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/lwtrace/control.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTraceProcessor
    : public ITraceProcessor
    , public std::enable_shared_from_this<TTraceProcessor>
{
private:
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ILoggingServicePtr Logging;
    const TTraceProcessorConfig Config;
    NLWTrace::TManager& LWManager;
    TVector<ITraceReaderPtr> Readers;

    TLog Log;
    TAtomic ShouldStop = 0;

public:
    TTraceProcessor(
        ITimerPtr timer,
        ISchedulerPtr scheduler,
        ILoggingServicePtr logging,
        TTraceProcessorConfig config,
        NLWTrace::TManager& lwManager,
        TVector<ITraceReaderPtr> readers)
        : Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Logging(std::move(logging))
        , Config(std::move(config))
        , LWManager(lwManager)
        , Readers(std::move(readers))
    {}

    void Start() override
    {
        Log = Logging->CreateLog(Config.ComponentName);
        ScheduleProcessLWDepot();
    }

    void Stop() override
    {
        AtomicSet(ShouldStop, 1);
    }

    void ForEach(std::function<void(ITraceReaderPtr)> fn) override
    {
        for (ITraceReaderPtr& keeper: Readers) {
            fn(keeper);
        }
    }

private:
    void ScheduleProcessLWDepot()
    {
        if (AtomicGet(ShouldStop)) {
            return;
        }

        auto weak_ptr = weak_from_this();

        Scheduler->Schedule(
            Timer->Now() + Config.DumpTracksInterval,
            [weak_ptr = std::move(weak_ptr)]
            {
                if (auto p = weak_ptr.lock()) {
                    p->ProcessLWDepot();
                    p->ScheduleProcessLWDepot();
                }
            });
    }

    void ProcessLWDepot()
    {
        for (auto& reader: Readers) {
            try {
                reader->Reset();
                LWManager.ExtractItemsFromCyclicDepot(reader->Id, *reader);
            } catch (...) {
                STORAGE_ERROR("Tracing error: " << CurrentExceptionMessage());
                Y_DEBUG_ABORT_UNLESS(0);
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTraceProcessorStub final
    : public ITraceProcessor
{
    void Start() override
    {}

    void Stop() override
    {}

    void ForEach(std::function<void(ITraceReaderPtr)> fn) override
    {
        Y_UNUSED(fn);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITraceProcessorPtr CreateTraceProcessor(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    TTraceProcessorConfig config,
    NLWTrace::TManager& lwManager,
    TVector<ITraceReaderPtr> readers)
{
    return std::make_shared<TTraceProcessor>(
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(config),
        lwManager,
        std::move(readers));
}

ITraceProcessorPtr CreateTraceProcessorStub()
{
    return std::make_shared<TTraceProcessorStub>();
}

}   // namespace NCloud
