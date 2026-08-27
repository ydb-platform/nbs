#include "test.h"

#include "client.h"
#include "context.h"
#include "request.h"
#include "shm_client.h"

#include <cloud/filestore/libs/client/config.h>
#include <cloud/filestore/libs/client/session.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/format.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/histogram.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/guid.h>
#include <util/generic/map.h>
#include <util/generic/overloaded.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/string/printf.h>
#include <util/system/condvar.h>
#include <util/system/event.h>
#include <util/system/mutex.h>
#include <util/system/thread.h>

#include <google/protobuf/util/json_util.h>

#include <variant>

namespace NCloud::NFileStore::NLoadTest {

using namespace NThreading;
using namespace NCloud::NFileStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestStats
{
    struct TStats
    {
        ui64 Requests = 0;
        ui64 RequestBytes = 0;
        // Resets after each progress report
        ui64 IntervalRequests = 0;
        // Whole-run distribution, reported in the final results
        TLatencyHistogram Hist;
        // Reset after each progress report, reported in progress reports
        TLatencyHistogram IntervalHist;
    };

    TString Name;
    bool Success = true;
    bool Stopping = false;
    TMap<NProto::EAction, TStats> ActionStats;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
T WaitForCompletion(
    const TString& request,
    const TFuture<T>& future)
{
    const auto& response = future.GetValue(TDuration::Max());
    if (HasError(response)) {
        const auto& error = response.GetError();
        throw yexception()
            << "Failed to execute " << request << " request: "
            << FormatError(error);
    }

    return response;
}

////////////////////////////////////////////////////////////////////////////////

class TRequestsCompletionQueue
{
private:
    TAdaptiveLock Lock;
    TDeque<std::unique_ptr<TCompletedRequest>> Items;

public:
    void Enqueue(std::unique_ptr<TCompletedRequest> request)
    {
        with_lock (Lock) {
            Items.emplace_back(std::move(request));
        }
    }

    std::unique_ptr<TCompletedRequest> Dequeue()
    {
        with_lock (Lock) {
            std::unique_ptr<TCompletedRequest> ptr;
            if (Items) {
                ptr = std::move(Items.front());
                Items.pop_front();
            }

            return ptr;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TSetupSession
{
    bool ReadOnly = false;
    ui64 SessionSeqNo = 0;
    TPromise<bool> Complete;
};

struct TTeardown
{
    TPromise<bool> Complete;
};
struct TSuspendLoad {};
struct TResumeLoad {};
struct TProcessCompletedRequests {};

using TLoadTestCommand = std::variant<
    TTeardown,
    TSuspendLoad,
    TResumeLoad,
    TProcessCompletedRequests,
    TSetupSession>;

////////////////////////////////////////////////////////////////////////////////

class TLoadTestCommandChannel
{
private:
    TAdaptiveLock Lock;
    TDeque<TLoadTestCommand> Items;
    bool HasProcessCompletedRequests = false;

public:
    void Enqueue(TLoadTestCommand command)
    {
        with_lock (Lock) {
            if (holds_alternative<TProcessCompletedRequests>(command)) {
                if (HasProcessCompletedRequests) {
                    return;
                }
                HasProcessCompletedRequests = true;
            }
            Items.emplace_back(std::move(command));
        }
    }

    TMaybe<TLoadTestCommand> Dequeue()
    {
        with_lock (Lock) {
            if (Items) {
                auto value = std::move(Items.front());
                Items.pop_front();
                if (holds_alternative<TProcessCompletedRequests>(value)) {
                    HasProcessCompletedRequests = false;
                }

                return value;
            }

            return {};
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TStartSource
{
};

struct TStartTarget
{
};

struct TFinishMigration
{
};

struct TTestFinished
{
    TString Id;
};

using TLoadTestControllerCommand = std::variant<
    TStartSource,
    TStartTarget,
    TFinishMigration,
    TTestFinished>;

////////////////////////////////////////////////////////////////////////////////

class TLoadTestControllerCommandChannel
{
private:
    TAdaptiveLock Lock;
    TDeque<TLoadTestControllerCommand> Items;
    TAutoEvent Event;

public:
    void Enqueue(TLoadTestControllerCommand command)
    {
        with_lock (Lock) {
            Items.emplace_back(std::move(command));
        }
        Event.Signal();
    }

    TMaybe<TLoadTestControllerCommand> Dequeue()
    {
        with_lock (Lock) {
            if (Items) {
                auto value = std::move(Items.front());
                Items.pop_front();
                return value;
            }

            return {};
        }
    }

    void Wait()
    {
        Event.WaitI();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLoadTest final
    : public ITest
    , public ISimpleThread
    , public std::enable_shared_from_this<TLoadTest>
{
private:
    static constexpr TDuration ReportInterval = TDuration::Seconds(5);

    const NProto::TLoadTest Config;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ILoggingServicePtr Logging;
    const TString TestInstanceId;
    const TString TestTag;

    TLoadTestControllerCommandChannel& Controller;
    const IClientFactoryPtr ClientFactory;

    TLog Log;
    IFileStoreServicePtr Client;
    IShmDataClientPtr ShmClient;
    ISessionPtr Session;

    TString FileSystemId;
    TString ClientId;
    TString SessionId;

    ui32 MaxIoDepth = 64;
    ui32 CurrentIoDepth = 0;

    TDuration MaxDuration;
    TInstant StartTs;

    ui64 MaxRequests = 0;
    ui64 RequestsSent = 0;
    ui64 RequestsCompleted = 0;

    TInstant LastReportTs;
    ui64 LastRequestsCompleted = 0;

    ui64 RequestBytes = 0;
    ui64 LastRequestBytes = 0;

    IRequestGeneratorPtr RequestGenerator;
    TRequestsCompletionQueue CompletionQueue;
    TLoadTestCommandChannel Commands;
    TAutoEvent Event;

    bool LoadEnabled = true;

    ui64 SeqNo = 0;

    TTestStats TestStats;
    TPromise<NProto::TTestStats> Result = NewPromise<NProto::TTestStats>();

    bool ShutdownFlag = false;

public:
    TLoadTest(
            NProto::TLoadTest config,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            ILoggingServicePtr logging,
            IClientFactoryPtr clientFactory,
            TString testInstanceId,
            TLoadTestControllerCommandChannel& controller)
        : Config(std::move(config))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Logging(std::move(logging))
        , TestInstanceId(std::move(testInstanceId))
        , TestTag(NLoadTest::MakeTestTag(Config.GetName()))
        , Controller(controller)
        , ClientFactory(clientFactory)
        , Client(clientFactory->CreateClient())
        , ClientId(Config.GetClientId() ? Config.GetClientId() : "test-client")
    {
        Log = Logging->CreateLog("RUNNER");
    }

    TFuture<NProto::TTestStats> Run() override
    {
        ISimpleThread::Start();
        return Result;
    }

    void GenerateRequests()
    {
        if (LoadEnabled && !ShouldStop()) {
            while (SendNextRequest()) {
                ++RequestsSent;
                STORAGE_DEBUG("%s request sent: %s %lu",
                    MakeTestTag().c_str(), SessionId.c_str(), SeqNo);
            }
        }
    }

    void EnqueueCommand(TLoadTestCommand command)
    {
        Commands.Enqueue(std::move(command));
        Event.Signal();
    }

    void SetupSession(TSetupSession& cmd)
    {
        if (!Session) {
            cmd.Complete.SetValue(true);
            return;
        }
        if (!ShouldStop()) {
            STORAGE_INFO("%s establishing session %lu",
                MakeTestTag().c_str(), cmd.SessionSeqNo);

            TFuture<NProto::TCreateSessionResponse> result;
            if (!SessionId) {
                result = Session->CreateSession(cmd.ReadOnly, cmd.SessionSeqNo);
            } else {
                result = Session->AlterSession(cmd.ReadOnly, cmd.SessionSeqNo);
            }

            WaitForCompletion("create-session", result);
            auto response = result.GetValue();
            if (FAILED(response.GetError().GetCode())) {
                STORAGE_INFO(
                    "%s failed establish session: %s %lu %s",
                    MakeTestTag().c_str(),
                    SessionId.c_str(),
                    cmd.SessionSeqNo,
                    response.GetError().GetMessage().c_str());
                TestStats.Success = false;
                cmd.Complete.SetValue(false);
                return;
            }
            auto sessionId = response.GetSession().GetSessionId();
            if (SessionId && SessionId != sessionId) {
                STORAGE_INFO(
                    "%s session established but id has changed: %s %lu %s",
                    MakeTestTag().c_str(),
                    SessionId.c_str(),
                    cmd.SessionSeqNo,
                    sessionId.c_str());
                TestStats.Success = false;
                cmd.Complete.SetValue(false);
                return;
            }
            SessionId = sessionId;
            STORAGE_INFO("%s session established: %s %lu",
                MakeTestTag().c_str(), SessionId.c_str(), cmd.SessionSeqNo);
        }

        cmd.Complete.SetValue(true);
    }

    bool HandleTeardown(TTeardown& cmd)
    {
        STORAGE_INFO("%s got teardown", MakeTestTag().c_str())
        TeardownTest(cmd);
        return false;
    }

    bool HandleSuspendLoad(const TSuspendLoad&)
    {
        LoadEnabled = false;
        return true;
    }

    bool HandleResumeLoad(const TResumeLoad&) {
        LoadEnabled = true;
        GenerateRequests();
        return true;
    }

    bool HandleProcessCompletedRequests(const TProcessCompletedRequests&)
    {
        ProcessCompletedRequests();
        ReportProgress();
        GenerateRequests();
        return true;
    }

    bool HandleSetupSession(TSetupSession& cmd) {
        SeqNo = cmd.SessionSeqNo;
        SetupSession(cmd);
        GenerateRequests();
        return true;
    }

    bool HandleCommand(TLoadTestCommand cmd)
    {
        return std::visit(
            TOverloaded {
                [this] (TTeardown& cmd) {
                    return HandleTeardown(cmd);
                },
                [this] (const TSuspendLoad& cmd) {
                    return HandleSuspendLoad(cmd);
                },
                [this] (const TResumeLoad& cmd) {
                    return HandleResumeLoad(cmd);
                },
                [this] (const TProcessCompletedRequests& cmd) {
                    return HandleProcessCompletedRequests(cmd);
                },
                [this] (TSetupSession& cmd) {
                    return HandleSetupSession(cmd);
                }
            },
            cmd);
    }

    void* ThreadProc() override
    {
        StartTs = TInstant::Now();
        NCloud::SetCurrentThreadName(MakeTestTag());

        try {
            SetupTest();
            LastReportTs = TInstant::Now();

            for(;;) {
                Event.WaitI();
                bool cont = true;
                while (auto maybeCmd = Commands.Dequeue()) {
                    auto& cmd = *maybeCmd;
                    if (cont = HandleCommand(cmd); !cont) {
                        break;
                    }
                }
                if (ShouldStop() && !ShutdownFlag) {
                    TTeardown dummy;
                    TeardownTest(dummy);

                    TTestFinished cmd;
                    cmd.Id = TestInstanceId;
                    Controller.Enqueue(cmd);
                    ShutdownFlag = true;
                }
                if (!cont) {
                    break;
                }
            }

            // prevent race between this thread and main thread
            // destroying test instance right after setvalue()
            auto result = Result;
            result.SetValue(GetStats(false /* sinceLastReport */));
        } catch(...) {
            STORAGE_ERROR("%s test has failed: %s",
                MakeTestTag().c_str(), CurrentExceptionMessage().c_str());

            Result.SetException(std::current_exception());
        }

        return nullptr;
    }

    TString GetTestId() const
    {
        return TestInstanceId;
    }

private:
    void SetupTest()
    {
        Client->Start();

        // setup test limits
        if (auto depth = Config.GetIODepth()) {
            MaxIoDepth = depth;
        }

        MaxRequests = Config.GetRequestsCount();
        MaxDuration = TDuration::Seconds(Config.GetTestDuration());

        STORAGE_INFO("%s setting up test", MakeTestTag().c_str());

        // setup test filestore
        if (Config.HasFileSystemId()) {
            FileSystemId = Config.GetFileSystemId();
        } else if (Config.HasCreateFileStoreRequest()) {
            FileSystemId = Config.GetCreateFileStoreRequest().GetFileSystemId();
        }

        if (!FileSystemId.empty()) {
            CreateSession();
        }

        NProto::THeaders headers;
        headers.SetClientId(Config.GetClientId());
        headers.SetSessionId(SessionId);
        switch (Config.GetSpecsCase()) {
            case NProto::TLoadTest::kIndexLoadSpec:
                RequestGenerator = CreateIndexRequestGenerator(
                    Config.GetIndexLoadSpec(),
                    Logging,
                    Client,
                    Session,
                    FileSystemId,
                    headers,
                    std::make_shared<TCountLimiter>(Config.GetMaxNodes()));
                break;
            case NProto::TLoadTest::kDataLoadSpec:
                RequestGenerator = CreateDataRequestGenerator(
                    Config.GetDataLoadSpec(),
                    Logging,
                    Session,
                    FileSystemId,
                    headers,
                    std::make_shared<TCountLimiter>(Config.GetMaxNodes()));
                break;
            case NProto::TLoadTest::kReplayFsSpec:
                RequestGenerator = CreateReplayRequestGeneratorFs(
                    Config.GetReplayFsSpec(),
                    Logging,
                    Session,
                    FileSystemId,
                    headers);
                break;
            case NProto::TLoadTest::kReplayGrpcSpec:
                RequestGenerator = CreateReplayRequestGeneratorGRPC(
                    Config.GetReplayGrpcSpec(),
                    Logging,
                    Session,
                    FileSystemId,
                    headers);
                break;
            case NProto::TLoadTest::kDatashardLikeLoadSpec: {
                const auto& spec = Config.GetDatashardLikeLoadSpec();
                if (!spec.GetSharedMemoryBaseDir().empty() &&
                    !spec.GetSharedMemoryFilePath().empty())
                {
                    ShmClient = CreateSharedMemoryClient(
                        spec.GetSharedMemoryBaseDir(),
                        spec.GetSharedMemoryFilePath(),
                        spec.GetSharedMemorySizeBytes(),
                        Max(static_cast<ui64>(spec.GetReadBytes()),
                            static_cast<ui64>(spec.GetWriteBytes())),
                        spec.GetSharedMemoryPageSize(),
                        ClientFactory->CreateShmControl(),
                        Scheduler,
                        Timer,
                        Logging,
                        spec.GetAllowOverlappingSharedMemoryPages());
                    ShmClient->Start();
                }
                RequestGenerator = CreateDatashardLikeRequestGenerator(
                    spec,
                    Logging,
                    Session,
                    ShmClient,
                    FileSystemId,
                    headers,
                    std::make_shared<TCountLimiter>(Config.GetMaxNodes()));
                break;
            }
            case NProto::TLoadTest::kFastShardLoadSpec:
                RequestGenerator = CreateFastShardRequestGenerator(
                    Config.GetFastShardLoadSpec(),
                    Config.GetIODepth(),
                    Logging,
                    std::make_shared<TCountLimiter>(Config.GetMaxNodes()));
                break;
            default:
                ythrow yexception()
                    << MakeTestTag()
                    << " config should have test spec";
        }
    }

    void CreateSession()
    {
        NProto::TSessionConfig proto;
        proto.SetFileSystemId(FileSystemId);
        proto.SetClientId(ClientId);
        proto.SetSessionPingTimeout(Config.GetSessionPingTimeout());

        Session = NClient::CreateSession(
            Logging,
            Timer,
            Scheduler,
            Client,
            std::make_shared<TSessionConfig>(proto));
    }

    bool ShouldStop() const
    {
        return TestStats.Stopping || !TestStats.Success || LimitsReached();
    }

    bool LimitsReached() const
    {
        return (MaxDuration && TInstant::Now() - StartTs >= MaxDuration) ||
            (MaxRequests && RequestsSent >= MaxRequests);
    }

    bool SendNextRequest()
    {
        if (LimitsReached() || (MaxIoDepth && CurrentIoDepth >= MaxIoDepth) ||
            !RequestGenerator->HasNextRequest() || ShouldStop())
        {
            return false;
        }

        auto self = weak_from_this();
        const auto future = RequestGenerator->ExecuteNextRequest();
        if (!future.Initialized()) {
            TestStats.Stopping = true;
            return false;
        }
        ++CurrentIoDepth;
        future.Apply(
            [=](const TFuture<TCompletedRequest>& future)
            {
                if (auto ptr = self.lock()) {
                    if (future.HasException()) {
                        ptr->SignalCompletion(TCompletedRequest{});
                    } else {
                        ptr->SignalCompletion(future.GetValue());
                    }
                }
            });

        if (RequestGenerator->ShouldImmediatelyProcessQueue()) {
            if (future.HasValue() || future.HasException()) {
                ProcessCompletedRequests();
            }
        }
        return true;
    }

    void SignalCompletion(TCompletedRequest request)
    {
        CompletionQueue.Enqueue(
            std::make_unique<TCompletedRequest>(std::move(request)));

        Commands.Enqueue(TProcessCompletedRequests{});
        Event.Signal();
    }

    void ProcessCompletedRequests()
    {
        while (auto request = CompletionQueue.Dequeue()) {
            Y_ABORT_UNLESS(CurrentIoDepth > 0);
            --CurrentIoDepth;
            ++RequestsCompleted;

            auto code = request->Error.GetCode();
            if (FAILED(code)) {
                if (RequestGenerator->ShouldFailOnError(request->Error)) {
                    STORAGE_ERROR(
                        "%s failing test %s due to: %s",
                        MakeTestTag().c_str(),
                        NProto::EAction_Name(request->Action).c_str(),
                        FormatError(request->Error).c_str());

                    TestStats.Success = false;
                }
            }

            auto& stats = TestStats.ActionStats[request->Action];
            ++stats.Requests;
            ++stats.IntervalRequests;
            stats.RequestBytes += request->RequestBytes;
            RequestBytes += request->RequestBytes;
            stats.Hist.RecordValue(request->Elapsed);
            stats.IntervalHist.RecordValue(request->Elapsed);
        }
    }

    void ReportProgress()
    {
        auto now = TInstant::Now();
        auto elapsed = now - LastReportTs;

        if (elapsed > ReportInterval) {
            const auto requestsCompleted = RequestsCompleted - LastRequestsCompleted;
            const auto requestBytes = RequestBytes - LastRequestBytes;

            auto stats = GetStats(true /* sinceLastReport */);
            STORAGE_INFO(
                "%s current rate: %ld r/s, bandwidth: %s/s; stats:\n%s",
                MakeTestTag().c_str(),
                (ui64)(requestsCompleted / elapsed.SecondsFloat()),
                FormatByteSize((ui64)(requestBytes / elapsed.SecondsFloat()))
                    .c_str(),
                NProtobufJson::Proto2Json(stats, {.FormatOutput = true})
                    .c_str());

            LastReportTs = now;
            LastRequestsCompleted = RequestsCompleted;
            LastRequestBytes = RequestBytes;
        }
    }

    // With sinceLastReport latencies cover only the requests completed after
    // the previous progress report; otherwise they cover the whole run
    NProto::TTestStats GetStats(bool sinceLastReport)
    {
        NProto::TTestStats results;
        results.SetName(Config.GetName());
        results.SetSuccess(TestStats.Success);

        auto* stats = results.MutableStats();
        for (auto& pair: TestStats.ActionStats) {
            if (sinceLastReport && pair.second.IntervalRequests == 0) {
                // Skip reporting latencies for actions that have not been
                // executed since the last report
                continue;
            }

            auto* action = stats->Add();
            action->SetAction(NProto::EAction_Name(pair.first));
            action->SetCount(pair.second.Requests);
            action->SetRequestBytes(pair.second.RequestBytes);
            if (sinceLastReport) {
                FillLatency(pair.second.IntervalHist, *action->MutableLatency());
                pair.second.IntervalHist.Reset();
                pair.second.IntervalRequests = 0;
            } else {
                FillLatency(pair.second.Hist, *action->MutableLatency());
            }
        }

        // TODO report some stats for the files generated during the shooting

        return results;
    }

    void TeardownTest(TTeardown& cmd)
    {
        if (ShutdownFlag) {
            if (cmd.Complete.Initialized()) {
                cmd.Complete.SetValue(true);
            }
            return;
        }

        STORAGE_INFO("%s tear down", MakeTestTag().c_str());
        while (CurrentIoDepth > 0) {
            Sleep(TDuration::Seconds(1));
            ProcessCompletedRequests();
        }

        if (Session) {
            auto result = Session->DestroySession();
            WaitForCompletion("destroy session", result);

            const auto& response = result.GetValue();
            if (FAILED(response.GetError().GetCode())) {
                STORAGE_INFO(
                    "%s failed to destroy session: %s %lu %s",
                    MakeTestTag().c_str(),
                    SessionId.c_str(),
                    SeqNo,
                    response.GetError().GetMessage().c_str());
                TestStats.Success = false;
            }

            if (cmd.Complete.Initialized()) {
                cmd.Complete.SetValue(SUCCEEDED(response.GetError().GetCode()));
            }
        } else {
            if (cmd.Complete.Initialized()) {
                cmd.Complete.SetValue(true);
            }
        }

        if (ShmClient) {
            ShmClient->Stop();
        }

        if (Client) {
            Client->Stop();
        }
    }

    void FillLatency(
        const TLatencyHistogram& hist,
        NProto::TLatency& latency)
    {
        latency.SetP50(hist.GetValueAtPercentile(50));
        latency.SetP90(hist.GetValueAtPercentile(90));
        latency.SetP95(hist.GetValueAtPercentile(95));
        latency.SetP99(hist.GetValueAtPercentile(99));
        latency.SetP999(hist.GetValueAtPercentile(99.9));
        latency.SetMin(hist.GetMin());
        latency.SetMax(hist.GetMax());
        latency.SetMean(hist.GetMean());
        latency.SetStdDeviation(hist.GetStdDeviation());
    }

    const TString& MakeTestTag() const
    {
        return TestTag;
    }
};

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<TLoadTest> CreateSimpleLoadTest(
    const NProto::TLoadTest& config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IClientFactoryPtr clientFactory,
    TString testInstanceId,
    TLoadTestControllerCommandChannel& controller)
{
    return std::make_shared<TLoadTest>(
        config,
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(clientFactory),
        std::move(testInstanceId),
        controller);
}

////////////////////////////////////////////////////////////////////////////////

class TLoadTestController final
    : public ITest
    , public ISimpleThread
    , public std::enable_shared_from_this<TLoadTestController>
{
private:
    const TAppContext& Ctx;
    const NProto::TLoadTest Config;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const ILoggingServicePtr Logging;
    const IClientFactoryPtr ClientFactory;
    const TString TestTag;

    TLog Log;
    std::shared_ptr<TLoadTest> SourceTest;
    std::shared_ptr<TLoadTest> TargetTest;
    TFuture<NProto::TTestStats> SourceFuture;
    TFuture<NProto::TTestStats> TargetFuture;

    TDuration MaxDuration;

    TInstant StartTs;

    TLoadTestControllerCommandChannel Events;

    TTestStats TestStats;
    TPromise<NProto::TTestStats> Result = NewPromise<NProto::TTestStats>();

    ui64 SessionSeqNo = 0;

    IFileStoreServicePtr Client;

    TProtoMessagePrinter ProtoMessagePrinter;

public:
    TLoadTestController(
            const TAppContext& ctx,
            const NProto::TLoadTest& config,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            ILoggingServicePtr logging,
            IClientFactoryPtr clientFactory)
        : Ctx(ctx)
        , Config(config)
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Logging(std::move(logging))
        , ClientFactory(std::move(clientFactory))
        , TestTag(NLoadTest::MakeTestTag("Controller_" + Config.GetName()))
        , Client(ClientFactory->CreateClient())
    {
        Log = Logging->CreateLog("TEST_CONTROLLER");
    }

    TFuture<NProto::TTestStats> Run() override
    {
        Events.Enqueue(TStartSource{});

        ISimpleThread::Start();

        return Result;
    }

    TFuture<bool> SendSetupTestSession(
        std::shared_ptr<TLoadTest>& test,
        bool readOnly,
        ui64 sessionSeqNo)
    {
        TPromise<bool> complete = NewPromise<bool>();

        TSetupSession cmd;
        cmd.ReadOnly = readOnly;
        cmd.SessionSeqNo = sessionSeqNo;
        cmd.Complete = complete;
        test->EnqueueCommand(std::move(cmd));

        return complete.GetFuture();
    }

    TFuture<bool> SendTeardownTest(std::shared_ptr<TLoadTest>& test)
    {
        TPromise<bool> complete = NewPromise<bool>();
        TTeardown teardown;
        teardown.Complete = complete;
        test->EnqueueCommand(teardown);

        return complete.GetFuture();
    }

    bool HandleStartSource(const TStartSource&)
    {
        auto testId = CreateGuidAsString();
        STORAGE_ERROR(
            "%s start test %s",
            MakeTestTag().c_str(),
            testId.c_str());

        SourceTest = CreateSimpleLoadTest(
            Config,
            Timer,
            Scheduler,
            Logging,
            ClientFactory,
            std::move(testId),
            Events);
        SourceFuture = SourceTest->Run();

        auto result = SendSetupTestSession(SourceTest, false, SessionSeqNo);
        if (!result.GetValueSync()) {
            return false;
        }

        auto period = Config.GetMigrationSpec().GetMigrationPeriod();
        if (period) {
            Scheduler->Schedule(
                Timer->Now() + TDuration::Seconds(period),
                [weakPtr = weak_from_this()] {
                    if (auto self = weakPtr.lock()) {
                        self->Events.Enqueue(TStartTarget{});
                    }
            });
        }
        return true;
    }

    bool HandleStartTarget(const TStartTarget&)
    {
        auto testId = CreateGuidAsString();
        STORAGE_INFO(
            "%s start a new client %s",
            MakeTestTag().c_str(),
            testId.c_str());

        TargetTest = CreateSimpleLoadTest(
            Config,
            Timer,
            Scheduler,
            Logging,
            ClientFactory,
            std::move(testId),
            Events);
        TargetFuture = TargetTest->Run();

        auto result = SendSetupTestSession(TargetTest, true, SessionSeqNo + 1);
        if (!result.GetValueSync()) {
            return false;
        }

        auto period = Config.GetMigrationSpec().GetStateTransferDelay();
        if (period) {
            Scheduler->Schedule(
                Timer->Now() + TDuration::Seconds(period),
                [weakPtr = weak_from_this()] {
                    if (auto self = weakPtr.lock()) {
                        self->Events.Enqueue(TFinishMigration{});
                    }
            });
        }
        return true;
    }

    bool HandleFinishMigration(const TFinishMigration&)
    {
        STORAGE_INFO(
            "%s switch to new client %s",
            MakeTestTag().c_str(),
            TargetTest->GetTestId().c_str());

        auto source = SendSetupTestSession(SourceTest, true, SessionSeqNo);
        auto target = SendSetupTestSession(TargetTest, false, ++SessionSeqNo);

        TVector<TFuture<bool>> futures {source, target};
        WaitAll(futures).Wait();
        if (!source.GetValue() || !target.GetValue()) {
            STORAGE_ERROR(
                "%s failed to switch to new client",
                MakeTestTag().c_str());
            return false;
        }

        TTeardown teardown;
        SourceTest->EnqueueCommand(teardown);
        SourceFuture.GetValueSync();

        SourceFuture = std::exchange(TargetFuture, TFuture<NProto::TTestStats>());
        std::swap(SourceTest, TargetTest);
        TargetTest.reset();

        auto period = Config.GetMigrationSpec().GetMigrationPeriod();
        Scheduler->Schedule(
            Timer->Now() + TDuration::Seconds(period),
            [weakPtr = weak_from_this()] {
                if (auto self = weakPtr.lock()) {
                    self->Events.Enqueue(TStartTarget());
                }
        });
        return true;
    }

    bool HandleTestFinished(const TTestFinished& cmd)
    {
        STORAGE_INFO(
            "%s test finished %s",
            MakeTestTag().c_str(),
            cmd.Id.c_str());

        if (SourceTest && SourceTest->GetTestId() == cmd.Id) {
            return false;
        }

        if (TargetTest && TargetTest->GetTestId() == cmd.Id) {
            return false;
        }

        return true;
    }

    bool HandleStateChange(TLoadTestControllerCommand cmd)
    {
        return std::visit(
            TOverloaded {
                [this] (const TStartSource& cmd) {
                    return HandleStartSource(cmd);
                },
                [this] (const TStartTarget& cmd) {
                    return HandleStartTarget(cmd);
                },
                [this] (const TFinishMigration& cmd) {
                    return HandleFinishMigration(cmd);
                },
                [this] (const TTestFinished& cmd) {
                    return HandleTestFinished(cmd);
                }
            },
            cmd);
    }

    void SetupTest()
    {
        Client->Start();

        MaxDuration = TDuration::Seconds(Config.GetTestDuration());

        if (Config.HasFileSystemId() && Config.HasCreateFileStoreRequest()) {
            ythrow yexception()
                << MakeTestTag()
                << " config should have either existing filesystem id or request to create one";
        }

        if (Config.HasCreateFileStoreRequest()) {
            auto request = std::make_shared<NProto::TCreateFileStoreRequest>(
                Config.GetCreateFileStoreRequest());

            STORAGE_INFO("%s create filestore: %s",
                MakeTestTag().c_str(),
                ProtoMessagePrinter.ToString(*request).c_str());

            TCallContextPtr ctx = MakeIntrusive<TCallContext>();
            auto result = Client->CreateFileStore(ctx, request);
            WaitForCompletion(GetRequestName(*request), result);
        }
    }

    void CleanupTest()
    {
        if (SourceTest) {
            auto result = SendTeardownTest(SourceTest).GetValueSync();
            if (!result) {
                STORAGE_ERROR(
                    "%s failed to teardown source client",
                    MakeTestTag().c_str());
            }
        }

        if (TargetTest) {
            auto result = SendTeardownTest(TargetTest).GetValueSync();
            if (!result) {
                STORAGE_ERROR(
                    "%s failed to teardown source client",
                    MakeTestTag().c_str());
            }
            TargetFuture.GetValueSync();
        }


        if (Config.HasCreateFileStoreRequest() && !Config.GetKeepFileStore()) {
            auto filesystemId =
                Config.GetCreateFileStoreRequest().GetFileSystemId();

            //if (Config.HasGarbageValidationSpec()) {
            ValidateGarbageCollection(filesystemId);
            //}

            STORAGE_INFO(
                "%s destroy fs %s",
                MakeTestTag().c_str(),
                filesystemId.c_str());

            auto request = std::make_shared<NProto::TDestroyFileStoreRequest>();
            request->SetFileSystemId(filesystemId);

            TCallContextPtr ctx = MakeIntrusive<TCallContext>();
            auto result = Client->DestroyFileStore(ctx, request);
            WaitForCompletion(GetRequestName(*request), result);
        }

        Client->Stop();

        // The future may already have exception set
        auto result = Result;
        result.TrySetValue(SourceFuture.GetValueSync());
    }

    void* ThreadProc() override
    {
        StartTs = TInstant::Now();
        NCloud::SetCurrentThreadName(MakeTestTag());

        SetupTest();

        while (!ShouldStop()) {
            try {
                Events.Wait();
                bool cont = true;
                while (auto maybeCmd = Events.Dequeue()) {
                    auto& cmd = *maybeCmd;
                    cont = HandleStateChange(cmd);
                    if (!cont) {
                        break;
                    }
                }
                if (!cont) {
                    break;
                }
            } catch(...) {
                STORAGE_ERROR("%s test has failed: %s",
                    MakeTestTag().c_str(), CurrentExceptionMessage().c_str());

                Result.SetException(std::current_exception());
            }
        }

        STORAGE_INFO("test is going to finish");

        CleanupTest();

        return nullptr;
    }

private:
    bool ShouldStop() const
    {
        return AtomicGet(Ctx.ShouldStop) ||
            (MaxDuration && TInstant::Now() - StartTs >= MaxDuration) ||
            (TargetFuture.HasValue() || SourceFuture.HasValue());
    }


    const TString& MakeTestTag() const
    {
        return TestTag;
    }

    NProtoPrivate::TStorageStats GetStorageStats(const TString& filesystemId)
    {
        NProtoPrivate::TGetStorageStatsRequest request;
        request.SetFileSystemId(filesystemId);
        request.SetCacheTTL(0);
        request.SetMode(NProtoPrivate::STATS_REQUEST_MODE_FORCE_FETCH_SHARDS);
        NProtoPrivate::TGetStorageStatsResponse response;
        ExecuteAction("getstoragestats", filesystemId, request, &response);
        if (response.HasError()) {
            STORAGE_ERROR("Failed to get storage stats");
        }
        return response.GetStats();
    }

    void ValidateStorageStats(const TString& filesystemId)
    {
        const auto& stats = GetStorageStats(filesystemId);
        bool res = true;

        TStringBuilder builder;
        bool first = true;
        auto check = [&](const char* name, auto value){
            if (value != 0) {
                if (first) {
                    first = false;
                } else {
                    builder << ", ";
                }
                builder << name << "=" << value;
            }
            res &= (value == 0);
        };

#define CHECK_STAT(x) check(#x, stats.Get##x())
        CHECK_STAT(FreshBlocksCount);
        CHECK_STAT(FreshBytesItemCount);
        CHECK_STAT(DeletedFreshBytesCount);
        CHECK_STAT(DeletionMarkersCount);
        CHECK_STAT(LargeDeletionMarkersCount);
        CHECK_STAT(GarbageQueueSize);
        CHECK_STAT(GarbageBlocksCount);
        CHECK_STAT(UnconfirmedDataCount);
#undef CHECK_STAT

        if (! res) {
            throw yexception() << "ValidateStorageStats failed: " << builder;
        }

        STORAGE_INFO("Storage GC stats validation passed");
    }

    void RunForcedOperation(const TString& filesystemId, NProtoPrivate::TForcedOperationRequest::EForcedOperationType type)
    {
        const auto& typeName = NProtoPrivate::TForcedOperationRequest::EForcedOperationType_Name(type);

        NProtoPrivate::TForcedOperationRequest request;
        request.SetFileSystemId(filesystemId);
        request.SetOpType(type);
        NProtoPrivate::TForcedOperationResponse response;
        ExecuteAction("forcedoperation", filesystemId, request, &response);
        if (response.HasError()) {
            throw yexception() << "failed to start forced operation "
                               << typeName << ": " << response.GetError();
        }

        while (true)
        {
            NProtoPrivate::TForcedOperationStatusRequest statusRequest;
            statusRequest.SetFileSystemId(filesystemId);
            statusRequest.SetOperationId(response.GetOperationId());
            NProtoPrivate::TForcedOperationStatusResponse statusResponse;
            ExecuteAction(
                "forcedoperationstatus",
                filesystemId,
                statusRequest,
                &statusResponse);

            if (statusResponse.GetError().GetCode() == E_NOT_FOUND) {
                // tablet rebooted
                throw yexception()
                    << typeName << ": operation not found (tablet rebooted?)";
                // restart the op ?
            }

            if (statusResponse.HasError()) {
                throw yexception() << typeName << ": failed to read op status: "
                                   << statusResponse.GetError();
            }

            bool done = false;
            switch (statusResponse.GetStatus()) {
                case NProtoPrivate::TForcedOperationStatusResponse::E_UNKNOWN:
                case NProtoPrivate::TForcedOperationStatusResponse::E_PENDING:
                case NProtoPrivate::TForcedOperationStatusResponse::E_RUNNING:
                    STORAGE_INFO(TStringBuilder() << typeName << ": waiting...");
                    break;
                case NProtoPrivate::TForcedOperationStatusResponse::E_COMPLETED:
                case NProtoPrivate::TForcedOperationStatusResponse::E_FAILED:
                    STORAGE_INFO(TStringBuilder() << typeName << ": completed");
                    done = true;
                    break;
                default:
                    STORAGE_INFO(
                        TStringBuilder() << typeName << ": unknown status");
                    break;
            }
            if (done) {
                break;
            }

            Sleep(TDuration::Seconds(1));
        }
    }

    template <typename TRequest, typename TResponse>
    void ExecuteAction(
        const TString& action,
        const TString& filesystemId,
        const TRequest& requestProto,
        TResponse* responseProto)
    {
        TString input;
        google::protobuf::util::MessageToJsonString(requestProto, &input);

        auto request = std::make_shared<NProto::TExecuteActionRequest>();
        request->SetAction(action);
        request->SetInput(std::move(input));

        const auto requestId = GetRequestId(*request);
        auto result = Client->ExecuteAction(
            MakeIntrusive<TCallContext>(filesystemId, requestId),
            std::move(request)).GetValueSync();

        if (HasError(result)) {
            responseProto->MutableError()->CopyFrom(result.GetError());
            return;
        }

        if (!google::protobuf::util::JsonStringToMessage(
                 result.GetOutput(),
                 responseProto)
                 .ok())
        {
            responseProto->MutableError()->CopyFrom(MakeError(
                E_BADMSG,
                TStringBuilder() << "failed to parse response json: "
                                 << result.GetOutput()));
        }
    }

    void CreateCommit(const TString& filesystemId)
    {
        NProto::TSessionConfig proto;
        proto.SetFileSystemId(filesystemId);
        proto.SetClientId(
            Config.GetClientId() ? Config.GetClientId() : "test-client");
        proto.SetSessionPingTimeout(Config.GetSessionPingTimeout());

        auto session = NClient::CreateSession(
            Logging,
            Timer,
            Scheduler,
            Client,
            std::make_shared<TSessionConfig>(proto));

        WaitForCompletion(
            "create session",
            session->CreateSession(false, SessionSeqNo));

        auto request = std::make_shared<NProto::TCreateNodeRequest>();
        request->SetNodeId(RootNodeId);
        request->SetName("commit");
        request->MutableFile()->SetMode(0777);

        auto response = WaitForCompletion(
            GetRequestName(*request),
            session->CreateNode(
                MakeIntrusive<TCallContext>(filesystemId),
                request));

        STORAGE_INFO(
            "commit node created: nodeId=%lu",
            response.GetNode().GetId());

        WaitForCompletion(
            "destroy session",
            session->DestroySession());
    }

    void ValidateGarbageCollection(const TString& filesystemId)
    {
        STORAGE_INFO("Starting forced garbage collection");
        try
        {
            RunForcedOperation(filesystemId, NProtoPrivate::TForcedOperationRequest::E_FLUSH);
            RunForcedOperation(filesystemId, NProtoPrivate::TForcedOperationRequest::E_FLUSH_BYTES);
            RunForcedOperation(filesystemId, NProtoPrivate::TForcedOperationRequest::E_CLEANUP);
            RunForcedOperation(filesystemId, NProtoPrivate::TForcedOperationRequest::E_COMPACTION);
            CreateCommit(filesystemId);
            RunForcedOperation(filesystemId, NProtoPrivate::TForcedOperationRequest::E_COLLECT_GARBAGE);
            ValidateStorageStats(filesystemId);
        }
        catch (...) {
            Result.SetException(std::current_exception());
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TString MakeTestTag(const TString& name)
{
    return Sprintf("[%s]", name.c_str());
}

ITestPtr CreateLoadTest(
    const TAppContext& ctx,
    const NProto::TLoadTest& config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IClientFactoryPtr clientFactory)
{
    return std::make_shared<TLoadTestController>(
        ctx,
        config,
        std::move(timer),
        std::move(scheduler),
        std::move(logging),
        std::move(clientFactory));
}

}   // namespace NCloud::NFileStore::NLoadTest
