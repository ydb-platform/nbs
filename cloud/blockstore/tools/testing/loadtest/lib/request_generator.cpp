#include "request_generator.h"

#include "range_allocator.h"
#include "range_map.h"

#include <cloud/blockstore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/blockstore/libs/service/request_helpers.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/eventlog/dumper/evlogdump.h>
#include <library/cpp/eventlog/eventlog.h>

#include <util/generic/algorithm.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/system/thread.h>
#include <util/thread/lfqueue.h>

namespace NCloud::NBlockStore::NLoadTest {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TArtificialRequestGenerator final: public IRequestGenerator
{
private:
    TLog Log;
    NProto::TRangeTest RangeTest;
    TRangeMap BlocksRange;
    TRangeAllocator RangeAllocator;

    TVector<std::pair<ui64, EBlockStoreRequest>> Rates;
    ui64 TotalRate = 0;
    ui64 SentRequestCount = 0;

public:
    TArtificialRequestGenerator(
        ILoggingServicePtr logging,
        NProto::TRangeTest range)
        : RangeTest(std::move(range))
        , BlocksRange(TBlockRange64::MakeClosedInterval(
              RangeTest.GetStart(),
              RangeTest.GetEnd()))
        , RangeAllocator(RangeTest)
    {
        Log = logging->CreateLog(Describe());

        SetupRequestWeights();
    }

    bool Next(TRequest* request) override;
    void Complete(TBlockRange64 blockRange) override;
    TString Describe() const override;
    bool HasMoreRequests() const override;

private:
    TMaybe<TBlockRange64> AllocateRange();
    void SetupRequestWeights();
    EBlockStoreRequest ChooseRequest() const;
};

////////////////////////////////////////////////////////////////////////////////

TString TArtificialRequestGenerator::Describe() const
{
    return TStringBuilder() << "Range[" << RangeTest.GetStart() << ','
                            << RangeTest.GetEnd() << ']';
}

bool TArtificialRequestGenerator::HasMoreRequests() const
{
    return !RangeTest.GetRequestsCount() ||
           RangeTest.GetRequestsCount() - SentRequestCount;
}

bool TArtificialRequestGenerator::Next(TRequest* request)
{
    auto blockRange = AllocateRange();
    if (!blockRange.Defined()) {
        STORAGE_WARN(
            "No free blocks found: (%lu) have = %s",
            BlocksRange.Size(),
            BlocksRange.DumpRanges().data());
        return false;
    }

    STORAGE_TRACE(
        "BlockAllocated: (%lu) allocated = %s left = %s",
        BlocksRange.Size(),
        DescribeRange(*blockRange).data(),
        BlocksRange.DumpRanges().data());

    request->RequestType = ChooseRequest();
    request->BlockRange = *blockRange;

    ++SentRequestCount;

    return true;
}

void TArtificialRequestGenerator::Complete(TBlockRange64 blockRange)
{
    STORAGE_TRACE(
        "Block Deallocated: (%lu) %s \n%s",
        BlocksRange.Size(),
        DescribeRange(blockRange).data(),
        BlocksRange.DumpRanges().data());

    if (RangeTest.GetAllowIntersectingRanges()) {
        return;
    }

    BlocksRange.PutBlock(blockRange);
}

void TArtificialRequestGenerator::SetupRequestWeights()
{
    if (RangeTest.GetWriteRate()) {
        TotalRate += RangeTest.GetWriteRate();
        Rates.emplace_back(TotalRate, EBlockStoreRequest::WriteBlocks);
    }

    if (RangeTest.GetReadRate()) {
        TotalRate += RangeTest.GetReadRate();
        Rates.emplace_back(TotalRate, EBlockStoreRequest::ReadBlocks);
    }

    if (RangeTest.GetZeroRate()) {
        TotalRate += RangeTest.GetZeroRate();
        Rates.emplace_back(TotalRate, EBlockStoreRequest::ZeroBlocks);
    }
}

EBlockStoreRequest TArtificialRequestGenerator::ChooseRequest() const
{
    auto it = LowerBound(
        Rates.begin(),
        Rates.end(),
        RandomNumber(TotalRate),
        [](const auto& a, const auto& b) { return a.first < b; });

    auto offset = std::distance(Rates.begin(), it);
    return Rates[offset].second;
}

TMaybe<TBlockRange64> TArtificialRequestGenerator::AllocateRange()
{
    const auto allocatedRange = RangeAllocator.AllocateRange();
    if (RangeTest.GetAllowIntersectingRanges()) {
        return allocatedRange;
    }

    return BlocksRange.GetBlock(
        allocatedRange,
        RangeTest.GetLoadType() == NProto::LOAD_TYPE_SEQUENTIAL);
}

////////////////////////////////////////////////////////////////////////////////

class TRealRequestGenerator final
    : public IRequestGenerator
    , public ISimpleThread
{
private:
    TLog Log;

    TString ProfileLogPath;
    TString DiskId;
    TString StartTime;
    TString EndTime;
    bool FullSpeed;
    ui64 MaxRequestsInMemory;
    TManualEvent Ev;
    std::atomic<bool>& ShouldStop;

    struct TRequestWithVersion
    {
        ui64 Version = 0;
        NProto::TProfileLogRequestInfo Request;

        bool operator<(const TRequestWithVersion& rhs) const
        {
            return Version == rhs.Version ? Request.GetTimestampMcs() <
                                                rhs.Request.GetTimestampMcs()
                                          : Version < rhs.Version;
        }

        bool HasValue() const
        {
            return Request.RangesSize();
        }
    };

    struct TQueueCounter
    {
        ui64 Count = 0;

        template <typename T>
        void IncCount(const T& /* data */)
        {
            Count += 1;
        }

        template <typename T>
        void DecCount(const T& /* data */)
        {
            Count -= 1;
        }
    };

    mutable TLockFreeQueue<TRequestWithVersion, TQueueCounter> Requests;
    TRequestWithVersion Head;
    std::atomic<bool> FinishedRead = false;
    mutable TInstant StartTs;
    mutable TInstant FirstRequestTs;
    mutable ui64 CurrentLogVersion = 0;

    struct TCompareByEnd
    {
        using is_transparent = void;

        bool operator()(const auto& lhs, const auto& rhs) const
        {
            return GetEnd(lhs) < GetEnd(rhs);
        }

        static ui64 GetEnd(const TBlockRange64& blockRange)
        {
            return blockRange.End;
        }

        static ui64 GetEnd(ui64 End)
        {
            return End;
        }
    };

    TSet<TBlockRange64, TCompareByEnd> InFlight;

public:
    TRealRequestGenerator(
        ILoggingServicePtr logging,
        TString profileLogPath,
        TString diskId,
        TString startTime,
        TString endTime,
        bool fullSpeed,
        ui64 maxRequestsInMemory,
        std::atomic<bool>& shouldStop)
        : ProfileLogPath(std::move(profileLogPath))
        , DiskId(std::move(diskId))
        , StartTime(std::move(startTime))
        , EndTime(std::move(endTime))
        , FullSpeed(fullSpeed)
        , MaxRequestsInMemory(maxRequestsInMemory)
        , ShouldStop(shouldStop)
    {
        Log = logging->CreateLog(Describe());

        Start();
    }

    ~TRealRequestGenerator()
    {
        while (!FinishedRead.load(std::memory_order_acquire)) {
            Sleep(TDuration::Seconds(1));
        }
    }

    bool Next(TRequest* request) override;
    TInstant Peek() override;
    void Complete(TBlockRange64 blockRange) override;
    TString Describe() const override;
    bool HasMoreRequests() const override;

    void* ThreadProc() override;

private:
    static EBlockStoreRequest RequestType(ui32 intType)
    {
        return static_cast<EBlockStoreRequest>(intType);
    }

    TInstant Timestamp(const TRequestWithVersion& r) const;
    void ReadRequests();

    bool StopRequested()
    {
        return ShouldStop.load(std::memory_order_acquire);
    }

    void Dequeue()
    {
        if (Head.HasValue()) {
            return;
        }

        if (Requests.Dequeue(&Head)) {
            Ev.Reset();
            return;
        }

        Ev.WaitI();

        if (Requests.Dequeue(&Head)) {
            Ev.Reset();
            return;
        }

        Y_ABORT_UNLESS(FinishedRead.load(std::memory_order_acquire));
    }
};

////////////////////////////////////////////////////////////////////////////////

TString TRealRequestGenerator::Describe() const
{
    return TStringBuilder() << "LogReplay[" << ProfileLogPath << "]";
}

bool TRealRequestGenerator::HasMoreRequests() const
{
    return Requests.GetCounter().Count ||
           !FinishedRead.load(std::memory_order_acquire) || Head.HasValue();
}

bool TRealRequestGenerator::Next(TRequest* request)
{
    Dequeue();

    if (!FullSpeed) {
        auto now = Now();
        if (now < Timestamp(Head)) {
            return false;
        }
    }

    request->RequestType = RequestType(Head.Request.GetRequestType());
    if (Head.Request.GetRanges().empty()) {
        request->BlockRange = TBlockRange64::WithLength(
            Head.Request.GetBlockIndex(),
            Head.Request.GetBlockCount());
    } else {
        request->BlockRange = TBlockRange64::WithLength(
            Head.Request.GetRanges(0).GetBlockIndex(),
            Head.Request.GetRanges(0).GetBlockCount());
    }

    auto it = InFlight.lower_bound(request->BlockRange.Start);
    if (it != InFlight.end() && it->Start <= request->BlockRange.End) {
        return false;
    }

    InFlight.insert(request->BlockRange);
    Head.Request.Clear();

    return true;
}

TInstant TRealRequestGenerator::Peek()
{
    Dequeue();

    if (!Head.HasValue()) {
        return TInstant::Zero();
    }

    return Timestamp(Head);
}

void TRealRequestGenerator::Complete(TBlockRange64 blockRange)
{
    InFlight.erase(blockRange);
}

TInstant TRealRequestGenerator::Timestamp(const TRequestWithVersion& r) const
{
    auto requestTs = TInstant::MicroSeconds(r.Request.GetTimestampMcs());

    if (CurrentLogVersion != r.Version || StartTs == TInstant::Zero()) {
        CurrentLogVersion = r.Version;
        StartTs = Now();
        FirstRequestTs = requestTs;
    }

    return StartTs + (requestTs - FirstRequestTs);
}

void* TRealRequestGenerator::ThreadProc()
{
    ReadRequests();
    return nullptr;
}

void TRealRequestGenerator::ReadRequests()
{
    struct TEventProcessor: TProtobufEventProcessor
    {
        TRealRequestGenerator& Parent;

        TEventProcessor(TRealRequestGenerator& parent)
            : Parent(parent)
        {}

        void DoProcessEvent(const TEvent* ev, IOutputStream* out) override
        {
            Y_UNUSED(out);

            Y_ENSURE(!Parent.StopRequested(), "Exit from reading test data");

            if (Parent.MaxRequestsInMemory) {
                while (Parent.Requests.GetCounter().Count >=
                       Parent.MaxRequestsInMemory)
                {
                    Sleep(TDuration::MilliSeconds(1));
                    Y_ENSURE(
                        !Parent.StopRequested(),
                        "Exit from reading test data");
                }
            }

            auto message =
                dynamic_cast<const NProto::TProfileLogRecord*>(ev->GetProto());

            if (message && message->GetDiskId() == Parent.DiskId) {
                TVector<TRequestWithVersion> readRecords;
                readRecords.reserve(message->RequestsSize());
                for (const auto& r: message->GetRequests()) {
                    if (IsReadWriteRequest(RequestType(r.GetRequestType()))) {
                        readRecords.push_back({ev->Timestamp, r});
                    }
                }

                Sort(readRecords);
                for (auto& r: readRecords) {
                    Parent.Requests.Enqueue(std::move(r));
                }
                Parent.Ev.Signal();
            }
        }
    };

    TEventProcessor eventProcessor(*this);

    const char* argv[6];
    size_t argc = 0;

    argv[argc++] = "foo";
    if (StartTime.size()) {
        argv[argc++] = "--start-time";
        argv[argc++] = StartTime.c_str();
    }
    if (EndTime.size()) {
        argv[argc++] = "--end-time";
        argv[argc++] = EndTime.c_str();
    }
    argv[argc++] = ProfileLogPath.c_str();

    auto code =
        IterateEventLog(NEvClass::Factory(), &eventProcessor, argc, argv);

    if (code && !StopRequested()) {
        ythrow yexception()
            << "profile log iteration has failed, code " << code;
    }

    FinishedRead.store(true, std::memory_order_release);

    Ev.Signal();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRequestGeneratorPtr CreateArtificialRequestGenerator(
    ILoggingServicePtr loggingService,
    NProto::TRangeTest range)
{
    return std::make_shared<TArtificialRequestGenerator>(
        std::move(loggingService),
        std::move(range));
}

IRequestGeneratorPtr CreateRealRequestGenerator(
    ILoggingServicePtr loggingService,
    TString profileLogPath,
    TString diskId,
    TString startTime,
    TString endTime,
    bool fullSpeed,
    ui64 maxRequestsInMemory,
    std::atomic<bool>& shouldStop)
{
    return std::make_shared<TRealRequestGenerator>(
        std::move(loggingService),
        std::move(profileLogPath),
        std::move(diskId),
        std::move(startTime),
        std::move(endTime),
        fullSpeed,
        maxRequestsInMemory,
        shouldStop);
}

}   // namespace NCloud::NBlockStore::NLoadTest
