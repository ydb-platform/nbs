#include <cloud/blockstore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/tools/analytics/libs/event-log/dump.h>

#include <library/cpp/eventlog/dumper/evlogdump.h>
#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/monlib/counters/histogram.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>

namespace {

using namespace NCloud::NBlockStore;

////////////////////////////////////////////////////////////////////////////////

// XXX hardcode
const auto ZERO_BLOCK_CHECKSUM = 2566472073;

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString EvlogDumperParamsStr;
    TString DiskId;
    size_t BlocksLimit = 0;
    TInstant Since;
    TInstant Till;
    bool NoInternalRequests = false;
    TVector<const char*> EvlogDumperArgv;

    TOptions(int argc, const char** argv)
    {
        using namespace NLastGetopt;

        TOpts opts;
        opts.AddHelpOption();

        opts.AddLongOption("evlog-dumper-params", "evlog dumper param string")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&EvlogDumperParamsStr);

        opts.AddLongOption("disk-id", "disk-id filter")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&DiskId);

        opts.AddLongOption("blocks-limit", "filter by request size")
            .Optional()
            .RequiredArgument("NUM")
            .StoreResult(&BlocksLimit);

        opts.AddLongOption("since", "filter out reuqests before date")
            .Optional()
            .RequiredArgument("DATE")
            .Handler1T<TString>([&] (const TString& val) {
                    Since = TInstant::ParseIso8601(val);
                });

        opts.AddLongOption("till", "filter out reuqests after date")
            .Optional()
            .RequiredArgument("DATE")
            .Handler1T<TString>([&] (const TString& val) {
                    Till = TInstant::ParseIso8601(val);
                });

        opts.AddLongOption("no-internal", "ignore internal requests")
            .NoArgument()
            .DefaultValue(NoInternalRequests)
            .SetFlag(&NoInternalRequests);

        TOptsParseResultException(&opts, argc, argv);

        if (Since && Till && Since > Till) {
            ythrow yexception() << "invalid date filter: "
                << Since.ToString() << " > " << Till.ToString();
        }


        EvlogDumperArgv.push_back("fake");

        TStringBuf sit(EvlogDumperParamsStr);
        TStringBuf arg;
        while (sit.NextTok(' ', arg)) {
            if (sit.size()) {
                const auto idx = EvlogDumperParamsStr.size() - sit.size() - 1;
                EvlogDumperParamsStr[idx] = 0;
            }
            EvlogDumperArgv.push_back(arg.data());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct IBlockInfoConsumer
{
    virtual ~IBlockInfoConsumer() {
    }

    virtual void UpdateBlock(ui64 commitId, ui64 blockIndex, ui32 checksum) = 0;
    virtual void RegisterAccess(
        TInstant ts,
        ui64 blockIndex,
        ui32 checksum,
        ui32 requestType) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TRequest
{
    TInstant Ts;
    EBlockStoreRequest Type;
    ui32 BlockIndex;
    ui32 BlockCount;
    TDuration Duration;

    TRequest(
            TInstant ts,
            EBlockStoreRequest type,
            ui32 blockIndex,
            ui32 blockCount,
            TDuration duration)
        : Ts(ts)
        , Type(type)
        , BlockIndex(blockIndex)
        , BlockCount(blockCount)
        , Duration(duration)
    {
    }

    bool operator<(const TRequest& r) const
    {
        return Ts < r.Ts;
    }
};

////////////////////////////////////////////////////////////////////////////////

constexpr auto WriteRequestType = static_cast<ui32>(
    EBlockStoreRequest::WriteBlocks
);
constexpr auto ZeroRequestType = static_cast<ui32>(
    EBlockStoreRequest::ZeroBlocks
);

////////////////////////////////////////////////////////////////////////////////

class TEventProcessor final
    : public TProtobufEventProcessor
{
private:
    const TOptions& Options;
    TVector<TRequest>* Requests;
    IBlockInfoConsumer* BlockInfoConsumer;
    ui64 LastTs = 0;

public:
    TEventProcessor(
            const TOptions& options,
            TVector<TRequest>* requests,
            IBlockInfoConsumer* blockInfoConsumer)
        : Options(options)
        , Requests(requests)
        , BlockInfoConsumer(blockInfoConsumer)
    {
    }

protected:
    void DoProcessEvent(const TEvent* ev, IOutputStream* out) override
    {
        Y_UNUSED(out);

        auto message =
            dynamic_cast<const NProto::TProfileLogRecord*>(ev->GetProto());
        if (message && message->GetDiskId() == Options.DiskId) {
            auto order = GetItemOrder(*message);

            for (const auto& id: order) {
                const auto ts = [&]()-> TMaybe<ui64> {
                    switch (id.Type) {
                        case EItemType::Request: {
                            return ProcessRequest(*message, id.Index);
                        }
                        case EItemType::BlockInfo: {
                            return ProcessBlockInfo(*message, id.Index);
                        }
                        default: {
                            return Nothing();
                        }
                    }
                }();

                if (!ts) {
                    continue;
                }

                LastTs = *ts;
            }
        }
    }

private:
    ui64 ProcessRequest(const NProto::TProfileLogRecord& record, int idx)
    {
        const auto& r = record.GetRequests(idx);
        OnRequest(r);

        if (r.GetRequestType() == ZeroRequestType) {
            for (const auto& range: r.GetRanges()) {
                for (ui64 i = 0; i < range.GetBlockCount(); ++i) {
                    BlockInfoConsumer->UpdateBlock(
                        0,  // TODO: log CommitId for request records
                        range.GetBlockIndex() + i,
                        ZERO_BLOCK_CHECKSUM
                    );
                }
            }
        }

        return r.GetTimestampMcs();
    }

    ui64 ProcessBlockInfo(const NProto::TProfileLogRecord& record, int idx)
    {
        const auto& bl = record.GetBlockInfoLists(idx);
        const auto maxReq = static_cast<ui32>(EBlockStoreRequest::MAX);

        if (bl.GetRequestType() == WriteRequestType) {
            for (const auto& bi: bl.GetBlockInfos()) {
                BlockInfoConsumer->UpdateBlock(
                    bl.GetCommitId(),
                    bi.GetBlockIndex(),
                    bi.GetChecksum()
                );
            }
        } else if (!Options.NoInternalRequests || bl.GetRequestType() < maxReq) {
            Y_ABORT_UNLESS(bl.GetRequestType() != ZeroRequestType);

            // deduplication needed - nbs can log different versions for the
            // same blockIndex - first some old versions and later on - the
            // last version
            THashMap<ui64, ui32> checksums;

            for (const auto& bi: bl.GetBlockInfos()) {
                checksums[bi.GetBlockIndex()] = bi.GetChecksum();
            }

            for (const auto [blockIndex, checksum]: checksums) {
                BlockInfoConsumer->RegisterAccess(
                    TInstant::MicroSeconds(bl.GetTimestampMcs()),
                    blockIndex,
                    checksum,
                    bl.GetRequestType()
                );
            }
        }

        return bl.GetTimestampMcs();
    }

    void OnRequest(const NProto::TProfileLogRequestInfo& r)
    {
        auto start = TInstant::MicroSeconds(r.GetTimestampMcs());
        if (Options.Since && start < Options.Since || Options.Till && start > Options.Till) {
            return;
        }

        if (Options.BlocksLimit) {
            ui64 blocks = 0;
            for (auto& range: r.GetRanges()) {
                blocks += range.GetBlockCount();
            }

            if (blocks > Options.BlocksLimit) {
                return;
            }
        }

        auto duration = TDuration::MicroSeconds(Max<ui64>(r.GetDurationMcs(), 1llu));
        auto end = start + duration;

        auto type = static_cast<EBlockStoreRequest>(r.GetRequestType());
        if (!IsReadWriteRequest(type)) {
            return;
        }

        ui64 startIndex;
        ui64 count;
        if (r.GetRanges().empty()) {
            startIndex = r.GetBlockIndex();
            count = r.GetBlockCount();
        } else {
            startIndex = r.GetRanges(0).GetBlockIndex();
            count = r.GetRanges(0).GetBlockCount();
        }

        Requests->emplace_back(
            start,
            type,
            startIndex,
            count,
            duration
        );

        Requests->emplace_back(
            end,
            type,
            startIndex,
            count,
            TDuration::Zero()
        );
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TIOStat
{
    TString Label;
    ui32 Current = 0;
    TInstant LastTs;
    TVector<TDuration> TimeAtDepth;
    TDuration TotalTime;

    NMonitoring::THdrHistogram Histogram;

    TIOStat(TString label)
        : Label(std::move(label))
        , TimeAtDepth(1)
        , Histogram(1, 10000000, 5)
    {
    }

    void OnRequest(TInstant ts, TDuration duration)
    {
        Y_ABORT_UNLESS(ts >= LastTs);

        if (LastTs.GetValue()) {
            if (TimeAtDepth.size() <= Current) {
                TimeAtDepth.resize(Current + 1);
            }

            TimeAtDepth[Current] += ts - LastTs;
            TotalTime += ts - LastTs;
        }

        LastTs = ts;
        if (duration) {
            ++Current;
            Histogram.RecordValue(duration.MicroSeconds());
        } else {
            Y_ABORT_UNLESS(Current);
            --Current;
        }
    }

    void Print()
    {
        Cout << Label << " idle time abs: " << TimeAtDepth[0] << Endl;

        double timeShare = 0;
        for (ui32 depth = 1; depth < TimeAtDepth.size(); ++depth) {
            if (TimeAtDepth[depth]) {
                timeShare += double(TimeAtDepth[depth].GetValue())
                    / (TotalTime - TimeAtDepth[0]).GetValue();
                Cout << Label << " at depth " << depth
                    << "\ttime abs: " << TimeAtDepth[depth]
                    << "\tcumulative time share: " << timeShare
                    << Endl;
            }
        }

        NMonitoring::THistogramSnapshot snapshot;
        Histogram.TakeSnaphot(&snapshot);

        Cout << Label << " latency stats(us):\n"
            << "\tp50: " << snapshot.Percentile50 << Endl
            << "\tp90: " << snapshot.Percentile90 << Endl
            << "\tp99: " << snapshot.Percentile99 << Endl
            << "\tp999: " << snapshot.Percentile999 << Endl
            << "\ttotal req: " << snapshot.TotalCount << Endl
            ;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TIntersectionStat
{
    THashMap<ui32, ui32> Block2ParallelAccesses;
    THashMap<ui32, ui32> Block2Inflight;

    void OnRequest(bool begin, ui32 blockIndex, ui32 blockCount)
    {
        for (ui32 i = 0; i < blockCount; ++i) {
            if (begin) {
                Ref(blockIndex + i);
            } else {
                UnRef(blockIndex + i);
            }
        }
    }

    void Ref(ui32 blockIndex)
    {
        auto& inflight = Block2Inflight[blockIndex];
        if (inflight) {
            ++Block2ParallelAccesses[blockIndex];
        }
        ++inflight;
    }

    void UnRef(ui32 blockIndex)
    {
        auto& inflight = Block2Inflight[blockIndex];
        Y_ABORT_UNLESS(inflight);
        if (--inflight == 0) {
            Block2Inflight.erase(blockIndex);
        }
    }

    void Print() const
    {
        Cout << "Block2ParallelAccesses:" << Endl;
        for (const auto& x: Block2ParallelAccesses) {
            Cout << x.first << "\t" << x.second << Endl;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockChecksumValidator: IBlockInfoConsumer
{
    struct TChecksum
    {
        ui64 CommitId = 0;
        ui32 Value = 0;
    };
    THashMap<ui64, TChecksum> Blocks;

    void UpdateBlock(ui64 commitId, ui64 blockIndex, ui32 checksum) override
    {
        auto& csum = Blocks[blockIndex];
        if (!commitId) {
            commitId = csum.CommitId;
        }
        if (commitId >= csum.CommitId) {
            csum.Value = checksum;
            csum.CommitId = commitId;
        }
    }

    void RegisterAccess(
        TInstant ts,
        ui64 blockIndex,
        ui32 checksum,
        ui32 requestType) override
    {
        auto it = Blocks.find(blockIndex);
        if (it != Blocks.end() && checksum != it->second.Value) {
            Cout << "corruption at " << ts
                << ", block " << blockIndex
                << ", request=" << RequestName(requestType)
                << ", old checksum=" << it->second.Value
                << ", new checksum=" << checksum
                << Endl;
        }

        const auto r = static_cast<ui32>(EBlockStoreRequest::ReadBlocks);
        const auto f = static_cast<ui32>(ESysRequestType::Flush);
        // XXX ReadBlocks and Flush can produce fake zero digests
        if (checksum != 0 || r != requestType && f != requestType) {
            // TODO: CommitId
            UpdateBlock(0, blockIndex, checksum);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    TOptions options(argc, argv);
    TVector<TRequest> requests;
    TBlockChecksumValidator validator;
    TEventProcessor processor(options, &requests, &validator);

    auto code = IterateEventLog(
        NEvClass::Factory(),
        &processor,
        options.EvlogDumperArgv.size(),
        options.EvlogDumperArgv.begin()
    );

    Sort(requests);

    TIOStat readStat("Read");
    TIOStat writeStat("Write");
    TIOStat zeroStat("Zero");
    TIntersectionStat intersectionStat;

    for (const auto& request: requests) {
        switch (request.Type) {
            case EBlockStoreRequest::ReadBlocks: {
                readStat.OnRequest(request.Ts, request.Duration);
                break;
            }

            case EBlockStoreRequest::WriteBlocks: {
                writeStat.OnRequest(request.Ts, request.Duration);
                break;
            }

            case EBlockStoreRequest::ZeroBlocks: {
                zeroStat.OnRequest(request.Ts, request.Duration);
                break;
            }

            default: {
            }
        }

        intersectionStat.OnRequest(
            (bool)request.Duration,
            request.BlockIndex,
            request.BlockCount
        );
    }

    readStat.Print();
    writeStat.Print();
    zeroStat.Print();
    intersectionStat.Print();

    return code;
}
