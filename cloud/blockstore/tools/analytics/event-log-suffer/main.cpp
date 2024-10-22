#include <cloud/blockstore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/blockstore/libs/diagnostics/volume_perf.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/tools/analytics/libs/event-log/dump.h>

#include <library/cpp/eventlog/dumper/evlogdump.h>
#include <library/cpp/getopt/small/last_getopt.h>

#include <util/datetime/base.h>
#include <util/datetime/cputimer.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace {

using namespace NCloud::NBlockStore;

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString EvlogDumperParamsStr;
    TString DiskId;
    TString StorageMediaKind;
    TInstant Since;
    TInstant Till;

    ui64 ReadIops = 0;
    ui64 ReadBandwidth = 0;
    ui64 WriteIops = 0;
    ui64 WriteBandwidth = 0;
    ui64 BlockSize = 0;

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

        opts.AddLongOption("storage-media-kind")
            .Required()
            .RequiredArgument("STR")
            .StoreResult(&StorageMediaKind);

        opts.AddLongOption("read-iops")
            .Required()
            .RequiredArgument("INTEGER")
            .StoreResult(&ReadIops);

        opts.AddLongOption("read-bandwidth")
            .Required()
            .RequiredArgument("INTEGER")
            .StoreResult(&ReadBandwidth);

        opts.AddLongOption("write-iops")
            .Required()
            .RequiredArgument("INTEGER")
            .StoreResult(&WriteIops);

        opts.AddLongOption("write-bandwidth")
            .Required()
            .RequiredArgument("INTEGER")
            .StoreResult(&WriteBandwidth);

        opts.AddLongOption("block-size")
            .Required()
            .RequiredArgument("INTEGER")
            .StoreResult(&BlockSize);

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

struct TRequest
{
    TInstant Ts;
    EBlockStoreRequest Type;
    ui32 BlockCount;
    TDuration Duration;
    TDuration PostponedTime;

    TRequest(
            TInstant ts,
            EBlockStoreRequest type,
            ui32 blockCount,
            TDuration duration,
            TDuration postponedTime)
        : Ts(ts)
        , Type(type)
        , BlockCount(blockCount)
        , Duration(duration)
        , PostponedTime(postponedTime)
    {
    }

    bool operator<(const TRequest& r) const
    {
        return Ts < r.Ts;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TEventProcessor final
    : public TProtobufEventProcessor
{
private:
    const TOptions& Options;
    TVector<TRequest>* Requests;
    ui64 LastTs = 0;

public:
    TEventProcessor(
            const TOptions& options,
            TVector<TRequest>* requests)
        : Options(options)
        , Requests(requests)
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

        return r.GetTimestampMcs();
    }

    void OnRequest(const NProto::TProfileLogRequestInfo& r)
    {
        auto start = TInstant::MicroSeconds(r.GetTimestampMcs());
        if (Options.Since && start < Options.Since || Options.Till && start > Options.Till) {
            return;
        }

        auto duration = TDuration::MicroSeconds(Max<ui64>(r.GetDurationMcs(), 1llu));
        auto postponed = TDuration::MicroSeconds(Max<ui64>(r.GetPostponedTimeMcs(), 1llu));

        auto type = static_cast<EBlockStoreRequest>(r.GetRequestType());
        if (!IsReadWriteRequest(type)) {
            return;
        }

        ui64 count;
        if (r.GetRanges().empty()) {
            count = r.GetBlockCount();
        } else {
            count = r.GetRanges(0).GetBlockCount();
        }

        Requests->emplace_back(
            start,
            type,
            count,
            duration,
            postponed
        );
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TSufferStat
{
    const TVector<TRequest>& Events;

    TVolumePerformanceCalculator PerfCalc;

    TSufferStat(
            const TVector<TRequest>& events,
            const NProto::TVolume& volume,
            TDiagnosticsConfigPtr diagnosticsConfig)
        : Events(events)
        , PerfCalc(volume, std::move(diagnosticsConfig))
    {
        PerfCalc.Register(volume);
    }

    void OnCompleteSecond(ui64 expectedCost, ui64 realCost)
    {
        auto calcExpectedCost = PerfCalc.GetExpectedCost().MicroSeconds();
        auto calcCurrentCost = PerfCalc.GetCurrentCost().MicroSeconds();
        bool curSuffer = PerfCalc.UpdateStats();
        Cout << " Exp: " << expectedCost
            << "\tReal: " << realCost
            << "\tCalc Exp: " << calcExpectedCost
            << "\tCalc Real: " << calcCurrentCost
            << "\t1 second suffer: " << curSuffer
            << "\t15 seconds suffer: " << PerfCalc.IsSuffering() << Endl;
    }

    void Print(ui64 blockSize)
    {
        if (Events.empty()) {
            return;
        }

        TInstant cTs = Events[0].Ts;
        ui64 expectedCost = 0;
        ui64 realCost = 0;
        ui64 reqCnt = 0;

        Cout << "[" << TInstant::Seconds(Events[0].Ts.Seconds()) << "]";

        for (const auto& e: Events) {
            auto type = static_cast<EBlockStoreRequest>(e.Type);

            if (!IsReadWriteRequest(type)) {
                continue;
            }

            if (e.Ts.MicroSeconds() > cTs.MicroSeconds() + 1e6) {
                OnCompleteSecond(expectedCost, realCost);
                Cout << "[" << TInstant::Seconds(e.Ts.Seconds()) << "]";
                expectedCost = 0;
                realCost = 0;
                reqCnt = 0;
                cTs = e.Ts;
            }

            auto requestBytes = e.BlockCount * blockSize;

            PerfCalc.OnRequestCompleted(
                type,
                DurationToCyclesSafe(e.Ts - Events[0].Ts - e.Duration),
                DurationToCyclesSafe(e.Ts - Events[0].Ts),
                DurationToCyclesSafe(e.PostponedTime),
                requestBytes);

            expectedCost += IsReadRequest(type) ?
                PerfCalc.GetExpectedReadCost(requestBytes).MicroSeconds() :
                PerfCalc.GetExpectedWriteCost(requestBytes).MicroSeconds();
            if (e.Duration > e.PostponedTime) {
                realCost += (e.Duration - e.PostponedTime).MicroSeconds();
            }
            ++reqCnt;
        }

        if (reqCnt) {
            OnCompleteSecond(expectedCost, realCost);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

NProto::TVolumePerfSettings& GetConfigSettings(
    NProto::TDiagnosticsConfig& diagnosticsConfig,
    NProto::EStorageMediaKind mediaKind)
{
    switch (mediaKind) {
        case NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED: {
            return *diagnosticsConfig.MutableNonreplPerfSettings();
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2: {
            return *diagnosticsConfig.MutableMirror2PerfSettings();
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3: {
            return *diagnosticsConfig.MutableMirror3PerfSettings();
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL: {
            return *diagnosticsConfig.MutableLocalSSDPerfSettings();
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD: {
            return *diagnosticsConfig.MutableSsdPerfSettings();
        }
        default: {
            return *diagnosticsConfig.MutableHddPerfSettings();
        }
    }
}

bool ParseMediaKind(const TStringBuf s, NProto::EStorageMediaKind& mediaKind)
{
    if (s == "ssd") {
        mediaKind = NProto::STORAGE_MEDIA_SSD;
    } else if (s == "hybrid") {
        mediaKind = NProto::STORAGE_MEDIA_HYBRID;
    } else if (s == "hdd") {
        mediaKind = NProto::STORAGE_MEDIA_HDD;
    } else if (s == "nonreplicated" || s == "ssd_nonrepl") {
        mediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED;
    } else if (s == "mirror2" || s == "ssd_mirror2") {
        mediaKind = NProto::STORAGE_MEDIA_SSD_MIRROR2;
    } else if (s == "mirror3" || s == "ssd_mirror3") {
        mediaKind = NProto::STORAGE_MEDIA_SSD_MIRROR3;
    } else if (s == "local" || s == "ssd_local") {
        mediaKind = NProto::STORAGE_MEDIA_SSD_LOCAL;
    } else {
        return false;
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

int main(int argc, const char** argv)
{
    TOptions options(argc, argv);

    NProto::EStorageMediaKind mediaKind;
    if (!ParseMediaKind(options.StorageMediaKind, mediaKind)) {
        Cerr  << "Failed to parse storage media kind: " << options.StorageMediaKind;
        return 0;
    }

    TVector<TRequest> requests;
    TEventProcessor processor(options, &requests);

    auto code = IterateEventLog(
        NEvClass::Factory(),
        &processor,
        options.EvlogDumperArgv.size(),
        options.EvlogDumperArgv.begin()
    );

    Sort(requests);

    NProto::TDiagnosticsConfig diag;

    auto& perf = GetConfigSettings(diag, mediaKind);
    perf.MutableWrite()->SetIops(options.WriteIops);
    perf.MutableWrite()->SetBandwidth(options.WriteBandwidth);
    perf.MutableRead()->SetIops(options.ReadIops);
    perf.MutableRead()->SetBandwidth(options.WriteBandwidth);

    NProto::TVolume volume;
    volume.SetDiskId(options.DiskId);
    volume.SetStorageMediaKind(NCloud::NProto::STORAGE_MEDIA_HYBRID);
    auto& profile = *volume.MutablePerformanceProfile();
    profile.SetMaxWriteIops(options.WriteIops);
    profile.SetMaxWriteBandwidth(options.WriteBandwidth);
    profile.SetMaxReadIops(options.ReadIops);
    profile.SetMaxWriteBandwidth(options.WriteBandwidth);

    TSufferStat sufferStats(
        requests,
        std::move(volume),
        std::make_shared<TDiagnosticsConfig>(diag));

    sufferStats.Print(4096);

    return code;
}
