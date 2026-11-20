#include <cloud/fastshard/libs/journal/iface/device.h>
#include <cloud/fastshard/libs/journal/iface/journalled_device.h>
#include <cloud/fastshard/libs/journal/impl/device_page_store.h>
#include <cloud/fastshard/libs/journal/impl/file_device.h>
#include <cloud/fastshard/libs/journal/impl/journal.h>
#include <cloud/fastshard/libs/journal/impl/journalled_device_v1.h>
#include <cloud/fastshard/libs/journal/impl/journalled_device_v2.h>
#include <cloud/fastshard/libs/journal/impl/key_buffer_store.h>
#include <cloud/fastshard/libs/journal/impl/log_record.h>
#include <cloud/fastshard/libs/journal/impl/memory_device.h>
#include <cloud/fastshard/public/api/protos/device.pb.h>

#include <cloud/storage/core/libs/aio/service.h>
#include <cloud/storage/core/libs/common/affinity.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/file_io_service.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/getopt/small/last_getopt.h>
#include <library/cpp/json/writer/json.h>
#include <library/cpp/string_utils/parse_size/parse_size.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/random/fast.h>
#include <util/stream/file.h>
#include <util/stream/format.h>
#include <util/stream/output.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/system/file.h>
#include <util/system/fstat.h>
#include <util/system/spinlock.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <thread>

#include <unistd.h>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TStringBuf DeviceUUID = "bench-device";

// How many pages one prefill request carries.
constexpr ui64 PrefillRequestPages = 256;

// How many page groups one prefill record carries at most. The record must
// fit into one page of the journal metadata.
constexpr int PrefillRecordGroups = 64;

constexpr auto MinRetryDelay = std::chrono::milliseconds(1);
constexpr auto MaxRetryDelay = std::chrono::milliseconds(100);

////////////////////////////////////////////////////////////////////////////////

enum class EBacking
{
    // The pages live in the process memory. Shows the cost of the device
    // code itself.
    Memory,

    // The pages live in a file opened with O_DIRECT.
    File,
};

enum class EDeviceVersion
{
    // No journal: a log record is written right to the data area.
    V1,

    // The journalled device.
    V2,
};

enum class EWorkload
{
    // WriteLogRecord requests.
    Write,

    // ReadPages requests, every page is in the journal.
    ReadJournal,

    // ReadPages requests, every page is in the data area.
    ReadData,

    // ReadPages requests, half of the pages are in the journal.
    ReadMixed,
};

EBacking ParseBacking(const TString& value)
{
    if (value == "memory") {
        return EBacking::Memory;
    }
    if (value == "file") {
        return EBacking::File;
    }
    ythrow yexception() << "--backing: unknown value " << value.Quote();
}

EDeviceVersion ParseDeviceVersion(const TString& value)
{
    if (value == "v1") {
        return EDeviceVersion::V1;
    }
    if (value == "v2") {
        return EDeviceVersion::V2;
    }
    ythrow yexception() << "--device: unknown value " << value.Quote();
}

EWorkload ParseWorkload(const TString& value)
{
    if (value == "write") {
        return EWorkload::Write;
    }
    if (value == "read-journal") {
        return EWorkload::ReadJournal;
    }
    if (value == "read-data") {
        return EWorkload::ReadData;
    }
    if (value == "read-mixed") {
        return EWorkload::ReadMixed;
    }
    ythrow yexception() << "--workload: unknown value " << value.Quote();
}

////////////////////////////////////////////////////////////////////////////////

struct TOptions
{
    TString DeviceVersionName = "v2";
    EDeviceVersion DeviceVersion = EDeviceVersion::V2;

    TString BackingName = "memory";
    EBacking Backing = EBacking::Memory;
    TString DevicePath;
    bool Sparse = false;

    TString WorkloadName;
    EWorkload Workload = EWorkload::Write;

    ui32 PageSize = 4_KB;
    ui64 Pages = 1;
    ui32 IoDepth = 1;
    ui32 Duration = 0;
    ui64 Requests = 0;

    ui64 LogMetaSize = 93_MB;
    ui64 LogDataSize = 930_MB;
    ui64 DataSize = 1_GB;
    ui64 WorkingSet = 256_MB;

    ui64 AdvanceEvery = 256;
    ui32 RetryTimeout = 30;

    ui32 AioThreads = 1;
    TVector<ui8> ExecutorCores;
    bool Spin = false;

    TString ResultsPath;
    bool Verbose = false;

    void Parse(int argc, char** argv);
    void Validate() const;

    ui64 GetLogMetaPages() const
    {
        return LogMetaSize / PageSize;
    }

    ui64 GetLogDataPages() const
    {
        return LogDataSize / PageSize;
    }

    ui64 GetDataPages() const
    {
        return DataSize / PageSize;
    }

    ui64 GetDevicePages() const
    {
        return GetLogMetaPages() + GetLogDataPages() + GetDataPages();
    }

    // The requests address whole slots of |Pages| pages.
    ui64 GetSlotCount() const
    {
        return WorkingSet / PageSize / Pages;
    }

    bool IsRead() const
    {
        return Workload != EWorkload::Write;
    }

    bool HasJournal() const
    {
        return DeviceVersion == EDeviceVersion::V2;
    }
};

void TOptions::Parse(int argc, char** argv)
{
    using namespace NLastGetopt;

    TOpts opts;
    opts.AddHelpOption('h');
    opts.SetFreeArgsNum(0);
    opts.SetTitle(
        "Benchmark of the journalled device: calls ReadPages and "
        "WriteLogRecord right on the device, without the network");

    opts.AddLongOption(
            "device",
            "v2 is the journalled device, v1 is the device without a journal")
        .RequiredArgument("STR")
        .DefaultValue(DeviceVersionName)
        .StoreResult(&DeviceVersionName);

    opts.AddLongOption("workload", "write, read-journal, read-data, read-mixed")
        .Required()
        .RequiredArgument("STR")
        .StoreResult(&WorkloadName);

    opts.AddLongOption("backing", "where the pages live: memory or file")
        .RequiredArgument("STR")
        .DefaultValue(BackingName)
        .StoreResult(&BackingName);

    opts.AddLongOption(
            "device-path",
            "file for --backing file; the benchmark overwrites it")
        .RequiredArgument("PATH")
        .StoreResult(&DevicePath);

    opts.AddLongOption(
            "sparse",
            "do not write the file through before the run; the file system "
            "then allocates blocks during the run")
        .NoArgument()
        .SetFlag(&Sparse);

    TString pageSize = ToString(PageSize);
    opts.AddLongOption("page-size", "page size in bytes")
        .RequiredArgument("SIZE")
        .DefaultValue(pageSize)
        .StoreResult(&pageSize);

    opts.AddLongOption("pages", "pages per request")
        .RequiredArgument("NUM")
        .DefaultValue(Pages)
        .StoreResult(&Pages);

    opts.AddLongOption("iodepth", "requests in flight")
        .RequiredArgument("NUM")
        .DefaultValue(IoDepth)
        .StoreResult(&IoDepth);

    opts.AddLongOption("duration", "seconds to run")
        .RequiredArgument("NUM")
        .StoreResult(&Duration);

    opts.AddLongOption("requests", "requests to issue")
        .RequiredArgument("NUM")
        .StoreResult(&Requests);

    TString logMetaSize = ToString(LogMetaSize);
    opts.AddLongOption("log-meta-size", "journal metadata part size")
        .RequiredArgument("SIZE")
        .DefaultValue(logMetaSize)
        .StoreResult(&logMetaSize);

    TString logDataSize = ToString(LogDataSize);
    opts.AddLongOption("log-data-size", "journal data part size")
        .RequiredArgument("SIZE")
        .DefaultValue(logDataSize)
        .StoreResult(&logDataSize);

    TString dataSize = ToString(DataSize);
    opts.AddLongOption("data-size", "data part size")
        .RequiredArgument("SIZE")
        .DefaultValue(dataSize)
        .StoreResult(&dataSize);

    TString workingSet = ToString(WorkingSet);
    opts.AddLongOption(
            "working-set",
            "size of the start of the data part that the requests address")
        .RequiredArgument("SIZE")
        .DefaultValue(workingSet)
        .StoreResult(&workingSet);

    opts.AddLongOption(
            "advance-every",
            "advance the lsn low watermark every NUM written records; 0 "
            "never advances it, so nothing is flushed")
        .RequiredArgument("NUM")
        .DefaultValue(AdvanceEvery)
        .StoreResult(&AdvanceEvery);

    opts.AddLongOption(
            "retry-timeout",
            "seconds to keep retrying a write that the full journal rejects")
        .RequiredArgument("NUM")
        .DefaultValue(RetryTimeout)
        .StoreResult(&RetryTimeout);

    opts.AddLongOption("aio-threads", "AIO completion threads")
        .RequiredArgument("NUM")
        .DefaultValue(AioThreads)
        .StoreResult(&AioThreads);

    TString executorCores;
    opts.AddLongOption(
            "executor-cores",
            "pin the executor thread to these cores, for example 2,3")
        .RequiredArgument("LIST")
        .StoreResult(&executorCores);

    opts.AddLongOption(
            "spin",
            "wait for a response in a busy loop and not in a sleep; takes "
            "one core per request in flight")
        .NoArgument()
        .SetFlag(&Spin);

    opts.AddLongOption("results", "write the results as JSON to this file")
        .RequiredArgument("PATH")
        .StoreResult(&ResultsPath);

    opts.AddLongOption("verbose", "enable debug logging")
        .NoArgument()
        .SetFlag(&Verbose);

    TOptsParseResult parseResult(&opts, argc, argv);

    DeviceVersion = ParseDeviceVersion(DeviceVersionName);
    Backing = ParseBacking(BackingName);
    Workload = ParseWorkload(WorkloadName);

    const ui64 pageSizeValue = NSize::ParseSize(pageSize);
    Y_ENSURE(pageSizeValue <= Max<ui32>(), "--page-size is too large");
    PageSize = static_cast<ui32>(pageSizeValue);

    LogMetaSize = NSize::ParseSize(logMetaSize);
    LogDataSize = NSize::ParseSize(logDataSize);
    DataSize = NSize::ParseSize(dataSize);
    WorkingSet = NSize::ParseSize(workingSet);

    auto cores = StringSplitter(executorCores).Split(',').SkipEmpty();
    for (const auto& token: cores) {
        ExecutorCores.push_back(FromString<ui8>(token.Token()));
    }

    Validate();
}

void TOptions::Validate() const
{
    Y_ENSURE(PageSize, "--page-size must be positive");
    Y_ENSURE(Pages, "--pages must be positive");
    Y_ENSURE(IoDepth, "--iodepth must be positive");
    Y_ENSURE(AioThreads, "--aio-threads must be positive");
    Y_ENSURE(Duration || Requests, "set --duration or --requests");

    Y_ENSURE(
        LogMetaSize % PageSize == 0 && LogDataSize % PageSize == 0 &&
            DataSize % PageSize == 0,
        "the part sizes must be multiples of --page-size");
    Y_ENSURE(
        GetLogMetaPages() && GetLogDataPages() && GetDataPages(),
        "the part sizes must be positive");

    Y_ENSURE(
        WorkingSet <= DataSize,
        "--working-set is larger than --data-size");
    Y_ENSURE(GetSlotCount(), "--working-set is smaller than one request");

    if (Backing == EBacking::File) {
        Y_ENSURE(DevicePath, "--backing file needs --device-path");
    }

    if (!HasJournal()) {
        Y_ENSURE(
            Workload == EWorkload::Write || Workload == EWorkload::ReadData,
            "--device v1 has no journal: use the write or read-data workload");
        return;
    }

    //
    // Check that the pages which stay in the journal fit into it. Nothing
    // frees them during the run
    //

    if (Workload == EWorkload::ReadJournal || Workload == EWorkload::ReadMixed)
    {
        const ui64 pages = Workload == EWorkload::ReadJournal
                               ? WorkingSet / PageSize
                               : WorkingSet / PageSize / 2;
        Y_ENSURE(
            pages <= GetLogDataPages(),
            "the journal can not hold the working set: lower --working-set "
            "or raise --log-data-size");
    }

    if (Workload == EWorkload::Write && !AdvanceEvery) {
        Y_ENSURE(
            Requests && Requests * Pages <= GetLogDataPages(),
            "with --advance-every 0 nothing is flushed, so set --requests "
            "to a number of records that fits into --log-data-size");
    }
}

////////////////////////////////////////////////////////////////////////////////

ui64 NowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

[[noreturn]] void Fail(TStringBuf action, const NProto::TError& error)
{
    Cerr << action << " failed: " << FormatError(error) << Endl;
    _exit(1);
}

TString MakePage(ui32 pageSize, ui64 seed)
{
    TString page;
    page.resize(pageSize);

    TFastRng64 rng(seed + 1);
    for (ui32 i = 0; i + sizeof(ui64) <= pageSize; i += sizeof(ui64)) {
        const ui64 value = rng.GenRand();
        memcpy(page.begin() + i, &value, sizeof(value));
    }

    return page;
}

// Every page gets its own copy of the content, as in a request that came
// from the network.
template <typename TRequest>
void AddPageGroup(TRequest& request, TPageRangeRef range, const TString& page)
{
    auto& group = *request.AddPageGroups();
    group.SetFirstPageNo(range.FirstPageNo);

    for (ui64 i = 0; i < range.PageCount; ++i) {
        group.AddContent(TString(page.data(), page.size()));
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TActionStats
{
    TVector<ui64> LatenciesNs;
    ui64 Retries = 0;

    void Merge(const TActionStats& other)
    {
        LatenciesNs.insert(
            LatenciesNs.end(),
            other.LatenciesNs.begin(),
            other.LatenciesNs.end());
        Retries += other.Retries;
    }
};

struct TWorkerStats
{
    TActionStats Main;
    TActionStats Advance;
};

struct TActionSummary
{
    TString Name;
    ui64 Ok = 0;
    ui64 Retries = 0;
    double Iops = 0;
    double MbPerSecond = 0;
    double MeanUs = 0;
    double P50Us = 0;
    double P90Us = 0;
    double P99Us = 0;
    double P999Us = 0;
    double MaxUs = 0;
};

double PercentileUs(const TVector<ui64>& sorted, double percentile)
{
    const auto index = static_cast<size_t>(
        std::ceil(percentile / 100 * static_cast<double>(sorted.size())));
    return static_cast<double>(sorted[index ? index - 1 : 0]) / 1000;
}

TActionSummary Summarize(
    TString name,
    TActionStats stats,
    double elapsedSeconds,
    ui64 bytesPerRequest)
{
    TActionSummary summary;
    summary.Name = std::move(name);
    summary.Ok = stats.LatenciesNs.size();
    summary.Retries = stats.Retries;

    if (!summary.Ok) {
        return summary;
    }

    auto& latencies = stats.LatenciesNs;
    Sort(latencies);

    const auto count = static_cast<double>(summary.Ok);

    summary.Iops = count / elapsedSeconds;
    summary.MbPerSecond =
        count * static_cast<double>(bytesPerRequest) / elapsedSeconds / 1_MB;
    summary.MeanUs =
        static_cast<double>(Accumulate(latencies, ui64{0})) / count / 1000;
    summary.P50Us = PercentileUs(latencies, 50);
    summary.P90Us = PercentileUs(latencies, 90);
    summary.P99Us = PercentileUs(latencies, 99);
    summary.P999Us = PercentileUs(latencies, 99.9);
    summary.MaxUs = static_cast<double>(latencies.back()) / 1000;

    return summary;
}

void Print(const TActionSummary& s)
{
    Cout << s.Name << ": " << s.Ok << " ok, " << s.Retries << " retries, "
         << Prec(s.Iops, PREC_POINT_DIGITS, 1) << " req/s, "
         << Prec(s.MbPerSecond, PREC_POINT_DIGITS, 2)
         << " MB/s, latency us: mean " << Prec(s.MeanUs, PREC_POINT_DIGITS, 1)
         << " p50 " << Prec(s.P50Us, PREC_POINT_DIGITS, 1) << " p90 "
         << Prec(s.P90Us, PREC_POINT_DIGITS, 1) << " p99 "
         << Prec(s.P99Us, PREC_POINT_DIGITS, 1) << " p99.9 "
         << Prec(s.P999Us, PREC_POINT_DIGITS, 1) << " max "
         << Prec(s.MaxUs, PREC_POINT_DIGITS, 1) << Endl;
}

void WriteJson(NJsonWriter::TBuf& json, const TActionSummary& s)
{
    json.WriteKey(s.Name).BeginObject();
    json.WriteKey("ok").WriteULongLong(s.Ok);
    json.WriteKey("retries").WriteULongLong(s.Retries);
    json.WriteKey("iops").WriteDouble(s.Iops);
    json.WriteKey("mb_per_second").WriteDouble(s.MbPerSecond);
    json.WriteKey("latency_us").BeginObject();
    json.WriteKey("mean").WriteDouble(s.MeanUs);
    json.WriteKey("p50").WriteDouble(s.P50Us);
    json.WriteKey("p90").WriteDouble(s.P90Us);
    json.WriteKey("p99").WriteDouble(s.P99Us);
    json.WriteKey("p999").WriteDouble(s.P999Us);
    json.WriteKey("max").WriteDouble(s.MaxUs);
    json.EndObject();
    json.EndObject();
}

////////////////////////////////////////////////////////////////////////////////

// Creates the file anew, so that the device starts with an empty journal,
// and writes it through unless |sparse| is set.
void PrepareFile(const TString& path, ui64 size, bool sparse)
{
    TFileStat stat(path);
    Y_ENSURE(
        stat.IsNull() || stat.IsFile(),
        path.Quote() << " is not a regular file");

    TFile file(path, CreateAlways | WrOnly);

    if (sparse) {
        return;
    }

    Cerr << "Writing " << path.Quote() << " through..." << Endl;

    const TVector<char> zeroes(1_MB, 0);
    for (ui64 offset = 0; offset < size; offset += zeroes.size()) {
        file.Write(zeroes.data(), Min<ui64>(zeroes.size(), size - offset));
    }

    file.Flush();
}

////////////////////////////////////////////////////////////////////////////////

class TBench
{
private:
    const TOptions Options;

    ILoggingServicePtr Logging;
    IFileIOServicePtr FileIO;
    TExecutorPtr Executor;
    IJournalledDevicePtr Device;

    // The lsn of the newest record that was given to a writer.
    std::atomic<ui64> LastLsn = 0;

    std::atomic<ui64> IssuedRequests = 0;
    ui64 DeadlineNs = 0;

public:
    explicit TBench(TOptions options)
        : Options(std::move(options))
    {}

    void Start();
    void Prefill();
    void Run();
    void Stop();

private:
    // Waits for the future and returns the time when it got the value.
    template <typename T>
    ui64 WaitFor(const TFuture<T>& future) const;

    bool TakeRequest();
    TPageRangeRef PickSlot(TFastRng64& rng) const;

    void PrefillData();
    void PrefillJournal(const TVector<TPageRangeRef>& ranges);

    void RunWorker(ui32 index, TWorkerStats& stats);
    void WriteLogRecord(
        TFastRng64& rng,
        const TString& page,
        TWorkerStats& stats);
    void AdvanceLsnLowWatermark(ui64 lsn, TActionStats& stats);
    void ReadPages(TFastRng64& rng, TActionStats& stats);

    void Report(TWorkerStats stats, double elapsedSeconds) const;
};

////////////////////////////////////////////////////////////////////////////////

void TBench::Start()
{
    Logging = CreateLoggingService(
        "console",
        {.FiltrationLevel = Options.Verbose ? TLOG_DEBUG : TLOG_INFO});
    Logging->Start();

    const ui32 pageSize = Options.PageSize;
    const ui64 logMetaPages = Options.GetLogMetaPages();
    const ui64 logDataPages = Options.GetLogDataPages();
    const ui64 dataPages = Options.GetDataPages();

    IDevicePtr device;

    if (Options.Backing == EBacking::File) {
        PrepareFile(
            Options.DevicePath,
            Options.GetDevicePages() * pageSize,
            Options.Sparse);

        FileIO = Options.AioThreads == 1
                     ? CreateAIOService()
                     : CreateThreadedAIOService(Options.AioThreads);
        FileIO->Start();

        device = CreateFileDevice(
            FileIO,
            Options.DevicePath,
            Options.GetDevicePages(),
            pageSize);
    } else {
        device = CreateInMemoryDevice(pageSize);
    }

    Executor = TExecutor::Create("JD", TAffinity(Options.ExecutorCores));
    Executor->Start();

    //
    // Split the device into the journal metadata, the journal data and the
    // data, in this order. This is the layout that the server builds
    //

    auto dataStore = CreateRegionDevice(
        device,
        {.FirstPageNo = logMetaPages + logDataPages, .PageCount = dataPages});

    if (!Options.HasJournal()) {
        //
        // Skip the journal parts, as the server does: the data stays where
        // the journalled device keeps it
        //

        Device = CreateJournalledDeviceV1(std::move(dataStore));
    } else {
        auto logMetaStore = CreateDeviceKeyBufferStore(
            Logging,
            CreateRegionDevice(
                device,
                {.FirstPageNo = 0, .PageCount = logMetaPages}),
            logMetaPages,
            pageSize,
            CalculateMinRecordBufferSize());

        auto logDataStore = CreateDevicePageStore(
            CreateRegionDevice(
                device,
                {.FirstPageNo = logMetaPages, .PageCount = logDataPages}),
            logDataPages,
            pageSize,
            EDevicePageStoreMode::Checked);

        auto journal = CreateJournal(
            Logging,
            Executor,
            std::move(logMetaStore),
            std::move(logDataStore),
            dataPages);

        Device = CreateJournalledDeviceV2(
            Logging,
            Executor,
            std::move(journal),
            std::move(dataStore),
            TString(DeviceUUID));
    }

    auto error = Device->Start().GetValueSync();
    if (HasError(error)) {
        Fail("Start", error);
    }
}

void TBench::Stop()
{
    Device->Stop().GetValueSync();
    Device.reset();

    Executor->Stop();

    if (FileIO) {
        FileIO->Stop();
    }

    Logging->Stop();
}

////////////////////////////////////////////////////////////////////////////////

void TBench::Prefill()
{
    const ui64 workingSetPages = Options.GetSlotCount() * Options.Pages;

    TVector<TPageRangeRef> journalled;

    switch (Options.Workload) {
        case EWorkload::Write:
            return;

        case EWorkload::ReadData:
            break;

        case EWorkload::ReadJournal:
            for (ui64 pageNo = 0; pageNo < workingSetPages;
                 pageNo += PrefillRequestPages)
            {
                journalled.push_back({
                    .FirstPageNo = pageNo,
                    .PageCount =
                        Min(PrefillRequestPages, workingSetPages - pageNo),
                });
            }
            break;

        case EWorkload::ReadMixed:
            //
            // Put the first half of every slot into the journal, so that
            // every request reads from both places. A slot of one page can
            // not be split: put every second slot into the journal instead
            //

            for (ui64 slot = 0; slot < Options.GetSlotCount(); ++slot) {
                if (Options.Pages == 1) {
                    if (slot % 2 == 0) {
                        journalled.push_back(
                            {.FirstPageNo = slot, .PageCount = 1});
                    }
                    continue;
                }

                journalled.push_back({
                    .FirstPageNo = slot * Options.Pages,
                    .PageCount = Options.Pages / 2,
                });
            }
            break;
    }

    Cerr << "Prefilling " << workingSetPages << " pages..." << Endl;

    //
    // Write the data area in every read workload. The pages which are in
    // the journal hide it, but the file gets real blocks this way even with
    // --sparse
    //

    PrefillData();
    PrefillJournal(journalled);
}

void TBench::PrefillData()
{
    const ui64 workingSetPages = Options.GetSlotCount() * Options.Pages;
    const TString page = MakePage(Options.PageSize, 1000);

    for (ui64 pageNo = 0; pageNo < workingSetPages;
         pageNo += PrefillRequestPages)
    {
        NProto::TWritePagesRequest request;
        request.SetDeviceUUID(TString(DeviceUUID));
        AddPageGroup(
            request,
            {.FirstPageNo = pageNo,
             .PageCount = Min(PrefillRequestPages, workingSetPages - pageNo)},
            page);

        auto response = Device->WritePages(std::move(request)).GetValueSync();
        if (HasError(response)) {
            Fail("Prefill of the data area", response.GetError());
        }
    }
}

void TBench::PrefillJournal(const TVector<TPageRangeRef>& ranges)
{
    const TString page = MakePage(Options.PageSize, 2000);

    //
    // Pack the ranges into records of about PrefillRequestPages pages and
    // at most PrefillRecordGroups groups. The watermark is never advanced,
    // so the records stay in the journal
    //

    size_t i = 0;
    while (i < ranges.size()) {
        const ui64 lsn = LastLsn.fetch_add(1) + 1;

        NProto::TWriteLogRecordRequest request;
        request.SetDeviceUUID(TString(DeviceUUID));
        request.SetLogSequenceNumber(lsn);
        request.SetPrevLogSequenceNumber(lsn - 1);

        ui64 pageCount = 0;
        while (i < ranges.size() && pageCount < PrefillRequestPages &&
               request.PageGroupsSize() < PrefillRecordGroups)
        {
            AddPageGroup(request, ranges[i], page);
            pageCount += ranges[i].PageCount;
            ++i;
        }

        auto response =
            Device->WriteLogRecord(std::move(request)).GetValueSync();
        if (HasError(response)) {
            Fail("Prefill of the journal", response.GetError());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
ui64 TBench::WaitFor(const TFuture<T>& future) const
{
    //
    // Take the time on the thread that completes the request. The time that
    // this thread needs to wake up is not a part of the request latency
    //

    ui64 doneNs = 0;
    auto done =
        future.Apply([&doneNs](const TFuture<T>&) { doneNs = NowNs(); });

    if (Options.Spin) {
        while (!done.HasValue()) {
            SpinLockPause();
        }
    } else {
        done.Wait();
    }

    return doneNs;
}

bool TBench::TakeRequest()
{
    if (DeadlineNs && NowNs() >= DeadlineNs) {
        return false;
    }

    if (Options.Requests &&
        IssuedRequests.fetch_add(1) >= Options.Requests)
    {
        return false;
    }

    return true;
}

TPageRangeRef TBench::PickSlot(TFastRng64& rng) const
{
    return {
        .FirstPageNo = rng.Uniform(Options.GetSlotCount()) * Options.Pages,
        .PageCount = Options.Pages,
    };
}

void TBench::WriteLogRecord(
    TFastRng64& rng,
    const TString& page,
    TWorkerStats& stats)
{
    const ui64 lsn = LastLsn.fetch_add(1) + 1;
    const auto slot = PickSlot(rng);

    auto makeRequest = [&]()
    {
        NProto::TWriteLogRecordRequest request;
        request.SetDeviceUUID(TString(DeviceUUID));
        request.SetLogSequenceNumber(lsn);
        request.SetPrevLogSequenceNumber(lsn - 1);
        AddPageGroup(request, slot, page);
        return request;
    };

    //
    // Build the request before the clock starts: a real request is built
    // by the network layer and not by the device
    //

    auto request = makeRequest();
    auto retryDelay = MinRetryDelay;

    const ui64 startNs = NowNs();

    for (;;) {
        auto future = Device->WriteLogRecord(std::move(request));
        const ui64 doneNs = WaitFor(future);
        const auto& error = future.GetValue().GetError();

        if (!HasError(error)) {
            stats.Main.LatenciesNs.push_back(doneNs - startNs);
            break;
        }

        //
        // Retry the record under the same lsn when the journal is full.
        // The record must not be skipped: the records after it wait for it
        //

        const ui64 retryTimeoutNs = ui64{Options.RetryTimeout} * 1'000'000'000;
        if (error.GetCode() != E_REJECTED ||
            NowNs() - startNs >= retryTimeoutNs)
        {
            Fail("WriteLogRecord", error);
        }

        ++stats.Main.Retries;

        std::this_thread::sleep_for(retryDelay);
        retryDelay = Min(retryDelay * 2, MaxRetryDelay);

        request = makeRequest();
    }

    if (Options.AdvanceEvery && lsn % Options.AdvanceEvery == 0) {
        AdvanceLsnLowWatermark(lsn, stats.Advance);
    }
}

void TBench::AdvanceLsnLowWatermark(ui64 lsn, TActionStats& stats)
{
    NProto::TAdvanceLsnLowWatermarkRequest request;
    request.SetDeviceUUID(TString(DeviceUUID));
    request.SetLsnLowWatermark(lsn);

    const ui64 startNs = NowNs();

    auto future = Device->AdvanceLsnLowWatermark(std::move(request));
    const ui64 doneNs = WaitFor(future);
    const auto& error = future.GetValue().GetError();

    if (!HasError(error)) {
        stats.LatenciesNs.push_back(doneNs - startNs);
        return;
    }

    //
    // Skip the advance when another writer is doing one right now. The
    // next advance covers this lsn too
    //

    if (error.GetCode() != E_REJECTED) {
        Fail("AdvanceLsnLowWatermark", error);
    }

    ++stats.Retries;
}

void TBench::ReadPages(TFastRng64& rng, TActionStats& stats)
{
    const auto slot = PickSlot(rng);

    NProto::TReadPagesRequest request;
    request.SetDeviceUUID(TString(DeviceUUID));

    auto& ref = *request.AddPageGroupRefs();
    ref.SetFirstPageNo(slot.FirstPageNo);
    ref.SetPageCount(slot.PageCount);
    ref.SetPageSize(Options.PageSize);

    const ui64 startNs = NowNs();

    auto future = Device->ReadPages(std::move(request));
    const ui64 doneNs = WaitFor(future);
    const auto& response = future.GetValue();

    if (HasError(response)) {
        Fail("ReadPages", response.GetError());
    }

    ui64 pageCount = 0;
    for (const auto& group: response.GetPageGroups()) {
        pageCount += group.ContentSize();
    }

    if (pageCount != slot.PageCount) {
        Fail(
            "ReadPages",
            MakeError(
                E_INVALID_STATE,
                TStringBuilder() << "got " << pageCount << " pages instead of "
                                 << slot.PageCount));
    }

    stats.LatenciesNs.push_back(doneNs - startNs);
}

void TBench::RunWorker(ui32 index, TWorkerStats& stats)
{
    TFastRng64 rng(index + 1);
    const TString page = MakePage(Options.PageSize, index);

    while (TakeRequest()) {
        if (Options.IsRead()) {
            ReadPages(rng, stats.Main);
        } else {
            WriteLogRecord(rng, page, stats);
        }
    }
}

void TBench::Run()
{
    Cerr << "Running " << Options.WorkloadName << " on device "
         << Options.DeviceVersionName << ", " << Options.BackingName
         << ": " << Options.Pages << " page(s) per request, iodepth "
         << Options.IoDepth << "..." << Endl;

    TVector<TWorkerStats> workerStats(Options.IoDepth);
    TVector<std::thread> workers;
    workers.reserve(Options.IoDepth);

    const ui64 startNs = NowNs();
    if (Options.Duration) {
        DeadlineNs = startNs + ui64{Options.Duration} * 1'000'000'000;
    }

    for (ui32 i = 0; i < Options.IoDepth; ++i) {
        workers.emplace_back(
            [this, i, &workerStats] { RunWorker(i, workerStats[i]); });
    }

    for (auto& worker: workers) {
        worker.join();
    }

    const double elapsedSeconds =
        static_cast<double>(NowNs() - startNs) / 1'000'000'000;

    TWorkerStats total;
    for (const auto& stats: workerStats) {
        total.Main.Merge(stats.Main);
        total.Advance.Merge(stats.Advance);
    }

    Report(std::move(total), elapsedSeconds);
}

void TBench::Report(TWorkerStats stats, double elapsedSeconds) const
{
    TVector<TActionSummary> summaries;

    summaries.push_back(Summarize(
        Options.IsRead() ? "ReadPages" : "WriteLogRecord",
        std::move(stats.Main),
        elapsedSeconds,
        Options.Pages * Options.PageSize));

    if (!Options.IsRead() && Options.AdvanceEvery) {
        summaries.push_back(Summarize(
            "AdvanceLsnLowWatermark",
            std::move(stats.Advance),
            elapsedSeconds,
            0));
    }

    for (const auto& summary: summaries) {
        Print(summary);
    }

    if (!Options.ResultsPath) {
        return;
    }

    NJsonWriter::TBuf json(NJsonWriter::HEM_UNSAFE);
    json.SetIndentSpaces(2);
    json.BeginObject();
    json.WriteKey("device").WriteString(Options.DeviceVersionName);
    json.WriteKey("workload").WriteString(Options.WorkloadName);
    json.WriteKey("backing").WriteString(Options.BackingName);
    json.WriteKey("page_size").WriteULongLong(Options.PageSize);
    json.WriteKey("pages").WriteULongLong(Options.Pages);
    json.WriteKey("iodepth").WriteULongLong(Options.IoDepth);
    json.WriteKey("elapsed_seconds").WriteDouble(elapsedSeconds);
    json.WriteKey("actions").BeginObject();
    for (const auto& summary: summaries) {
        WriteJson(json, summary);
    }
    json.EndObject();
    json.EndObject();

    TFileOutput out(Options.ResultsPath);
    out << json.Str() << Endl;
}

}   // namespace

}   // namespace NCloud::NJournalled

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv)
{
    using namespace NCloud::NJournalled;

    try {
        TOptions options;
        options.Parse(argc, argv);

        TBench bench(std::move(options));
        bench.Start();
        bench.Prefill();
        bench.Run();
        bench.Stop();
    } catch (...) {
        Cerr << "journalled-device-bench failed: " << CurrentExceptionMessage()
             << Endl;
        return 1;
    }

    return 0;
}
