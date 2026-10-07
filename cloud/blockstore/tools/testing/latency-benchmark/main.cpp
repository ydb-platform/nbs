#include <cloud/blockstore/libs/diagnostics/latency_sli.h>
#include <cloud/blockstore/libs/service/latency.h>
#include <cloud/blockstore/libs/service/service_method.h>
#include <cloud/blockstore/libs/service/split_request_service.h>
#include <cloud/blockstore/libs/storage/api/public.h>
#include <cloud/blockstore/libs/storage/volume/model/volume_throttling_policy.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/throttling/public.h>
#include <cloud/storage/core/libs/vhost-client/monotonic_buffer_resource.h>
#include <cloud/storage/core/libs/vhost-client/vhost-client.h>
#include <cloud/contrib/vhost/virtio/virtio_blk_spec.h>
#include <cloud/contrib/vhost/platform.h>
#include <util/generic/yexception.h>
#include <util/string/cast.h>
#include <sys/resource.h>
#include <algorithm>
#include <chrono>
#include <cstring>
#include <functional>
#include <iomanip>
#include <fstream>
#include <sstream>
#include <unistd.h>
#include <iostream>
#include <vector>

using namespace NCloud;
using namespace NCloud::NBlockStore;
using namespace NCloud::NBlockStore::NStorage;
using namespace NThreading;
namespace BSProto = NCloud::NBlockStore::NProto;

namespace {
ui64 Ns()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
}

double CpuSeconds()
{
    rusage u{};
    getrusage(RUSAGE_SELF, &u);
    return u.ru_utime.tv_sec + u.ru_utime.tv_usec / 1e6 +
           u.ru_stime.tv_sec + u.ru_stime.tv_usec / 1e6;
}

double ProcessCpuSeconds(ui32 pid)
{
    std::ifstream file("/proc/" + std::to_string(pid) + "/stat");
    std::string stat;
    std::getline(file, stat);
    Y_ENSURE(!stat.empty(), "cannot read server CPU counters");
    std::istringstream fields(stat.substr(stat.rfind(')') + 2));
    std::string skip;
    for (int i = 0; i < 11; ++i) fields >> skip;
    ui64 user = 0, system = 0;
    fields >> user >> system;
    return static_cast<double>(user + system) / sysconf(_SC_CLK_TCK);
}

struct TMeasurements
{
    ui64 Count = 0;
    ui64 ElapsedNs = 0;
    double Cpu = 0;
    double ServerCpu = 0;
    ui64 Checksum = 0;
    ui64 WireBytes = 0;
    ui64 QuotaDelays = 0;
    std::vector<ui64> Samples;

    void Print(const char* kind, ui64 extra = 0)
    {
        std::sort(Samples.begin(), Samples.end());
        auto percentile = [&](double p) {
            return Samples.empty() ? 0. :
                Samples[std::min(Samples.size() - 1,
                    static_cast<size_t>(p * Samples.size()))] / 1000.;
        };
        rusage u{};
        getrusage(RUSAGE_SELF, &u);
        std::cout << std::setprecision(12)
            << "{\"kind\":\"" << kind << "\",\"operations\":" << Count
            << ",\"seconds\":" << ElapsedNs / 1e9
            << ",\"ops_per_sec\":" << Count * 1e9 / ElapsedNs
            << ",\"ns_per_op\":" << static_cast<double>(ElapsedNs) / Count
            << ",\"p50_us\":" << percentile(.50)
            << ",\"p99_us\":" << percentile(.99)
            << ",\"cpu_seconds\":" << Cpu
            << ",\"cpu_us_per_op\":" << Cpu * 1e6 / Count
            << ",\"server_cpu_seconds\":" << ServerCpu
            << ",\"server_cpu_us_per_op\":" << ServerCpu * 1e6 / Count
            << ",\"max_rss_kib\":" << u.ru_maxrss
            << ",\"detail\":" << extra
            << ",\"decision_checksum\":" << Checksum
            << ",\"quota_attributed_attempts_in_prelude\":" << QuotaDelays
            << ",\"wire_bytes\":" << WireBytes << "}\n";
    }
};

TMeasurements Measure(double seconds, const std::function<void()>& op)
{
    const auto warmUntil = Ns() + 1000000000;
    do {
        for (int i = 0; i < 64; ++i) op();
    } while (Ns() < warmUntil);
    TMeasurements result;
    result.Samples.reserve(1 << 20);
    const auto cpu = CpuSeconds();
    const auto begin = Ns();
    const auto until = begin + static_cast<ui64>(seconds * 1e9);
    do {
        for (int i = 0; i < 64; ++i) {
            if ((result.Count & 63) == 0 && result.Samples.size() < (1 << 20)) {
                const auto started = Ns();
                op();
                result.Samples.push_back(Ns() - started);
            } else {
                op();
            }
            ++result.Count;
        }
    } while (Ns() < until);
    result.ElapsedNs = Ns() - begin;
    result.Cpu = CpuSeconds() - cpu;
    return result;
}

BSProto::TDiagnosticsConfig ThresholdConfig()
{
    BSProto::TDiagnosticsConfig config;
    config.SetEnableLatency(true);
    config.SetLatencyThresholdVersion(1);
    for (bool write: {false, true}) {
        auto* row = config.AddLatencyThresholds();
        row->SetMediaKind(NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
        row->SetWrite(write);
        row->SetStartBytes(0);
        row->SetEndBytes(1ull << 40);
        row->SetThresholdUs(1000000);
    }
    return config;
}

struct TLeaf final: TBlockStoreImpl<TLeaf, IBlockStore>
{
    ui64 Calls = 0;
    ui32 Parts;
    explicit TLeaf(ui32 parts): Parts(parts) {}
    void Start() override {}
    void Stop() override {}
    TStorageBuffer AllocateBuffer(size_t) override { return nullptr; }

    TFuture<BSProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr, std::shared_ptr<BSProto::TMountVolumeRequest> req) override
    {
        BSProto::TMountVolumeResponse response;
        auto* volume = response.MutableVolume();
        volume->SetDiskId(req->GetDiskId());
        volume->SetBlockSize(4096);
        volume->SetBlocksCount(16 * Parts);
        volume->SetStorageMediaKind(NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
        for (ui32 i = 0; i < Parts; ++i) {
            volume->AddDevices()->SetBlockCount(16);
        }
        return MakeFuture(std::move(response));
    }

    TFuture<BSProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr ctx,
        std::shared_ptr<BSProto::TReadBlocksLocalRequest>) override
    {
        ++Calls;
        return MakeFuture(WithLatencyLeaf(ctx, BSProto::TReadBlocksLocalResponse{}));
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr, std::shared_ptr<typename TMethod::TRequest>)
    {
        return MakeFuture<typename TMethod::TResponse>();
    }
};

void Split(bool enabled, ui32 parts, double seconds)
{
    auto leaf = std::make_shared<TLeaf>(parts);
    auto service = CreateSplitRequestService(leaf);
    auto mount = std::make_shared<BSProto::TMountVolumeRequest>();
    mount->SetDiskId("benchmark");
    service->MountVolume(CreateCallContext(), mount).GetValueSync();
    TLatencyThresholds thresholds(ThresholdConfig());
    auto registry = MakeIntrusive<NMonitoring::TDynamicCounters>();
    TLatencyCounters counters;
    counters.Register(*registry);
    std::vector<char> buffer(parts * 16 * 4096);
    TGuardedSgList sglist{TSgList{TBlockDataRef(buffer.data(), buffer.size())}};
    ui64 lastNodes = 0, wireBytes = 0;
    auto op = [&] {
        const auto started = GetCycleCount();
        auto context = CreateCallContext();
        if (enabled) context->EnableLatency();
        auto request = std::make_shared<BSProto::TReadBlocksLocalRequest>();
        request->SetDiskId("benchmark");
        request->SetStartIndex(0);
        request->SetBlocksCount(parts * 16);
        request->SetBlockSize(4096);
        request->Sglist = sglist;
        const auto before = leaf->Calls;
        auto response = service->ReadBlocksLocal(context, request).GetValueSync();
        Y_ENSURE(!response.GetError().GetCode());
        Y_ENSURE(leaf->Calls - before == parts, "split path was not exercised");
        if (enabled) {
            const auto& graph = response.GetHeaders().GetLatency();
            const auto result = EvaluateLatency(thresholds,
                NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                EBlockStoreRequest::ReadBlocksLocal, buffer.size(),
                CyclesToDurationSafe(GetCycleCount() - started), &graph, true);
            Y_ENSURE(!result.Unknown && result.Good == 1, "incomplete timing graph");
            counters.Add(result);
            if (!wireBytes) {
                lastNodes = graph.NodesSize();
                wireBytes = graph.ByteSizeLong();
            }
        }
    };
    auto result = Measure(seconds, op);
    result.WireBytes = wireBytes;
    result.Print("split-service", lastNodes);
}

void Quota(bool enabled, ui32 pressure, double seconds)
{
    BSProto::TVolumePerformanceProfile profile;
    profile.SetThrottlingEnabled(true);
    profile.SetMaxReadIops(5000);
    profile.SetMaxWriteIops(5000);
    profile.SetMaxReadBandwidth(100 * 1024 * 1024);
    profile.SetMaxWriteBandwidth(100 * 1024 * 1024);
    profile.SetBurstPercentage(10);
    profile.SetBoostPercentage(600);
    profile.SetBoostTime(1000);
    profile.SetBoostRefillTime(10000);
    profile.SetMaxPostponedWeight(1ull << 30);
    TVolumeThrottlingPolicy policy(profile,
        TThrottlerConfig(TDuration::Seconds(30), 10, 4096,
            CalculateBoostTime(profile), false));
    policy.EnableLatency(enabled);
    TBackpressureReport report;
    report.FreshIndexScore = pressure;
    policy.OnBackpressureReport(TInstant::MicroSeconds(1000000), report, 0);
    TThrottlingRequestInfo request{4096,
        static_cast<ui32>(EVolumeThrottlingOpType::Write), policy.GetVersion()};
    ui64 time = 1000000, waits = 0, attempts = 0, quotaDelays = 0;
    ui64 checksum = 1469598103934665603ull;
    auto op = [&] {
        const auto ts = TInstant::MicroSeconds(time);
        const auto delay = policy.SuggestDelay(ts, TDuration::Zero(), request);
        Y_ENSURE(delay.Defined(), "unexpected throttler rejection");
        if (attempts++ < 100000) {
            checksum ^= delay->MicroSeconds();
            checksum *= 1099511628211ull;
            if (enabled && policy.GetLatencyQuotaDelay()) ++quotaDelays;
        }
        if (*delay) {
            ++waits;
            policy.OnPostponedEvent(ts, request);
            time += delay->MicroSeconds();
        } else {
            time += 10;
        }
    };
    for (int i = 0; i < 100000; ++i) op();
    Y_ENSURE(!enabled || policy.IsLatencyQuotaKnown(), "quota observation became unknown");
    Y_ENSURE(!enabled || pressure != 1 || quotaDelays, "profile quota path was not exercised");
    auto result = Measure(seconds, op);
    Y_ENSURE(!enabled || policy.IsLatencyQuotaKnown(), "quota observation became unknown");
    result.QuotaDelays = quotaDelays;
    result.Checksum = checksum;
    result.Print("quota-policy-attempt", waits);
}

void Fifo(bool enabled, ui32 waiters, double seconds)
{
    std::vector<std::shared_ptr<TLatencyVolumeRequest>> requests;
    std::vector<std::weak_ptr<TLatencyVolumeRequest>> pending;
    for (ui32 i = 0; i < waiters; ++i) {
        requests.push_back(std::make_shared<TLatencyVolumeRequest>());
        pending.push_back(requests.back());
    }
    auto op = [&] {
        if (enabled) {
            for (const auto& weak: pending) {
                if (auto waiter = weak.lock()) waiter->Operation.Invalidate();
            }
        }
        // Match the existing actor's independent real-limiter call boundary.
        std::atomic_signal_fence(std::memory_order_seq_cst);
    };
    auto result = Measure(seconds, op);
    result.Print("unknown-fifo-invalidation", waiters);
}

void Checkpoint(bool durable, const TString& path, double seconds)
{
    TLatencyBatchTracker tracker;
    if (durable) tracker.SetCheckpointPath(path);
    TLatencyBatch batch;
    batch.Version = LatencyDiagnosticsVersion;
    batch.ThresholdVersion = 1;
    batch.Generation = 1;
    auto op = [&] {
        batch.CapturedAt = TInstant::Now();
        ++batch.Sequence;
        ++batch.Read.Good;
        const auto result = tracker.Update(batch, 1, batch.CapturedAt,
            TDuration::Seconds(30));
        Y_ENSURE(result.Status == ELatencyBatchStatus::Accepted ||
                 result.Status == ELatencyBatchStatus::Unknown);
    };
    auto result = Measure(seconds, op);
    result.Print("checkpoint-batch", durable);
}

void Vhost(const TString& socket, ui32 bytes, ui32 depth,
           double seconds, ui32 writePercent, ui32 serverPid)
{
    Y_ENSURE(init_platform_page_size() == 0, "cannot initialize vhost page size");
    NVHost::TClient client(socket, {.QueueCount = 1, .QueueSize = 256,
        .MemorySize = (bytes + 8192ull) * depth + 65536});
    Y_ENSURE(client.Init(), "vhost client initialization failed");
    NVHost::TMonotonicBufferResource memory(client.GetMemory());
    struct TSlot {
        std::span<char> Header, Data, Status;
        TFuture<ui32> Future;
        ui64 Started = 0;
    };
    std::vector<TSlot> slots(depth);
    for (auto& slot: slots) {
        slot.Header = memory.Allocate(sizeof(virtio_blk_req_hdr), 4096);
        slot.Data = memory.Allocate(bytes, 4096);
        slot.Status = memory.Allocate(1);
        Y_ENSURE(!slot.Header.empty() && !slot.Data.empty() && !slot.Status.empty());
        memset(slot.Data.data(), 0x5a, bytes);
    }
    ui64 sequence = 0;
    auto submit = [&](TSlot& slot) {
        auto* header = reinterpret_cast<virtio_blk_req_hdr*>(slot.Header.data());
        const bool write = (sequence++ % 100) < writePercent;
        *header = {.type = static_cast<ui32>(write ? VIRTIO_BLK_T_OUT : VIRTIO_BLK_T_IN),
            .sector = 0};
        slot.Status[0] = static_cast<char>(0xff);
        slot.Started = Ns();
        slot.Future = write
            ? client.WriteAsync(0, {slot.Header, slot.Data}, {slot.Status})
            : client.WriteAsync(0, {slot.Header}, {slot.Data, slot.Status});
    };
    auto phase = [&](double duration, bool measured) {
        TMeasurements result;
        if (measured) result.Samples.reserve(1 << 20);
        const auto serverCpu = ProcessCpuSeconds(serverPid);
        const auto cpu = CpuSeconds();
        const auto begin = Ns();
        const auto until = begin + static_cast<ui64>(duration * 1e9);
        for (auto& slot: slots) submit(slot);
        ui64 i = 0;
        do {
            auto& slot = slots[i++ % slots.size()];
            const auto len = slot.Future.GetValue(TDuration::Seconds(10));
            Y_ENSURE(len && slot.Status[0] == VIRTIO_BLK_S_OK,
                "vhost I/O failed");
            const auto now = Ns();
            if (measured && (result.Count & 7) == 0 &&
                result.Samples.size() < (1 << 20)) {
                result.Samples.push_back(now - slot.Started);
            }
            ++result.Count;
            submit(slot);
        } while (Ns() < until);
        for (auto& slot: slots) {
            slot.Future.GetValue(TDuration::Seconds(10));
            Y_ENSURE(slot.Status[0] == VIRTIO_BLK_S_OK);
        }
        result.ElapsedNs = Ns() - begin;
        result.Cpu = CpuSeconds() - cpu;
        result.ServerCpu = ProcessCpuSeconds(serverPid) - serverCpu;
        return result;
    };
    phase(1, false);
    auto result = phase(seconds, true);
    client.DeInit();
    result.Print("external-vhost-io", bytes);
}
} // namespace

int main(int argc, char** argv)
{
    try {
        Y_ENSURE(argc >= 5,
            "split|quota|fifo enabled parameter seconds; checkpoint durable path seconds; vhost socket bytes depth seconds write_percent server_pid");
        const TString mode = argv[1];
        if (mode == "vhost") {
            Y_ENSURE(argc == 8);
            Vhost(argv[2], FromString<ui32>(argv[3]), FromString<ui32>(argv[4]),
                FromString<double>(argv[5]), FromString<ui32>(argv[6]),
                FromString<ui32>(argv[7]));
        } else {
            const bool enabled = FromString<ui32>(argv[2]);
            const double seconds = FromString<double>(argv[4]);
            if (mode == "split") Split(enabled, FromString<ui32>(argv[3]), seconds);
            else if (mode == "quota") Quota(enabled, FromString<ui32>(argv[3]), seconds);
            else if (mode == "fifo") Fifo(enabled, FromString<ui32>(argv[3]), seconds);
            else if (mode == "checkpoint") Checkpoint(enabled, argv[3], seconds);
            else ythrow yexception() << "unknown benchmark mode";
        }
        return 0;
    } catch (...) {
        std::cerr << CurrentExceptionMessage() << '\n';
        return 1;
    }
}
