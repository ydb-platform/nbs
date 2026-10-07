#include <cloud/blockstore/libs/service/service_method.h>
#include <cloud/blockstore/libs/service/split_request_service.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/vhost-client/monotonic_buffer_resource.h>
#include <cloud/storage/core/libs/vhost-client/vhost-client.h>
#include <cloud/contrib/vhost/virtio/virtio_blk_spec.h>
#include <cloud/contrib/vhost/platform.h>
#include <util/generic/yexception.h>
#include <util/string/cast.h>
#include <util/string/builder.h>
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
using namespace NThreading;
namespace BSProto = NCloud::NBlockStore::NProto;

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/dumpable.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <google/protobuf/text_format.h>
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
    std::vector<ui64> Samples;
    std::vector<ui64> CompletionSamples;

    void Print(const char* kind, ui64 extra = 0)
    {
        std::sort(Samples.begin(), Samples.end());
        auto percentile = [&](double p) {
            return Samples.empty() ? 0. :
                Samples[std::min(Samples.size() - 1,
                    static_cast<size_t>(p * Samples.size()))] / 1000.;
        };
        std::sort(CompletionSamples.begin(), CompletionSamples.end());
        auto completionPercentile = [&](double p) {
            return CompletionSamples.empty() ? 0. : CompletionSamples[
                std::min(CompletionSamples.size() - 1,
                    static_cast<size_t>(p * CompletionSamples.size()))] / 1000.;
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
            << ",\"completion_p50_us\":" << completionPercentile(.50)
            << ",\"completion_p99_us\":" << completionPercentile(.99)
            << ",\"cpu_seconds\":" << Cpu
            << ",\"cpu_us_per_op\":" << Cpu * 1e6 / Count
            << ",\"server_cpu_seconds\":" << ServerCpu
            << ",\"server_cpu_us_per_op\":" << ServerCpu * 1e6 / Count
            << ",\"max_rss_kib\":" << u.ru_maxrss
            << ",\"detail\":" << extra
            << ",\"decision_checksum\":" << Checksum
            << "}\n";
    }
};

TMeasurements Measure(double seconds, const std::function<void()>& op,
                      const std::function<void()>& beginMeasurement = {})
{
    const auto warmUntil = Ns() + 1000000000;
    do {
        for (int i = 0; i < 64; ++i) op();
    } while (Ns() < warmUntil);
    TMeasurements result;
    result.Samples.reserve(1 << 20);
    if (beginMeasurement) beginMeasurement();
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
        Y_UNUSED(ctx);
        return MakeFuture(BSProto::TReadBlocksLocalResponse{});
    }

    TFuture<BSProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr, std::shared_ptr<BSProto::TWriteBlocksLocalRequest>) override
    {
        ++Calls;
        return MakeFuture(BSProto::TWriteBlocksLocalResponse{});
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> Execute(
        TCallContextPtr, std::shared_ptr<typename TMethod::TRequest>)
    {
        return MakeFuture<typename TMethod::TResponse>();
    }
};


struct TDumpable final: IDumpable
{
    void Dump(IOutputStream&) const override {}
    void DumpHtml(IOutputStream&) const override {}
};

void Split(bool enabled, ui32 parts, double seconds, bool write)
{
    BSProto::TDiagnosticsConfig proto;
    if (enabled) {
        TString text = "EnableLatencySli: true\n";
        for (bool write: {false, true}) {
            ui64 startBytes = 1;
            for (ui64 endBytes: {4097ull, 65537ull, 1048577ull, 8388609ull, 1ull << 40}) {
                text += TStringBuilder() << "LatencySliThresholds { "
                    << "MediaKind: STORAGE_MEDIA_SSD_NONREPLICATED Write: "
                    << (write ? "true" : "false") << " StartBytes: " << startBytes
                    << " EndBytes: " << endBytes << " ThresholdUs: 1000000000 }\n";
                startBytes = endBytes;
            }
        }
        Y_ENSURE(google::protobuf::TextFormat::ParseFromString(text, &proto));
    }
    auto config = std::make_shared<TDiagnosticsConfig>(proto);
    auto timer = CreateWallClockTimer();
    auto monitoring = CreateMonitoringServiceStub();
    auto volumes = CreateVolumeStats(monitoring, config, TDuration::Minutes(15),
                                    EVolumeStatsType::EServerStats, timer);
    auto stats = CreateServerStats(std::make_shared<TDumpable>(), config,
        monitoring, nullptr, CreateServerRequestStats(monitoring->GetCounters(),
        timer, EHistogramCounterOption::ReportMultipleCounters, {}), volumes);
    auto leaf = std::make_shared<TLeaf>(parts);
    auto service = CreateSplitRequestService(leaf);
    auto mount = std::make_shared<BSProto::TMountVolumeRequest>();
    mount->SetDiskId("benchmark");
    auto volume = service->MountVolume(CreateCallContext(), mount).GetValueSync().GetVolume();
    stats->MountVolume(volume, "client", "instance");
    std::vector<char> buffer(parts * 16 * 4096);
    TGuardedSgList sglist{TSgList{TBlockDataRef(buffer.data(), buffer.size())}};
    TLog log;
    ui64 sampleIndex = 0;
    bool measuring = false;
    std::vector<ui64> completions;
    completions.reserve(1 << 20);
    auto op = [&] {
        auto context = CreateCallContext();
        TMetricRequest metric{write ? EBlockStoreRequest::WriteBlocksLocal
                                    : EBlockStoreRequest::ReadBlocksLocal};
        stats->PrepareMetricRequest(metric, "client", "benchmark", 0, buffer.size(), false);
        stats->RequestStarted(log, metric, *context);
        const auto before = leaf->Calls;
        const bool sample = measuring && ((sampleIndex++ & 63) == 0);
        const auto start = sample ? Ns() : 0;
        auto perform = [&]<typename TRequest>() {
            auto request = std::make_shared<TRequest>();
            request->SetDiskId("benchmark");
            request->SetStartIndex(0);
            if constexpr (std::is_same_v<TRequest, BSProto::TReadBlocksLocalRequest>) {
                request->SetBlocksCount(parts * 16);
            } else {
                request->BlocksCount = parts * 16;
            }
            request->SetBlockSize(4096);
            request->Sglist = sglist;
            if constexpr (std::is_same_v<TRequest, BSProto::TReadBlocksLocalRequest>) {
                return service->ReadBlocksLocal(context, request).GetValueSync().GetError();
            } else {
                return service->WriteBlocksLocal(context, request).GetValueSync().GetError();
            }
        };
        auto error = write ? perform.template operator()<BSProto::TWriteBlocksLocalRequest>()
                           : perform.template operator()<BSProto::TReadBlocksLocalRequest>();
        stats->ResponseSent(metric, *context);
        stats->RequestCompleted(log, metric, *context, error);
        if (sample && completions.size() < (1 << 20)) {
            completions.push_back(Ns() - start);
        }
        Y_ENSURE(!HasError(error));
        Y_ENSURE(leaf->Calls - before == parts);
    };
    auto result = Measure(seconds, op, [&] { measuring = true; });
    result.CompletionSamples = std::move(completions);
    if (enabled) {
        auto group = monitoring->GetCounters()->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server_volume")->GetSubgroup("host", "cluster")
            ->GetSubgroup("volume", "benchmark")->GetSubgroup("instance", "instance")
            ->GetSubgroup("cloud", "")->GetSubgroup("folder", "")
            ->GetSubgroup("type", "ssd_nonrepl")
            ->GetSubgroup("request", write ? "WriteBlocks" : "ReadBlocks");
        // Verification is outside the measured loop; the count must cover the
        // warm-up and measured logical operations exactly once.
        result.Checksum = group->GetCounter("LatencyGoodOps", true)->Val();
        Y_ENSURE(result.Checksum == leaf->Calls / parts,
                 "latency counters did not count original operations once");
        Y_ENSURE(group->GetCounter("LatencyUnknownOps", true)->Val() == 0);
    }
    result.Print(write ? "split-write" : "split-read", parts);
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
        Y_ENSURE(argc >= 5);
        if (TString(argv[1]) == "vhost") {
            Y_ENSURE(argc == 8);
            Vhost(argv[2], FromString<ui32>(argv[3]), FromString<ui32>(argv[4]),
                FromString<double>(argv[5]), FromString<ui32>(argv[6]),
                FromString<ui32>(argv[7]));
        } else {
            Y_ENSURE(argc == 6);
            Split(FromString<ui32>(argv[2]), FromString<ui32>(argv[3]),
                FromString<double>(argv[4]), FromString<ui32>(argv[5]));
        }
        return 0;
    } catch (...) {
        std::cerr << CurrentExceptionMessage() << '\n';
        return 1;
    }
}
