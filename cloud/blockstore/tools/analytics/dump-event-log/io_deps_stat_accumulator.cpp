#include "io_deps_stat_accumulator.h"

#include <library/cpp/containers/stack_vector/stack_vec.h>

#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <regex>

namespace NCloud::NBlockStore {

namespace {

///////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultBlockSize = 4096;
constexpr NCloud::NProto::EStorageMediaKind DefaultMediaKind =
    NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD;

constexpr TDuration WindowWidth = TDuration::Seconds(5);

void Link(
    TIoDepsStatAccumulator::TRequestInfo& lhs,
    TIoDepsStatAccumulator::TRequestInfo& rhs)
{
    if (&lhs == &rhs) {
        return;
    }

    const bool sameDisk = lhs.DiskInfo.DiskId == rhs.DiskInfo.DiskId;

    // From left to right
    rhs.InflightData.Add(
        lhs.DiskInfo.MediaKind,
        lhs.RequestType,
        sameDisk,
        lhs.BlockRange.Size() * lhs.DiskInfo.BlockSize);

    // From right to left
    lhs.InflightData.Add(
        rhs.DiskInfo.MediaKind,
        rhs.RequestType,
        sameDisk,
        rhs.BlockRange.Size() * rhs.DiskInfo.BlockSize);
}

THashMap<TString, TDiskInfo> LoadKnownDisks(const TString& filename)
{
    if (!filename) {
        return {};
    }
    const std::regex pattern(R"(^(.*?)\t(\d+)\t(.*)$)");
    const TMap<TString, NCloud::NProto::EStorageMediaKind> kindMapping = {
        {"ssd", NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD},
        {"hdd", NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_HDD},
        {"ssd-nonreplicated",
         NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD_NONREPLICATED},
        {"ssd-mirror2",
         NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD_MIRROR2},
        {"ssd-mirror3",
         NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_SSD_MIRROR3},
    };

    auto getKind = [&](const TString& kind) -> NCloud::NProto::EStorageMediaKind
    {
        if (const auto* k = kindMapping.FindPtr(kind)) {
            return *k;
        }
        return NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT;
    };

    THashMap<TString, TDiskInfo> result;
    TFileInput input(filename);
    TString line;
    while (input.ReadLine(line)) {
        std::smatch match;
        ui32 blockSize = 0;
        if (std::regex_match(line.c_str(), match, pattern) &&
            TryFromString(match[2], blockSize))
        {
            result.emplace(
                match[1].str(),
                TDiskInfo{
                    .DiskId = match[1].str(),
                    .MediaKind = getKind(match[3].str()),
                    .BlockSize = blockSize});
        }
    }

    return result;
}

}   // namespace

TIoDepsStatAccumulator::TRequestInfo::TRequestInfo(
    const TDiskInfo& diskInfo,
    TInstant startAt,
    TDuration duration,
    TDuration postponed,
    ui32 requestType,
    TBlockRange64 blockRange,
    const TReplicaChecksums& replicaChecksums)
    : DiskInfo(diskInfo)
    , StartAt(startAt)
    , BlockRange(blockRange)
    , Duration(duration)
    , Postponed(postponed)
    , RequestType(requestType)
    , ReplicaChecksums(replicaChecksums)
{
    InflightData.Add(
        DiskInfo.MediaKind,
        RequestType,
        true,
        BlockRange.Size() * DiskInfo.BlockSize);
}

////////////////////////////////////////////////////////////////////////////////

TIoDepsStatAccumulator::TIoDepsStatAccumulator(const TString& knownDisksFile)
    : KnownDiskInfos(LoadKnownDisks(knownDisksFile))
{}

TIoDepsStatAccumulator::~TIoDepsStatAccumulator()
{
    ExtractRequests(TInstant::Max());
}

void TIoDepsStatAccumulator::AddEventHandler(
    std::unique_ptr<IProfileLogEventHandler> handler)
{
    EventHandlers.push_back(std::move(handler));
}

void TIoDepsStatAccumulator::ProcessRequest(
    const TString& diskId,
    TInstant timestamp,
    ui32 requestType,
    TBlockRange64 blockRange,
    TDuration duration,
    TDuration postponed,
    const TReplicaChecksums& replicaChecksums)
{
    const TInstant executionStartAt = timestamp + postponed;
    const TInstant finishedAt = timestamp + duration;

    auto inserted = Requests.emplace(
        finishedAt,
        TRequestInfo(
            GetDiskInfo(diskId),
            timestamp,
            duration,
            postponed,
            requestType,
            blockRange,
            replicaChecksums));

    //
    auto it = inserted;
    ++it;
    for (; it != Requests.end(); ++it) {
        TRequestInfo& request = it->second;
        if (request.ExecutionStartAt() <= finishedAt) {
            Link(inserted->second, request);
        }
    }

    for (auto it = inserted; it->first >= executionStartAt; --it) {
        Link(inserted->second, it->second);
        if (it == Requests.begin()) {
            break;
        }
    }

    ExtractRequests(timestamp - WindowWidth);
}

const TDiskInfo& TIoDepsStatAccumulator::GetDiskInfo(const TString& diskId)
{
    if (const auto* diskInfo = DiskInfos.FindPtr(diskId)) {
        return *diskInfo;
    }

    if (const auto* diskInfo = KnownDiskInfos.FindPtr(diskId)) {
        auto [it, inserted] = DiskInfos.emplace(diskId, *diskInfo);
        return it->second;
    }

    Cerr << "Disk " << diskId.Quote() << " not found" << Endl;
    auto [it, inserted] = DiskInfos.emplace(
        diskId,
        TDiskInfo{
            .DiskId = diskId,
            .MediaKind = DefaultMediaKind,
            .BlockSize = DefaultBlockSize});
    return it->second;
}

void TIoDepsStatAccumulator::ExtractRequests(TInstant windowStart)
{
    if (Requests.begin()->second.ExecutionStartAt() > windowStart) {
        return;
    }

    using TRequestIt = decltype(Requests.begin());

    TStackVec<TRequestIt> outOfWindow;
    outOfWindow.reserve(20);
    for (auto it = Requests.begin(); it != Requests.end(); ++it) {
        const TRequestInfo& request = it->second;
        if (request.ExecutionStartAt() > windowStart) {
            break;
        }
        outOfWindow.push_back(it);
    }

    Sort(
        outOfWindow,
        [](TRequestIt lhs, TRequestIt rhs)
        { return lhs->second.StartAt < rhs->second.StartAt; });

    for (TRequestIt it: outOfWindow) {
        const auto& request = it->second;
        const TTimeData timeData{
            .StartAt = request.StartAt,
            .Postponed = request.Postponed,
            .ExecutionTime = request.Duration - request.Postponed};

        for (auto& handler: EventHandlers) {
            handler->ProcessRequest(
                request.DiskInfo,
                timeData,
                request.RequestType,
                request.BlockRange,
                request.ReplicaChecksums,
                request.InflightData);
        }

        Requests.erase(it);
    }
}

}   // namespace NCloud::NBlockStore
