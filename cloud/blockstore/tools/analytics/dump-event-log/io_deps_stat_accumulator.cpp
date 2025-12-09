#include "io_deps_stat_accumulator.h"

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

constexpr TDuration WindowWidth = TDuration::Seconds(10);

void Link(
    TIoDepsStatAccumulator::TRequestInfo& lhs,
    TIoDepsStatAccumulator::TRequestInfo& rhs)
{
    if (&lhs == &rhs) {
        return;
    }

    const ui32 lhsByteCount = lhs.BlockRange.Size() * lhs.DiskInfo.BlockSize;
    const ui32 rhsByteCount = rhs.BlockRange.Size() * rhs.DiskInfo.BlockSize;

    // From left to right
    if (IsReadRequestType(lhs.RequestType)) {
        if (IsDiskRegistryMediaKind(lhs.DiskInfo.MediaKind)) {
            ++rhs.InflightInfo.HostDiskRegistryReadInflight;
            rhs.InflightInfo.HostDiskRegistryReadInflightByteCount +=
                lhsByteCount;
        } else {
            ++rhs.InflightInfo.HostBlobStorageReadInflight;
            rhs.InflightInfo.HostBlobStorageReadInflightByteCount +=
                lhsByteCount;
        }

        if (lhs.DiskInfo.DiskId == rhs.DiskInfo.DiskId) {
            ++rhs.InflightInfo.DiskReadInflight;
            rhs.InflightInfo.DiskReadInflightByteCount += lhsByteCount;
        }
    }

    if (IsWriteRequestType(lhs.RequestType)) {
        if (IsDiskRegistryMediaKind(lhs.DiskInfo.MediaKind)) {
            ++rhs.InflightInfo.HostDiskRegistryWriteInflight;
            rhs.InflightInfo.HostDiskRegistryWriteInflightByteCount +=
                lhsByteCount;
        } else {
            ++rhs.InflightInfo.HostBlobStorageWriteInflight;
            rhs.InflightInfo.HostBlobStorageWriteInflightByteCount +=
                lhsByteCount;
        }
        if (lhs.DiskInfo.DiskId == rhs.DiskInfo.DiskId) {
            ++rhs.InflightInfo.DiskWriteInflight;
            rhs.InflightInfo.DiskWriteInflightByteCount += lhsByteCount;
        }
    }

    // From right to left
    if (IsReadRequestType(rhs.RequestType)) {
        if (IsDiskRegistryMediaKind(rhs.DiskInfo.MediaKind)) {
            ++lhs.InflightInfo.HostDiskRegistryReadInflight;
            lhs.InflightInfo.HostDiskRegistryReadInflightByteCount +=
                rhsByteCount;
        } else {
            ++lhs.InflightInfo.HostBlobStorageReadInflight;
            lhs.InflightInfo.HostBlobStorageReadInflightByteCount +=
                rhsByteCount;
        }
        if (lhs.DiskInfo.DiskId == rhs.DiskInfo.DiskId) {
            ++lhs.InflightInfo.DiskReadInflight;
            lhs.InflightInfo.DiskReadInflightByteCount += rhsByteCount;
        }
    }

    if (IsWriteRequestType(rhs.RequestType)) {
        if (IsDiskRegistryMediaKind(rhs.DiskInfo.MediaKind)) {
            ++lhs.InflightInfo.HostDiskRegistryWriteInflight;
            lhs.InflightInfo.HostDiskRegistryWriteInflightByteCount +=
                rhsByteCount;
        } else {
            ++lhs.InflightInfo.HostBlobStorageWriteInflight;
            lhs.InflightInfo.HostBlobStorageWriteInflightByteCount +=
                rhsByteCount;
        }
        if (lhs.DiskInfo.DiskId == rhs.DiskInfo.DiskId) {
            ++lhs.InflightInfo.DiskWriteInflight;
            lhs.InflightInfo.DiskWriteInflightByteCount += rhsByteCount;
        }
    }
}

THashMap<TString, TDiskInfo> LoadKnownDisks(const TString& filename)
{
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
    if (IsReadRequestType(RequestType)) {
        InflightInfo.DiskReadInflight = 1;
        InflightInfo.DiskReadInflightByteCount =
            BlockRange.Size() * DiskInfo.BlockSize;
        if (IsDiskRegistryMediaKind(DiskInfo.MediaKind)) {
            InflightInfo.HostDiskRegistryReadInflight = 1;
            InflightInfo.HostDiskRegistryReadInflightByteCount =
                BlockRange.Size() * DiskInfo.BlockSize;
        } else {
            InflightInfo.HostBlobStorageReadInflight = 1;
            InflightInfo.HostBlobStorageReadInflightByteCount =
                BlockRange.Size() * DiskInfo.BlockSize;
        }
    }

    if (IsWriteRequestType(RequestType)) {
        InflightInfo.DiskWriteInflight = 1;
        InflightInfo.DiskWriteInflightByteCount =
            blockRange.Size() * DiskInfo.BlockSize;
        if (IsDiskRegistryMediaKind(DiskInfo.MediaKind)) {
            InflightInfo.HostDiskRegistryWriteInflight = 1;
            InflightInfo.HostDiskRegistryWriteInflightByteCount =
                BlockRange.Size() * DiskInfo.BlockSize;
        } else {
            InflightInfo.HostBlobStorageWriteInflight = 1;
            InflightInfo.HostBlobStorageWriteInflightByteCount =
                BlockRange.Size() * DiskInfo.BlockSize;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TIoDepsStatAccumulator::TIoDepsStatAccumulator(const TString& knownDisksFile)
    : DiskInfos(LoadKnownDisks(knownDisksFile))
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
    const TInstant finishedAt = executionStartAt + duration;
    auto inserted = Requests.insert(
        {finishedAt,
         TRequestInfo(
             GetDiskInfo(diskId),
             timestamp,
             duration,
             postponed,
             requestType,
             blockRange,
             replicaChecksums)});

    for (auto it = Requests.lower_bound(executionStartAt); it != Requests.end();
         ++it)
    {
        TRequestInfo& request = it->second;
        if (request.StartAt <= finishedAt) {
            Link(inserted->second, request);
        }
    }

    ExtractRequests(timestamp - WindowWidth);
}

const TDiskInfo& TIoDepsStatAccumulator::GetDiskInfo(
    const TString& diskId)
{
    auto it = DiskInfos.find(diskId);
    if (it == DiskInfos.end()) {
        Cerr << "Disk " << diskId.Quote() << " not found" << Endl;
        auto [it, inserted] = DiskInfos.emplace(
            diskId,
            TDiskInfo{
                .DiskId = diskId,
                .MediaKind = DefaultMediaKind,
                .BlockSize = DefaultBlockSize});
        return it->second;
    }
    return it->second;
}

void TIoDepsStatAccumulator::ExtractRequests(TInstant windowStart)
{
    using TRequestIt = decltype(Requests.begin());

    TVector<TRequestIt> outOfWindow;
    for (auto it = Requests.begin(); it != Requests.end(); ++it) {
        TRequestInfo& request = it->second;
        if (request.ExecutionStartAt() > windowStart) {
            break;
        }
        outOfWindow.push_back(it);
    }
    if (outOfWindow.empty()) {
        return;
    }

    Sort(
        outOfWindow,
        [](TRequestIt lhs, TRequestIt rhs)
        { return lhs->second.StartAt < rhs->second.StartAt; });

    for (TRequestIt it: outOfWindow) {
        const auto& request = it->second;

        for (auto& handler: EventHandlers) {
            handler->ProcessRequest(
                request.DiskInfo,
                request.StartAt,
                request.RequestType,
                request.BlockRange,
                request.Duration,
                request.Postponed,
                request.ReplicaChecksums,
                request.InflightInfo);
        }

        Requests.erase(it);
    }
}

}   // namespace NCloud::NBlockStore
