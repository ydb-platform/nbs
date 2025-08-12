#include "log_title.h"

#include <cloud/storage/core/libs/common/format.h>

#include <util/datetime/cputimer.h>
#include <util/string/builder.h>
#include <util/system/datetime.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TLogTitle::TLogTitle(ui64 tabletId, TString diskId, ui64 startTime)
    : Type(EType::Volume)
    , StartTime(startTime)
    , TabletId(tabletId)
    , DiskId(std::move(diskId))
{
    Rebuild();
}

TLogTitle::TLogTitle(
        ui64 tabletId,
        TString diskId,
        ui64 startTime,
        ui32 partitionIndex,
        ui32 partitionCount)
    : Type(EType::Partition)
    , StartTime(startTime)
    , PartitionIndex(partitionIndex)
    , PartitionCount(partitionCount)
    , TabletId(tabletId)
    , DiskId(std::move(diskId))
{
    Rebuild();
}

TLogTitle::TLogTitle(
        EType type,
        TString sessionId,
        TString diskId,
        bool temporaryServer,
        ui64 startTime)
    : Type(type)
    , SessionId(std::move(sessionId))
    , StartTime(startTime)
    , TemporaryServer(temporaryServer)
    , DiskId(std::move(diskId))
{
    Rebuild();
}

TLogTitle::TLogTitle(
        ui64 tabletId,
        TString sessionId,
        TString clientId,
        TString diskId,
        bool temporaryServer,
        ui64 startTime)
    : Type(EType::Client)
    , SessionId(std::move(sessionId))
    , ClientId(std::move(clientId))
    , StartTime(startTime)
    , TemporaryServer(temporaryServer)
    , TabletId(tabletId)
    , DiskId(std::move(diskId))
{
    Rebuild();
}

TLogTitle::TLogTitle(TString diskId, bool temporaryServer, ui64 startTime)
    : Type(EType::VolumeProxy)
    , StartTime(startTime)
    , TemporaryServer(temporaryServer)
    , DiskId(std::move(diskId))
{
    Rebuild();
}

TLogTitle::TLogTitle(EType type, ui64 startTime)
    : Type(type)
    , StartTime(startTime)
{}

TLogTitle TLogTitle::CreatePartitionNonreplLog(TString diskId, ui64 startTime)
{
    auto log = TLogTitle(EType::PartitionNonrepl, startTime);
    log.DiskId = std::move(diskId);
    log.Rebuild();
    return log;
}

// static
TString TLogTitle::GetPartitionPrefix(
    ui64 tabletId,
    ui32 partitionIndex,
    ui32 partitionCount)
{
    auto builder = TStringBuilder();

    builder << "p";
    if (partitionCount > 1) {
        builder << partitionIndex;
    }
    builder << ":";
    builder << tabletId;

    return builder;
}

TChildLogTitle TLogTitle::GetChild(const ui64 startTime) const
{
    TStringBuilder childPrefix;
    childPrefix << CachedPrefix;
    const auto duration = CyclesToDurationSafe(startTime - StartTime);
    childPrefix << " t:" << FormatDuration(duration);

    return TChildLogTitle(childPrefix, startTime);
}

TChildLogTitle TLogTitle::GetChildWithTags(
    const ui64 startTime,
    std::span<const std::pair<TString, TString>> additionalTags) const
{
    TStringBuilder childPrefix;
    childPrefix << CachedPrefix;

    for (const auto& [key, value]: additionalTags) {
        childPrefix << " " << key << ":" << value;
    }

    const auto duration = CyclesToDurationSafe(startTime - StartTime);
    childPrefix << " t:" << FormatDuration(duration);

    return TChildLogTitle(childPrefix, startTime);
}

TChildLogTitle TLogTitle::GetChildWithTags(
    const ui64 startTime,
    std::initializer_list<std::pair<TString, TString>> additionalTags) const
{
    return GetChildWithTags(startTime, std::span(additionalTags));
}

TString TLogTitle::Get(EDetails details) const
{
    TStringBuilder result;
    result << CachedPrefix;

    switch (details) {
        case EDetails::Brief: {
            break;
        }
        case EDetails::WithTime: {
            const auto duration =
                CyclesToDurationSafe(GetCycleCount() - StartTime);
            result << " t:" << FormatDuration(duration);
            break;
        }
    }

    result << "]";
    return result;
}

TString TLogTitle::GetWithTime() const
{
    return Get(EDetails::WithTime);
}

TString TLogTitle::GetBrief() const
{
    return Get(EDetails::Brief);
}

void TLogTitle::SetDiskId(TString diskId)
{
    DiskId = std::move(diskId);
    Rebuild();
}

void TLogTitle::SetGeneration(ui32 generation)
{
    Generation = generation;
    Rebuild();
}

void TLogTitle::SetTabletId(ui64 tabletId)
{
    TabletId = tabletId;
    Rebuild();
}

void TLogTitle::Rebuild()
{
    switch (Type) {
        case EType::Volume: {
            RebuildForVolume();
            break;
        }
        case EType::Partition: {
            RebuildForPartition();
            break;
        }
        case EType::Session: {
            RebuildForSession();
            break;
        }
        case EType::Client: {
            RebuildForClient();
            break;
        }
        case EType::VolumeProxy: {
            RebuildForVolumeProxy();
            break;
        }
        case EType::PartitionNonrepl: {
            RebuildForPartitionNonrepl();
            break;
        }
    }
}

void TLogTitle::RebuildForVolume()
{
    auto builder = TStringBuilder();

    builder << "[";
    builder << "v:" << TabletId;
    if (Generation) {
        builder << " g:" << Generation;
    } else {
        builder << " g:?";
    }
    builder << " d:" << (DiskId.empty() ? "???" : DiskId);

    CachedPrefix = builder;
}

void TLogTitle::RebuildForPartition()
{
    auto builder = TStringBuilder();

    builder << "[";
    builder << GetPartitionPrefix(TabletId, PartitionIndex, PartitionCount);
    if (Generation) {
        builder << " g:" << Generation;
    } else {
        builder << " g:?";
    }
    builder << " d:" << (DiskId.empty() ? "???" : DiskId);

    CachedPrefix = builder;
}

////////////////////////////////////////////////////////////////////////////////

TChildLogTitle::TChildLogTitle(TString cachedPrefix, ui64 startTime)
    : CachedPrefix(std::move(cachedPrefix))
    , StartTime(startTime)
{}

TString TChildLogTitle::GetWithTime() const
{
    const auto duration = CyclesToDurationSafe(GetCycleCount() - StartTime);
    TStringBuilder builder;
    builder << CachedPrefix << " + " << FormatDuration(duration) << "]";
    return builder;
}

void TLogTitle::RebuildForSession()
{
    auto builder = TStringBuilder();

    builder << "[";
    if (TemporaryServer) {
        builder << "~";
    }
    builder << "vs:";
    if (TabletId) {
        builder << TabletId;
    } else {
        builder << "?";
    }
    builder << " d:" << DiskId;
    builder << " s:" << SessionId;

    CachedPrefix = builder;
}

void TLogTitle::RebuildForClient()
{
    auto builder = TStringBuilder();

    builder << "[";
    if (TemporaryServer) {
        builder << "~";
    }
    builder << "vc:" << TabletId;
    builder << " d:" << DiskId;
    builder << " s:" << SessionId;
    builder << " c:" << ClientId;

    builder << " pg:";
    if (Generation) {
        builder << Generation;
    } else {
        builder << "?";
    }
    CachedPrefix = builder;
}

void TLogTitle::RebuildForVolumeProxy()
{
    auto builder = TStringBuilder();

    builder << "[";
    if (TemporaryServer) {
        builder << "~";
    }
    builder << "vp:";
    if (TabletId) {
        builder << TabletId;
    } else {
        builder << "?";
    }
    builder << " d:" << DiskId;
    builder << " pg:" << Generation;

    CachedPrefix = builder;
}

void TLogTitle::RebuildForPartitionNonrepl()
{
    auto builder = TStringBuilder();

    builder << "[";
    builder << "nrd:" << DiskId;

    CachedPrefix = builder;
}

}   // namespace NCloud::NBlockStore::NStorage
