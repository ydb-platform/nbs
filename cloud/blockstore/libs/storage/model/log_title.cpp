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
    , TabletId(tabletId)
    , PartitionIndex(partitionIndex)
    , PartitionCount(partitionCount)
    , DiskId(std::move(diskId))
{
    Rebuild();
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

}   // namespace NCloud::NBlockStore::NStorage
