#include "log_title.h"

#include <cloud/storage/core/libs/common/format.h>

#include <util/datetime/cputimer.h>
#include <util/generic/overloaded.h>
#include <util/string/builder.h>
#include <util/system/datetime.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TOptional
{
    const T& Value;
};

template <typename T>
TOptional(const T&) -> TOptional<T>;

template <typename T>
TStringBuilder& operator<<(TStringBuilder& stream, TOptional<T> opt)
{
    if (!opt.Value) {
        if constexpr (std::is_same_v<TString, T>) {
            stream << "???";
        } else {
            stream << "?";
        }
    } else{
        stream << opt.Value;
    }

    return stream;
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(const TLogTitle::TVolume& data)
{
    TStringBuilder stream;

    stream << "[v:" << data.TabletId;
    stream << " g:" << TOptional{data.Generation};
    stream << " d:" << TOptional{data.DiskId};

    return stream;
}

TString ToString(const TLogTitle::TPartition& data)
{
    TStringBuilder stream;

    stream << "[";
    stream << TLogTitle::GetPartitionPrefix(
        data.TabletId,
        data.PartitionIndex,
        data.PartitionCount);
    stream << " g:" << TOptional{data.Generation};
    stream << " d:" << TOptional{data.DiskId};

    return stream;
}

TString ToString(const TLogTitle::TSession& data)
{
    TStringBuilder stream;

    stream << "[";
    if (data.TemporaryServer) {
        stream << "~";
    }
    stream << "vs:" << TOptional{data.TabletId};
    stream << " d:" << data.DiskId;
    stream << " s:" << data.SessionId;

    return stream;
}

TString ToString(const TLogTitle::TClient& data)
{
    TStringBuilder stream;

    stream << "[";
    if (data.TemporaryServer) {
        stream << "~";
    }
    stream << "vc:" << data.TabletId;
    stream << " d:" << data.DiskId;
    stream << " s:" << data.SessionId;
    stream << " c:" << data.ClientId;
    stream << " pg:" << TOptional{data.Generation};

    return stream;
}

TString ToString(const TLogTitle::TVolumeProxy& data)
{
    TStringBuilder stream;

    stream << "[";
    if (data.TemporaryServer) {
        stream << "~";
    }
    stream << "vp:" << TOptional{data.TabletId};
    stream << " d:" << data.DiskId;
    stream << " pg:" << data.Generation;

    return stream;
}

TString ToString(const TLogTitle::TPartitionNonrepl& data)
{
    return TStringBuilder() << "[nrd:" << data.DiskId;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

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

    return {childPrefix, startTime};
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

    return {childPrefix, startTime};
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
    std::visit(
        [this, &diskId](auto& data)
        {
            data.DiskId = std::move(diskId);
            CachedPrefix = ToString(data);
        },
        Data);
}

void TLogTitle::SetGeneration(ui32 generation)
{
    std::visit(TOverloaded(
        [this, generation]<typename T>(T& data)
            requires(std::is_same_v<T, TVolume> ||
                     std::is_same_v<T, TVolumeProxy> ||
                     std::is_same_v<T, TPartition> ||
                     std::is_same_v<T, TClient>)
        {
            data.Generation = generation;
            CachedPrefix = ToString(data);
        },
        [](auto&) { Y_DEBUG_ABORT_UNLESS(false, "it does not make sense"); }),
        Data);
}

void TLogTitle::SetTabletId(ui64 tabletId)
{
    std::visit(TOverloaded(
        [this, tabletId]<typename T>(T& data)
            requires(std::is_same_v<T, TVolumeProxy> ||
                     std::is_same_v<T, TPartition> ||
                     std::is_same_v<T, TClient> ||
                     std::is_same_v<T, TSession>)
        {
            data.TabletId = tabletId;
            CachedPrefix = ToString(data);
        },
        [](auto&) { Y_DEBUG_ABORT_UNLESS(false, "it does not make sense"); }),
        Data);
}

void TLogTitle::Rebuild()
{
    std::visit([this](auto& data) { CachedPrefix = ToString(data); }, Data);
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

}   // namespace NCloud::NBlockStore::NStorage
