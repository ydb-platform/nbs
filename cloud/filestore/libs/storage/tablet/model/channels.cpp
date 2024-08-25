#include "channels.h"

#include <cloud/storage/core/libs/tablet/model/channels.h>

#include <util/generic/deque.h>
#include <util/generic/vector.h>

#include <array>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TChannelMeta
{
    ui32 Channel = Max<ui32>();
    EChannelDataKind DataKind = EChannelDataKind::Max;
    TString PoolKind;
    bool Writable = true;
    bool ToMove = false;
    double FreeSpaceShare = 0;

    TChannelMeta() = default;

    TChannelMeta(ui32 channel, EChannelDataKind dataKind, TString poolKind)
        : Channel(channel)
        , DataKind(dataKind)
        , PoolKind(std::move(poolKind))
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TChannelRegistry
{
    TVector<TChannelMeta*> ChannelMetas;
    ui32 ChannelIndex = 0;

    const TChannelMeta* SelectChannel(
        double minFreeSpace,
        double freeSpaceThreshold)
    {
        const TChannelMeta* bestMeta = nullptr;
        double bestSpaceShare = 0;
        for (ui32 i = 0; i < ChannelMetas.size(); ++i) {
            const auto* meta = ChannelMetas[ChannelIndex % ChannelMetas.size()];
            ++ChannelIndex;
            if (!meta->Writable) {
                continue;
            }

            const bool ok = CheckChannelFreeSpaceShare(
                meta->FreeSpaceShare,
                minFreeSpace,
                freeSpaceThreshold);

            if (ok) {
                return meta;
            }

            if (meta->FreeSpaceShare > bestSpaceShare) {
                bestMeta = meta;
                bestSpaceShare = meta->FreeSpaceShare;
            }
        }

        return bestMeta;
    }

    TVector<ui32> GetChannels() const
    {
        TVector<ui32> channels(Reserve(ChannelMetas.size()));

        for (const auto* meta: ChannelMetas) {
            channels.push_back(meta->Channel);
        }

        return channels;
    }
};

using TChannelsByDataKind = std::array<
    TChannelRegistry,
    static_cast<ui32>(EChannelDataKind::Max)
>;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TChannels::TImpl
{
    TDeque<TChannelMeta> AllChannels;
    TChannelsByDataKind ByDataKind;

    void AddChannel(ui32 channel, EChannelDataKind dataKind, TString poolKind);
    void UpdateChannelStats(
        ui32 channel,
        bool writable,
        bool toMove,
        double freeSpaceShare);
    TMaybe<ui32> SelectChannel(
        EChannelDataKind dataKind,
        double minFreeSpace,
        double freeSpaceThreshold);

    TVector<ui32> GetChannels(EChannelDataKind dataKind) const;
    TVector<ui32> GetUnwritableChannels() const;
    TVector<ui32> GetChannelsToMove(ui32 percentageThreshold) const;

    TVector<NCloud::NStorage::TChannelMonInfo> MakeChannelMonInfos() const;

    TChannelsStats CalculateChannelsStats() const;

    ui32 Size() const;
    bool Empty() const;
};

////////////////////////////////////////////////////////////////////////////////

void TChannels::TImpl::AddChannel(
    ui32 channel,
    EChannelDataKind dataKind,
    TString poolKind)
{
    if (AllChannels.size() < channel + 1) {
        AllChannels.resize(channel + 1);
    }

    AllChannels[channel] = TChannelMeta(channel, dataKind, std::move(poolKind));
    auto& byDataKind = ByDataKind[static_cast<ui32>(dataKind)];
    byDataKind.ChannelMetas.push_back(&AllChannels.back());
}

void TChannels::TImpl::UpdateChannelStats(
    ui32 channel,
    bool writable,
    bool toMove,
    double freeSpaceShare)
{
    Y_ABORT_UNLESS(channel < AllChannels.size());

    AllChannels[channel].Writable = writable;
    AllChannels[channel].ToMove = toMove;
    // a value which is exactly 0 is equivalent to "no data"
    if (freeSpaceShare != 0.) {
        AllChannels[channel].FreeSpaceShare = freeSpaceShare;
    }
}

TVector<ui32> TChannels::TImpl::GetChannels(EChannelDataKind dataKind) const
{
    return ByDataKind[static_cast<ui32>(dataKind)].GetChannels();
}

TVector<ui32> TChannels::TImpl::GetUnwritableChannels() const
{
    TVector<ui32> result;

    for (const auto& meta: AllChannels) {
        if (!meta.Writable) {
            result.push_back(meta.Channel);
        }
    }

    return result;
}

TVector<ui32> TChannels::TImpl::GetChannelsToMove(
    ui32 percentageThreshold) const
{
    TVector<ui32> result;

    for (const auto& meta: AllChannels) {
        if (meta.ToMove) {
            result.push_back(meta.Channel);
        }
    }

    const ui32 absThreshold = (percentageThreshold / 100.) * AllChannels.size();

    if (result.size() < absThreshold) {
        return {};
    }

    return result;
}

TVector<NCloud::NStorage::TChannelMonInfo>
TChannels::TImpl::MakeChannelMonInfos() const
{
    TVector<NCloud::NStorage::TChannelMonInfo> result;

    for (const auto& meta: AllChannels) {
        result.push_back({
            meta.PoolKind,
            TStringBuilder() << meta.DataKind,
            meta.Writable,
            meta.Writable, // TODO: SystemWritable
            meta.FreeSpaceShare,
        });
    }

    return result;
}

TChannelsStats TChannels::TImpl::CalculateChannelsStats() const
{
    TChannelsStats stats;

    for (const auto& meta: AllChannels) {
        stats.WritableChannelCount += meta.Writable;
        stats.UnwritableChannelCount += !meta.Writable;
        stats.ChannelsToMoveCount += meta.ToMove;
    }

    return stats;
}

TMaybe<ui32> TChannels::TImpl::SelectChannel(
    EChannelDataKind dataKind,
    double minFreeSpace,
    double freeSpaceThreshold)
{
    auto& byDataKind = ByDataKind[static_cast<ui32>(dataKind)];
    const auto* meta =
        byDataKind.SelectChannel(minFreeSpace, freeSpaceThreshold);
    if (meta) {
        return meta->Channel;
    }

    return Nothing();
}

ui32 TChannels::TImpl::Size() const
{
    return AllChannels.size();
}

bool TChannels::TImpl::Empty() const
{
    return AllChannels.empty();
}

////////////////////////////////////////////////////////////////////////////////

TChannels::TChannels()
    : Impl(new TImpl())
{}

TChannels::TChannels(TChannels&& other) = default;

TChannels::~TChannels() = default;

void TChannels::AddChannel(
    ui32 channel,
    EChannelDataKind dataKind,
    TString poolKind)
{
    GetImpl().AddChannel(channel, dataKind, std::move(poolKind));
}

void TChannels::UpdateChannelStats(
    ui32 channel,
    bool writable,
    bool toMove,
    double freeSpaceShare)
{
    GetImpl().UpdateChannelStats(channel, writable, toMove, freeSpaceShare);
}

TVector<ui32> TChannels::GetChannels(EChannelDataKind dataKind) const
{
    return GetImpl().GetChannels(dataKind);
}

TVector<ui32> TChannels::GetUnwritableChannels() const
{
    return GetImpl().GetUnwritableChannels();
}

TVector<ui32> TChannels::GetChannelsToMove(ui32 percentageThreshold) const
{
    return GetImpl().GetChannelsToMove(percentageThreshold);
}

TVector<NCloud::NStorage::TChannelMonInfo> TChannels::MakeChannelMonInfos() const
{
    return GetImpl().MakeChannelMonInfos();
}

TChannelsStats TChannels::CalculateChannelsStats() const
{
    return GetImpl().CalculateChannelsStats();
}

TMaybe<ui32> TChannels::SelectChannel(
    EChannelDataKind dataKind,
    double minFreeSpace,
    double freeSpaceThreshold)
{
    return GetImpl().SelectChannel(dataKind, minFreeSpace, freeSpaceThreshold);
}

ui32 TChannels::Size() const
{
    return GetImpl().Size();
}

bool TChannels::Empty() const
{
    return GetImpl().Empty();
}

TChannels::TImpl& TChannels::GetImpl()
{
    return *Impl;
}

const TChannels::TImpl& TChannels::GetImpl() const
{
    return *Impl;
}

}   // namespace NCloud::NFileStore::NStorage
