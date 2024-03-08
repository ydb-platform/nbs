#pragma once

#include "public.h"

#include <cloud/filestore/libs/storage/model/channel_data_kind.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TChannelsStats
{
    ui32 WritableChannelCount = 0;
    ui32 UnwritableChannelCount = 0;
    ui32 ChannelsToMoveCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TChannels
{
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TChannels();
    TChannels(TChannels&& other);
    ~TChannels();

    void AddChannel(ui32 channel, EChannelDataKind dataKind, TString poolKind);
    void RegisterUnwritableChannel(ui32 channel);
    void RegisterChannelToMove(ui32 channel);
    TMaybe<ui32> SelectChannel(EChannelDataKind dataKind);

    TVector<ui32> GetChannels(EChannelDataKind dataKind) const;
    TVector<ui32> GetUnwritableChannels() const;
    TVector<ui32> GetChannelsToMove(ui32 percentageThreshold) const;

    TChannelsStats CalculateChannelsStats() const;

    ui32 Size() const;
    bool Empty() const;

private:
    TImpl& GetImpl();
    const TImpl& GetImpl() const;
};

}   // namespace NCloud::NFileStore::NStorage
