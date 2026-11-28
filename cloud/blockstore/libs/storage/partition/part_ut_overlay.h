#pragma once

#include "part_ut.h"

#include <cloud/blockstore/libs/storage/testlib/part_client.h>

#include <variant>

namespace NCloud::NBlockStore::NStorage::NPartition::NOverlayTest {

////////////////////////////////////////////////////////////////////////////////

struct TEmpty
{
};

struct TBlob
{
    TBlob(
        ui32 number,
        ui8 offset,
        ui8 blockCount = 1,
        ui32 channel = 0,
        ui32 generation = 0)
        : Number(number)
        , Offset(offset)
        , BlockCount(blockCount)
        , Channel(channel)
        , Generation(generation)
    {}

    ui32 Number;
    ui8 Offset;
    ui8 BlockCount;
    ui32 Channel;
    ui32 Generation;
};

struct TFresh
{
    TFresh(ui8 value)
        : Value(value)
    {}

    ui8 Value;
};

using TBlockDescription = std::variant<TEmpty, TBlob, TFresh>;

using TPartitionContent = TVector<TBlockDescription>;

////////////////////////////////////////////////////////////////////////////////

struct TPartitionWithRuntime
{
    std::unique_ptr<NActors::TTestActorRuntime> Runtime;
    std::unique_ptr<TPartitionClient> Partition;
};

TPartitionWithRuntime SetupOverlayPartition(
    ui64 overlayTabletId,
    ui64 baseTabletId,
    const TPartitionContent& basePartitionContent = {},
    TMaybe<ui32> channelsCount = {},
    ui32 blockSize = DefaultBlockSize,
    ui32 blockCount = 1024,
    const NProto::TStorageServiceConfig& config = DefaultConfig(),
    TMaybe<ui32> MaxBlocksInBlob = {});

}   // namespace NCloud::NBlockStore::NStorage::NPartition::NOverlayTest
