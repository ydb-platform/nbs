#pragma once

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/storage/testlib/part_client.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>

#include <util/generic/maybe.h>
#include <util/generic/variant.h>
#include <util/generic/vector.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage::NPartition::NTest {

////////////////////////////////////////////////////////////////////////////////

struct TEmpty {};

struct TBlob
{
    TBlob(
        ui32 number,
        ui8 offset,
        ui8 blockCount = 1,
        ui32 channel = 0,
        ui32 generation = 0
    )
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

////////////////////////////////////////////////////////////////////////////////

TPartitionWithRuntime SetupOverlayPartition(
    ui64 overlayTabletId,
    ui64 baseTabletId,
    const TPartitionContent& basePartitionContent,
    TMaybe<ui32> channelsCount,
    ui32 blockSize,
    ui32 blockCount,
    const NProto::TStorageServiceConfig& config,
    TMaybe<ui32> maxBlocksInBlob);

}   // namespace NCloud::NBlockStore::NStorage::NPartition::NTest
