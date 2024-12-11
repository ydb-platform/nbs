#pragma once

#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config.h>

#include <util/stream/file.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TInvalidBlock
{
    ui64 BlockIdx = 0;
    ui64 ExpectedRequestNumber = 0;
    ui64 ActualRequestNumber = 0;
};

inline IOutputStream& operator<<(
    IOutputStream& out,
    const TInvalidBlock& block)
{
    out << "{ "
        << "BlockIdx " << block.BlockIdx
        << "ExpectedRequestNumber " << block.ExpectedRequestNumber
        << "ActualRequestNumber " << block.ActualRequestNumber
        << " }";

    return out;
}

struct TRangeValidationResult
{
    ui64 GuessedStep = 0;
    ui64 GuessedLastBlockIdx = 0;
    ui64 GuessedNumberToWrite = 0;
    TVector<TInvalidBlock> InvalidBlocks;
};

TRangeValidationResult ValidateRange(
    TFile& file,
    IConfigHolderPtr configHolder,
    ui32 rangeIdx);

////////////////////////////////////////////////////////////////////////////////

TVector<TBlockData> ValidateBlocks(
    TFile file,
    ui64 blockSize,
    TVector<ui64> blockIndices);

}   // namespace NCloud::NBlockStore
