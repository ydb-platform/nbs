#pragma once

#include <cloud/blockstore/tools/testing/eternal_tests/eternal-load/lib/config.h>

#include <util/stream/file.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TBlockData ReadBlockData(TFile& file, ui64 offset);

////////////////////////////////////////////////////////////////////////////////

struct TBlockValidationResult
{
    ui64 BlockIdx = 0;
    ui64 Expected = 0;
    ui64 Actual = 0;
};

TVector<TBlockValidationResult> ValidateRange(
    TFile& file,
    IConfigHolderPtr configHolder,
    ui32 rangeIdx);

}   // namespace NCloud::NBlockStore
