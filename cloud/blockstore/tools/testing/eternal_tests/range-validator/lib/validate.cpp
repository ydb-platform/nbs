#include "validate.h"

#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/str_stl.h>

#include <numeric>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

TBlockData ReadBlockData(TFile& file, ui64 offset)
{
    constexpr ui64 PageSize = 4096;
    // using O_DIRECT imposes some alignment restrictions:
    //   - offset should be sector aligned
    //   - buffer should be page aligned
    //   - size should be a multiple of a page size
    size_t len = (sizeof(TBlockData) + PageSize - 1) / PageSize * PageSize;
    alignas(PageSize) char buf[len];
    file.Seek(offset, sSet);
    file.Read(buf, len);
    return *reinterpret_cast<TBlockData*>(buf);
}

}   //  namespace

////////////////////////////////////////////////////////////////////////////////

TRangeValidationResult ValidateRange(
    TFile& file,
    IConfigHolderPtr configHolder,
    ui32 rangeIdx)
{
    const auto& config = configHolder->GetConfig().GetRanges()[rangeIdx];

    ui64 len = config.GetRequestCount();
    ui64 startOffset = config.GetStartOffset();
    ui64 requestSize = config.GetRequestBlockCount() * 4_KB;

    TVector<TBlockData> actual(len);
    for (ui64 i = 0; i < len; ++i) {
        actual[i] = ReadBlockData(file, (i + startOffset) * requestSize);
    }

    auto sortedActual = actual;
    Sort(
        sortedActual,
        [](const auto& l, const auto& r) {
            return l.RequestNumber > r.RequestNumber;
        });

    {
        auto unique = UniqueBy(
            sortedActual.begin(),
            sortedActual.end(),
            [](const auto& x) { return x.RequestNumber; });
        Y_ENSURE(unique == sortedActual.end(), "All elements must be unique");
    }
    Y_ENSURE(len > 1, "RequestCount should be greater than 1");

    const auto maxRequestNumber = sortedActual[0].RequestNumber;
    const auto secondMaxRequestNumber = sortedActual[1].RequestNumber;

    const auto maxIt = FindIf(
        actual,
        [=](const auto& x) { return x.RequestNumber == maxRequestNumber; });
    const auto secondMaxIt = FindIf(
        actual,
        [=](const auto& x) { return x.RequestNumber == secondMaxRequestNumber; });

    TRangeValidationResult res;
    if (maxIt >= secondMaxIt) {
        res.GuessedStep = maxIt - secondMaxIt;
    } else {
        res.GuessedStep = len - (secondMaxIt - maxIt);
    }

    const auto step = res.GuessedStep;
    Y_ENSURE(step != 0, "GuessedStep should not be zero");
    Y_ENSURE(
        std::gcd(step, len) == 1,
        "GuessedStep and RequestCount should be coprime");

    res.GuessedLastBlockIdx = maxIt - actual.begin();
    res.GuessedNumberToWrite = maxRequestNumber;

    TVector<ui64> expectedRequestNumbers(len);
    {
        Y_ENSURE(
            res.GuessedNumberToWrite != 0,
            "GuessedNumberToWrite should not be zero");

        ui64 num = res.GuessedNumberToWrite;
        ui64 blockIdx = res.GuessedLastBlockIdx;
        ui64 cnt = 0;
        while (cnt < len && num != 0) {
            expectedRequestNumbers[blockIdx] = num;
            blockIdx = (blockIdx + len - step) % len;
            num--;
            ++cnt;
        }
    }

    for (ui64 i = 0; i < len; ++i) {
        if (expectedRequestNumbers[i] != actual[i].RequestNumber) {
            res.InvalidBlocks.push_back(TInvalidBlock {
                .BlockIdx = (i + startOffset),
                .ExpectedRequestNumber = expectedRequestNumbers[i],
                .ActualRequestNumber = actual[i].RequestNumber,
            });
        }
    }

    return res;
}

////////////////////////////////////////////////////////////////////////////////

TVector<TBlockData> ValidateBlocks(
    TFile file,
    ui64 blockSize,
    TVector<ui64> blockIndices)
{
    constexpr ui64 MaxMirrorReplicas = 3;

    TVector<TBlockData> res;

    for (ui64 blockIndex: blockIndices) {
        std::set<TBlockData> blocks;

        for (ui64 i = 0; i < MaxMirrorReplicas; i++) {
            auto blockData = ReadBlockData(file, blockIndex * blockSize);
            blocks.emplace(blockData);
        }

        if (blockIndices.size() == 1) {
            continue;
        }

        for (const auto& block: blocks) {
            res.push_back(block);
        }
    }

    return res;
}

}   // namespace NCloud::NBlockStore
