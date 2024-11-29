#include "validate.h"

#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/str_stl.h>

#include <numeric>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 PageSize = 4096;

}   //  namespace

TBlockData ReadBlockData(TFile& file, ui64 offset)
{
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

////////////////////////////////////////////////////////////////////////////////

TVector<TBlockValidationResult> ValidateRange(
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

    Cout << "Guessing Step, NumberToWrite, LastBlockIdx from actual data"
            << Endl;

    const auto maxIt = MaxElementBy(
        actual,
        [](const auto& d) { return d.RequestNumber; });

    auto secondMaxIt = actual.begin();
    for (auto it = actual.begin() + 1; it < actual.end(); ++it) {
        if (it != maxIt &&
            it->RequestNumber > secondMaxIt->RequestNumber
        ) {
            secondMaxIt = it;
        }
    }

    ui64 step = 0;
    if (maxIt >= secondMaxIt) {
        step = maxIt - secondMaxIt;
    } else {
        step = len - (secondMaxIt - maxIt);
    }

    ui64 curNum = maxIt->RequestNumber;
    ui64 curBlockIdx = maxIt - actual.begin();

    Cout << "Step: " << step
            << " NumberToWrite: " << curNum
            << " LastBlockIdx: " << curBlockIdx << Endl;

    Y_ENSURE(step != 0, "Step should not be zero");
    Y_ENSURE(
        std::gcd(step, len) == 1,
        "Step and RequestCount should be coprime");

    TVector<ui64> expected(len);
    ui64 cnt = 0;
    while (cnt < len && curNum != 0) {
        curBlockIdx = (curBlockIdx + len - step) % len;
        expected[curBlockIdx] = --curNum;
        ++cnt;
    }

    TVector<TBlockValidationResult> results;

    for (ui64 i = 0; i < len; ++i) {
        if (expected[i] != actual[i].RequestNumber) {
            results.push_back(TBlockValidationResult {
                .BlockIdx = (i + startOffset),
                .Expected = expected[i],
                .Actual = actual[i].RequestNumber,
            });
        }
    }

    return results;
}

}   // namespace NCloud::NBlockStore
