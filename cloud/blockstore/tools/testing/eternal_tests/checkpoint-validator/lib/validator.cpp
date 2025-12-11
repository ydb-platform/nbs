#include "validator.h"

#include <util/generic/array_ref.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCheckpointValidator final: public ICheckpointValidator
{
private:
    const TTestConfig& Config;

    TVector<TVector<ui64>> RangeValues;
    bool Result = true;

    ui64 CurReadPos = 0;
    ui64 RangeReadCount = 0;

    TLog Log;

public:
    TCheckpointValidator(const TTestConfig& config, const TLog& log)
        : Config(config)
        , RangeValues(Config.GetIoDepth())
        , Log(log)
    {
        for (ui16 i = 0; i < Config.GetIoDepth(); ++i) {
            RangeValues[i].reserve(Config.GetRanges(i).GetRequestCount());
        }
    }

    void DoWrite(const void* buf, size_t len) override;
    void DoFinish() override;

    bool GetResult() const override;
};

////////////////////////////////////////////////////////////////////////////////

void TCheckpointValidator::DoWrite(const void* buf, size_t len)
{
    const auto* ptr = static_cast<const char*>(buf);

    while (RangeReadCount < Config.GetIoDepth()) {
        if (CurReadPos >= len) {
            CurReadPos -= len;
            break;
        }
        const auto& range = Config.GetRanges(RangeReadCount);
        if (RangeValues[RangeReadCount].size() == range.GetRequestCount()) {
            CurReadPos +=
                (range.GetRequestCount() * range.GetRequestBlockCount() -
                 Config.GetRangeBlockCount()) *
                Config.GetBlockSize();
            ++RangeReadCount;
            continue;
        }
        const auto* data =
            reinterpret_cast<const TBlockData*>(ptr + CurReadPos);
        if (Config.GetTestId() == data->TestId) {
            RangeValues[RangeReadCount].push_back(data->RequestNumber);
        } else {
            RangeValues[RangeReadCount].push_back(0);
        }
        CurReadPos += range.GetRequestBlockCount() * Config.GetBlockSize();
    }
}

void TCheckpointValidator::DoFinish()
{
    for (ui16 rangeIdx = 0; rangeIdx < Config.GetIoDepth(); ++rangeIdx) {
        const auto& range = Config.GetRanges(rangeIdx);
        const auto& values = RangeValues[rangeIdx];

        auto actual = std::max_element(values.begin(), values.end());
        ui64 expected = *actual;

        const ui64 step = range.GetStep();
        for (ui64 i = 0; i < range.GetRequestCount(); ++i) {
            if (*actual != expected) {
                STORAGE_ERROR(
                    "Wrong data in checkpoint at block index "
                    << std::distance(values.begin(), actual) *
                               range.GetRequestBlockCount() +
                           range.GetStartOffset()
                    << ", expected: " << expected << ", actual: " << *actual);
                Result = false;
            }

            if (expected == 0) {
                // reached the initial value
                break;
            }

            if (ui64 d = std::distance(values.begin(), actual); d < step) {
                actual = values.end() - step + d;
            } else {
                actual -= step;
            }

            expected -= 1;
        }
    }
}

bool TCheckpointValidator::GetResult() const
{
    return Result;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TValidatorPtr CreateValidator(const TTestConfig& config, const TLog& log)
{
    return std::make_shared<TCheckpointValidator>(config, log);
}

}   // namespace NCloud::NBlockStore
