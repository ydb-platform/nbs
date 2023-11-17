#include "validator.h"

#include <util/generic/array_ref.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCheckpointValidator final
    : public ICheckpointValidator
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
        , RangeValues(Config.IoDepth())
        , Log(log)
    {
        for (ui16 i = 0; i < Config.IoDepth(); ++i) {
            RangeValues[i].reserve(Config.Ranges(i).RequestCount());
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

    while (RangeReadCount < Config.IoDepth()) {
        if (CurReadPos >= len) {
            CurReadPos -= len;
            break;
        }
        const auto& range = Config.Ranges()[RangeReadCount];
        if (RangeValues[RangeReadCount].size() == range.RequestCount()) {
            CurReadPos += (range.RequestCount() * range.RequestBlockCount() -
                Config.RangeBlockCount()) * Config.BlockSize();
            ++RangeReadCount;
            continue;
        }
        const auto* data = reinterpret_cast<const TBlockData*>(ptr + CurReadPos);
        if (Config.TestId() == data->TestId) {
            RangeValues[RangeReadCount].push_back(data->RequestNumber);
        } else {
            RangeValues[RangeReadCount].push_back(0);
        }
        CurReadPos += range.RequestBlockCount() * Config.BlockSize();
    }
}

void TCheckpointValidator::DoFinish()
{
    for (ui16 rangeIdx = 0; rangeIdx < Config.IoDepth(); ++rangeIdx) {
        const auto& range = Config.Ranges()[rangeIdx];
        const auto& values = RangeValues[rangeIdx];

        auto actual = std::max_element(values.begin(), values.end());
        ui64 expected = *actual;

        const ui64 step = range.Step();
        for (ui64 i = 0; i < range.RequestCount(); ++i) {
            if (*actual != expected) {
                STORAGE_ERROR(
                    "Wrong data in checkpoint at block index "
                    << std::distance(values.begin(), actual) *
                        range.RequestBlockCount() + range.StartOffset()
                    << ", expected: " << expected
                    << ", actual: " << *actual);
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
