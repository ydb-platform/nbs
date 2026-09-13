#pragma once

#include <util/generic/string.h>
#include <util/system/types.h>

#include <memory>
#include <variant>

namespace NCloud {

namespace NProto {
class TRequestTimingTrace;
}

class TRequestTimingCollector;
struct TCallContextBase;

// Value-only completion for a request with no recorded events; otherwise owns
// the frozen collector, never the call context, backend or response future.
class TRequestTimingSnapshot
{
    friend struct TCallContextBase;

private:
    struct TOrdinary
    {
        ui64 RequestId;
        ui64 TotalMicros;
        ui32 ErrorCode;
    };

    // An eventful request owns its completion in the collector. Do not carry
    // another copy of those scalars through every queued profile record.
    std::variant<
        std::monostate,
        TOrdinary,
        std::shared_ptr<TRequestTimingCollector>>
        Data;

public:
    TRequestTimingSnapshot() = default;

    bool HasData() const
    {
        return Data.index() != 0;
    }

    // The writer stores compact observations; evaluation belongs to the reader.
    void FillTrace(NProto::TRequestTimingTrace& trace) const;

    // Explicit evaluation for diagnostic consumers and tests.
    TString Serialize() const;
};

}   // namespace NCloud
