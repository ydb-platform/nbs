#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <array>
#include <limits>
#include <optional>

namespace NCloud {

namespace NProto {
class TRequestTimingTrace;
}

// Offline evaluation of a compact trace; malformed input yields nulls/reason.
TString FormatRequestTimingTrace(const NProto::TRequestTimingTrace& trace);

// Categories match EProcessingStage: Postponed=1, Backoff=2, Shaping=4.
// Times and intervals use microseconds relative to the measured request start.
struct TTimingWait
{
    ui64 Begin = 0;
    ui64 End = 0;
    ui32 Categories = 0;
};

struct TTimingDependency
{
    ui32 Node = std::numeric_limits<ui32>::max();
    ui64 Lag = 0;
};

struct TTimingStage
{
    ui64 Begin = 0;
    ui64 End = 0;
    TVector<TTimingWait> Waits;
    TVector<TTimingDependency> Dependencies;
    ui64 NotBefore = 0;
    ui32 MissingCategories = 0;
    TString IncompleteReason;
    std::array<ui64, 3> UnlocatedWaits = {};
};

struct TRequestTimingResult
{
    TDuration TotalTime;
    std::optional<TDuration> TimeWithoutWaits;
    std::optional<TDuration> WaitImpact;
    TString IncompleteReason;
};

class TRequestTiming
{
public:
    static constexpr ui32 SupportedWaitCategories = 1u | 2u | 4u;

    static TRequestTimingResult Calculate(
        const TVector<TTimingStage>& stages,
        ui32 completionNode, ui32 selectedMask, ui64 totalMicros);
};

}   // namespace NCloud
