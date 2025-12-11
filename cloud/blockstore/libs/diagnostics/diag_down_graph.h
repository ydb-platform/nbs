#pragma once

#include <util/datetime/base.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// This class is used to draw a graph of the service's downtime.
// Generates green/red svg, adding up/down events to timeline.
class TSvgWithDownGraph
{
private:
    enum EState
    {
        Unknown,
        Ok,
        Fail
    };

    float MaxWidth;
    TDuration MaxDuration;

    TString SvgHeader;
    TString SvgFooter;
    TString SvgElement;

    IOutputStream& Str;
    EState PrevIsDown = EState::Unknown;
    float PrevX = 0.0;
    int ProcessedEvents = 0;
    TInstant PrevDate;

public:
    explicit TSvgWithDownGraph(IOutputStream& str);
    TSvgWithDownGraph(
        IOutputStream& str,
        const TDuration& maxDuration,
        float width);

    void AddEvent(const TInstant& time, bool isDown);

    ~TSvgWithDownGraph();

    explicit inline operator bool() const noexcept
    {
        return true;   // just to work with WITH_SCOPED
    }

private:
    static std::string_view ColorForState(EState state);
    void Init();
};

}   // namespace NCloud::NBlockStore
