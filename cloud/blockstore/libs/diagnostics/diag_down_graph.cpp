#include "diag_down_graph.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>

#include <library/cpp/resource/resource.h>

#include <util/generic/vector.h>
#include <util/string/printf.h>

namespace {

//////////////////////////////////8//////////////////////////////////////////////

constexpr float WIDTH = 1024.0;
constexpr float HEIGHT = 50.0;
constexpr auto MAX_DURATION = TDuration::Hours(1);
constexpr std::string_view UNKNOWN_STATE_COLOR = "#777777";
constexpr std::string_view OK_STATE_COLOR = "#5cb85c";
constexpr std::string_view FAIL_STATE_COLOR = "#d9534f";

}   // namespace

////////////////////////////////////////////////////////////////////////////////

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

TSvgWithDownGraph::TSvgWithDownGraph(IOutputStream& str)
    : MaxWidth(WIDTH)
    , MaxDuration(MAX_DURATION)
    , Str(str)
{
    Init();
}

TSvgWithDownGraph::TSvgWithDownGraph(
        IOutputStream& str,
        const TDuration& maxDuration,
        float width)
    : MaxWidth(width)
    , MaxDuration(maxDuration)
    , Str(str)

{
    Init();
}

TSvgWithDownGraph::~TSvgWithDownGraph()
{
    if (SvgHeader.empty() || SvgFooter.empty() || SvgElement.empty()) {
        Str << "Svg templates not found. " << ProcessedEvents
            << " events were processed";
        return;
    }

    auto message = (PrevDate ? PrevDate.ToString() : "Start") + " - Now";
    Str << Sprintf(
        SvgElement.data(),
        message.c_str(),
        PrevX,
        0.0,
        MaxWidth - PrevX,
        HEIGHT,
        ColorForState(PrevIsDown).data());
    Str << SvgFooter.data();
}

void TSvgWithDownGraph::Init()
{
    try {
        SvgHeader = NResource::Find("diag_graph_svg_header.xml");
        SvgFooter = NResource::Find("diag_graph_svg_footer.xml");
        SvgElement = NResource::Find("diag_graph_svg_graph_element.xml");

        Str << Sprintf(SvgHeader.data(), MaxWidth, HEIGHT, MaxWidth, HEIGHT);
    } catch (const yexception& ex) {
        ReportMonitoringSvgTemplatesNotFound(ex.what());
        SvgHeader = "";
        SvgFooter = "";
        SvgElement = "";
    }
}

void TSvgWithDownGraph::AddEvent(const TInstant& time, bool isDown)
{
    auto now = TInstant::Now();
    auto start = now - MaxDuration;

    if (time < start) {
        PrevIsDown = isDown ? EState::Fail : EState::Ok;
        PrevDate = TInstant::Zero();
        return;
    }

    ProcessedEvents++;
    if (SvgHeader.empty() || SvgFooter.empty() || SvgElement.empty()) {
        return;
    }

    auto x = MaxWidth * (time - start).Seconds() / (now - start).Seconds();
    auto message =
        (PrevDate ? PrevDate.ToString() : "Start") + " - " + time.ToString();

    Str << Sprintf(
        SvgElement.data(),
        message.c_str(),
        PrevX,
        0.0f,
        x - PrevX,
        HEIGHT,
        ColorForState(PrevIsDown).data());
    PrevIsDown = isDown ? EState::Fail : EState::Ok;
    PrevDate = time;
    PrevX = x;
}

std::string_view TSvgWithDownGraph::ColorForState(EState state)
{
    switch (state) {
        case EState::Unknown:
            return UNKNOWN_STATE_COLOR;
        case EState::Ok:
            return OK_STATE_COLOR;
        case EState::Fail:
            return FAIL_STATE_COLOR;
    }
    Y_DEBUG_ABORT_UNLESS(false, "Unknown graph state");
    return UNKNOWN_STATE_COLOR;
}

}   // namespace NCloud::NBlockStore
