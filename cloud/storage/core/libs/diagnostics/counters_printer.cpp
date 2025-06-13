#include "counters_printer.h"

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

namespace {

IOutputStream& Indent(IOutputStream* out, int level)
{
    for (int i = 0; i < level; i++) {
        out->Write("  ");
    }
    return *out;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCountersPrinter::TCountersPrinter(IOutputStream* out)
    : Out(out)
{}

void TCountersPrinter::OnCounter(
    const TString& labelName,
    const TString& labelValue,
    const NMonitoring::TCounterForPtr* counter)
{
    Indent(Out, Level) << labelName << ':' << labelValue << " = "
                       << counter->Val() << '\n';
}

void TCountersPrinter::OnHistogram(
    const TString& labelName,
    const TString& labelValue,
    NMonitoring::IHistogramSnapshotPtr snapshot,
    bool derivative)
{
    Y_UNUSED(derivative);
    Indent(Out, Level) << labelName << ':' << labelValue << " = " << *snapshot
                       << '\n';
}

void TCountersPrinter::OnGroupBegin(
    const TString& labelName,
    const TString& labelValue,
    const NMonitoring::TDynamicCounters* snapshot)
{
    Y_UNUSED(snapshot);
    Indent(Out, Level++) << labelName << ':' << labelValue << " {\n";
}

void TCountersPrinter::OnGroupEnd(
    const TString& labelName,
    const TString& labelValue,
    const NMonitoring::TDynamicCounters* snapshot)
{
    Y_UNUSED(labelName);
    Y_UNUSED(labelValue);
    Y_UNUSED(snapshot);
    Indent(Out, --Level) << "}\n";
}

}   // namespace NCloud::NStorage
