#pragma once

#include <library/cpp/monlib/counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

class TCountersPrinter: public NMonitoring::ICountableConsumer
{
public:
    explicit TCountersPrinter(IOutputStream* out)
        : Out(out)
    {}

private:
    void OnCounter(
        const TString& labelName,
        const TString& labelValue,
        const NMonitoring::TCounterForPtr* counter) override
    {
        Indent(Out, Level) << labelName << ':' << labelValue << " = "
                           << counter->Val() << '\n';
    }

    void OnHistogram(
        const TString& labelName,
        const TString& labelValue,
        NMonitoring::IHistogramSnapshotPtr snapshot,
        bool derivative) override
    {
        Y_UNUSED(snapshot);
        Y_UNUSED(derivative);
        Indent(Out, Level) << labelName << ':' << labelValue << " = "
                           << *snapshot << '\n';
    }

    void OnGroupBegin(
        const TString& labelName,
        const TString& labelValue,
        const NMonitoring::TDynamicCounters* snapshot) override
    {
        Y_UNUSED(snapshot);
        Indent(Out, Level++) << labelName << ':' << labelValue << " {\n";
    }

    void OnGroupEnd(
        const TString& labelName,
        const TString& labelValue,
        const NMonitoring::TDynamicCounters* snapshot) override
    {
        Y_UNUSED(labelName);
        Y_UNUSED(labelValue);
        Y_UNUSED(snapshot);
        Indent(Out, --Level) << "}\n";
    }

    static IOutputStream& Indent(IOutputStream* out, int level)
    {
        for (int i = 0; i < level; i++) {
            out->Write("  ");
        }
        return *out;
    }

    IOutputStream* Out;
    int Level = 0;
};

}   // namespace NCloud::NBlockStore::NStorage
