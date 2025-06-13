#pragma once

#include <library/cpp/monlib/counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NStorage {

class TCountersPrinter: public NMonitoring::ICountableConsumer
{
private:
    IOutputStream* Out;
    int Level = 0;

public:
    explicit TCountersPrinter(IOutputStream* out);

    void OnCounter(
        const TString& labelName,
        const TString& labelValue,
        const NMonitoring::TCounterForPtr* counter) override;

    void OnHistogram(
        const TString& labelName,
        const TString& labelValue,
        NMonitoring::IHistogramSnapshotPtr snapshot,
        bool derivative) override;

    void OnGroupBegin(
        const TString& labelName,
        const TString& labelValue,
        const NMonitoring::TDynamicCounters* snapshot) override;

    void OnGroupEnd(
        const TString& labelName,
        const TString& labelValue,
        const NMonitoring::TDynamicCounters* snapshot) override;
};

}   // namespace NCloud::NStorage
