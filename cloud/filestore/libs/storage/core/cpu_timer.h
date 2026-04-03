#pragma once

#include <util/datetime/base.h>
#include <util/system/hp_timer.h>

#include <atomic>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TCPUUsageTimer
{
private:
    std::atomic<i64>& Metric;
    THPTimer HPTimer;
    ui64 Version = 0;
    bool Recording = false;

public:
    explicit TCPUUsageTimer(std::atomic<i64>& metric)
        : Metric(metric)
    {}

    [[nodiscard]] bool IsRecording() const
    {
        return Recording;
    }

    [[nodiscard]] ui64 GetVersion() const
    {
        return Version;
    }

    void StartRecording()
    {
        HPTimer.Reset();
        Recording = true;
    }

    void CompleteRecording(ui64 version)
    {
        if (version != Version) {
            return;
        }

        const double passed =
            HPTimer.Passed() * TDuration::Seconds(1).MicroSeconds();
        Metric.fetch_add(static_cast<i64>(passed), std::memory_order_relaxed);
        ++Version;
        Recording = false;
    }
};

class TCPUUsageTimerGuard
{
private:
    TCPUUsageTimer& Timer;
    ui64 Version;

public:
    explicit TCPUUsageTimerGuard(TCPUUsageTimer& timer)
        : Timer(timer)
    {
        if (timer.IsRecording()) {
            timer.CompleteRecording(timer.GetVersion());
        }
        Version = timer.GetVersion();
        Timer.StartRecording();
    }

    ~TCPUUsageTimerGuard()
    {
        Timer.CompleteRecording(Version);
    }
};

}   // namespace NCloud::NFileStore::NStorage
