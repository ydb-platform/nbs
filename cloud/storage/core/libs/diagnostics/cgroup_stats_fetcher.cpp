#include "cgroup_stats_fetcher.h"

#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/cputimer.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/file.h>

namespace NCloud::NStorage {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCgroupStatsFetcher final
    : public ICgroupStatsFetcher
{
private:
    const TString ComponentName;

    const ILoggingServicePtr Logging;
    const IMonitoringServicePtr Monitoring;
    const TString StatsFile;
    const TCgroupStatsFetcherMonitoringSettings MonitoringSettings;

    TLog Log;

    TFile CpuAcctWait;

    TDuration Last;

    TIntrusivePtr<NMonitoring::TCounterForPtr> FailCounter;

public:
    TCgroupStatsFetcher(
            TString componentName,
            ILoggingServicePtr logging,
            IMonitoringServicePtr monitoring,
            TString statsFile,
            TCgroupStatsFetcherMonitoringSettings monitoringSettings)
        : ComponentName(std::move(componentName))
        , Logging(std::move(logging))
        , Monitoring(std::move(monitoring))
        , StatsFile(std::move(statsFile))
        , MonitoringSettings(std::move(monitoringSettings))
    {
    }

    void Start() override
    {
        Log = Logging->CreateLog(ComponentName);

        try {
            CpuAcctWait = TFile(
                StatsFile,
                EOpenModeFlag::OpenExisting | EOpenModeFlag::RdOnly);
        } catch (...) {
            ReportCpuWaitFatalError();
            STORAGE_ERROR(BuildErrorMessageFromException());
            return;
        }

        if (!CpuAcctWait.IsOpen()) {
            ReportCpuWaitFatalError();
            STORAGE_ERROR("Failed to open " << StatsFile);
            return;
        }

        auto ret = GetCpuWait();
        if (ret.HasError()) {
            STORAGE_ERROR("Failed to get cpu stats: " << ret.GetError());
        } else {
            Last = ret.GetResult();
        }
    }

    void Stop() override
    {
    }

    TResultOrError<TDuration> GetCpuWait() override
    {
        if (!CpuAcctWait.IsOpen()) {
            return MakeError(E_FAIL, "Failed to open " + StatsFile);
        }

        try {
            CpuAcctWait.Seek(0, SeekDir::sSet);

            constexpr i64 bufSize = 1024;

            if (CpuAcctWait.GetLength() >= bufSize - 1) {
                ReportCpuWaitFatalError();
                CpuAcctWait.Close();
                return MakeError(E_FAIL, StatsFile + " is too large");
            }

            char buf[bufSize];

            auto cnt = CpuAcctWait.Read(buf, bufSize - 1);
            if (buf[cnt - 1] == '\n') {
                --cnt;
            }
            buf[cnt] = '\0';
            auto value = TDuration::MicroSeconds(FromString<ui64>(buf) / 1000);

            if (value < Last) {
                auto errorMessage = ReportCpuWaitCounterReadError(
                    TStringBuilder() << StatsFile << " : new value " << value
                                     << " is less than previous " << Last);
                Last = value;
                return MakeError(E_FAIL, std::move(errorMessage));
            }
            auto retval = value - Last;
            Last = value;

            return retval;
        } catch (...) {
            ReportCpuWaitFatalError();
            auto errorMessage = BuildErrorMessageFromException();
            CpuAcctWait.Close();
            return MakeError(E_FAIL, std::move(errorMessage));
        }

        return MakeError(E_FAIL);
    }

    TString BuildErrorMessageFromException()
    {
        auto msg = TStringBuilder() << "IO error for " << StatsFile;
        msg << " with exception " << CurrentExceptionMessage();
        return msg;
    }

    void ReportCpuWaitFatalError()
    {
        if (FailCounter) {
            return;
        }
        if (MonitoringSettings.ComponentGroupName.empty() ||
            MonitoringSettings.CountersGroupName.empty() ||
            MonitoringSettings.CounterName.empty())
        {
            return;
        }
        FailCounter = Monitoring->GetCounters()
            ->GetSubgroup("counters", MonitoringSettings.CountersGroupName)
            ->GetSubgroup("component", MonitoringSettings.ComponentGroupName)
            ->GetCounter(MonitoringSettings.CounterName, false);
        *FailCounter = 1;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TCgroupStatsFetcherStub final
    : public ICgroupStatsFetcher
{
    void Start() override
    {
    }

    void Stop() override
    {
    }

    TResultOrError<TDuration> GetCpuWait() override
    {
        return TDuration::Zero();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ICgroupStatsFetcherPtr CreateCgroupStatsFetcher(
    TString componentName,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    TString statsFile,
    TCgroupStatsFetcherMonitoringSettings settings)
{
    return std::make_shared<TCgroupStatsFetcher>(
        std::move(componentName),
        std::move(logging),
        std::move(monitoring),
        std::move(statsFile),
        std::move(settings));
}

ICgroupStatsFetcherPtr CreateCgroupStatsFetcherStub()
{
    return std::make_shared<TCgroupStatsFetcherStub>();
}

TString BuildCpuWaitStatsFilename(const TString& serviceName)
{
    static constexpr auto CpuWaitStatsFilenameTemplate =
        "/sys/fs/cgroup/cpu/system.slice/%s.service/cpuacct.wait";
    if (!serviceName.empty()) {
        return Sprintf(CpuWaitStatsFilenameTemplate, serviceName.c_str());
    }
    return {};
}

}   // namespace NCloud::NStorage
