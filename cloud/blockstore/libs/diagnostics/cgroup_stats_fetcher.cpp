#include "cgroup_stats_fetcher.h"

#include "critical_events.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/cputimer.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/file.h>


namespace NCloud::NBlockStore {

using namespace NMonitoring;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TString ComponentName = "BLOCKSTORE_CGROUP";

////////////////////////////////////////////////////////////////////////////////

struct TCgroupStatsFetcher final
    : public ICgroupStatsFetcher
{
private:
    const ILoggingServicePtr Logging;
    const IMonitoringServicePtr Monitoring;
    const TString StatsFile;
    TLog Log;

    TFile CpuAcctWait;

    TDuration Last;

    TIntrusivePtr<NMonitoring::TCounterForPtr> FailCounter;

public:
    TCgroupStatsFetcher(
            ILoggingServicePtr logging,
            IMonitoringServicePtr monitoring,
            TString statsFile)
        : Logging(std::move(logging))
        , Monitoring(std::move(monitoring))
        , StatsFile(std::move(statsFile))
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
            STORAGE_ERROR(TStringBuilder() << "Failed to open " << StatsFile);
            return;
        }

        Last = GetCpuWait();
    }

    void Stop() override
    {
    }

    TDuration GetCpuWait() override
    {
        if (!CpuAcctWait.IsOpen()) {
            return {};
        }

        try {
            CpuAcctWait.Seek(0, SeekDir::sSet);

            constexpr i64 bufSize = 1024;

            if (CpuAcctWait.GetLength() >= bufSize - 1) {
                ReportCpuWaitFatalError();
                STORAGE_ERROR(TStringBuilder() << StatsFile << " is too large");
                CpuAcctWait.Close();
                return {};
            }

            char buf[bufSize];

            auto cnt = CpuAcctWait.Read(buf, bufSize - 1);
            if (buf[cnt - 1] == '\n') {
                --cnt;
            }
            buf[cnt] = '\0';
            auto value = TDuration::MicroSeconds(FromString<ui64>(buf) / 1000);

            if (value < Last) {
                STORAGE_ERROR(
                    ReportCpuWaitCounterReadError(
                        TStringBuilder() << StatsFile <<
                        " : new value " << value <<
                        " is less than previous " << Last));
                Last = value;
                return {};
            }
            auto retval = value - Last;
            Last = value;

            return retval;
        } catch (...) {
            ReportCpuWaitFatalError();
            STORAGE_ERROR(BuildErrorMessageFromException())
            CpuAcctWait.Close();
            return {};
        }
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
        FailCounter = Monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "server")
            ->GetCounter("CpuWaitFailure", false);
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

    TDuration GetCpuWait() override
    {
        return {};
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ICgroupStatsFetcherPtr CreateCgroupStatsFetcher(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    TString statsFile)
{
    return std::make_shared<TCgroupStatsFetcher>(
        std::move(logging),
        std::move(monitoring),
        std::move(statsFile));
}

ICgroupStatsFetcherPtr CreateCgroupStatsFetcherStub()
{
    return std::make_shared<TCgroupStatsFetcherStub>();
}

}   // namespace NCloud::NBlockStore

