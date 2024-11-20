#include "cgroup_stats_fetcher.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/datetime/cputimer.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>
#include <util/system/file.h>

namespace NCloud::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCgroupStatsFetcher final
    : public ICgroupStatsFetcher
{
private:
    const TString ComponentName;

    const ILoggingServicePtr Logging;
    const TString StatsFile;

    TLog Log;

    TFile CpuAcctWait;

    TDuration Last;

public:
    TCgroupStatsFetcher(
            TString componentName,
            ILoggingServicePtr logging,
            TString statsFile)
        : ComponentName(std::move(componentName))
        , Logging(std::move(logging))
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
            STORAGE_ERROR(BuildErrorMessageFromException());
            return;
        }

        if (!CpuAcctWait.IsOpen()) {
            STORAGE_ERROR("Failed to open " << StatsFile);
            return;
        }

        if (auto [cpuWait, error] = GetCpuWait(); HasError(error)) {
            STORAGE_ERROR("Failed to get CpuWait stats: " << error);
        } else {
            Last = cpuWait;
        }
    }

    void Stop() override
    {
    }

    TResultOrError<TDuration> GetCpuWait() override
    {
        if (!CpuAcctWait.IsOpen()) {
            return MakeError(E_INVALID_STATE, "Failed to open " + StatsFile);
        }

        try {
            CpuAcctWait.Seek(0, SeekDir::sSet);

            constexpr i64 bufSize = 1024;

            if (CpuAcctWait.GetLength() >= bufSize - 1) {
                CpuAcctWait.Close();
                return MakeError(E_INVALID_STATE, StatsFile + " is too large");
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
                return MakeError(E_INVALID_STATE, std::move(errorMessage));
            }
            auto retval = value - Last;
            Last = value;

            return retval;
        } catch (...) {
            auto errorMessage = BuildErrorMessageFromException();
            CpuAcctWait.Close();
            return MakeError(E_FAIL, std::move(errorMessage));
        }
    }

    TString BuildErrorMessageFromException()
    {
        auto msg = TStringBuilder() << "IO error for " << StatsFile;
        msg << " with exception " << CurrentExceptionMessage();
        return msg;
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
    TString statsFile)
{
    return std::make_shared<TCgroupStatsFetcher>(
        std::move(componentName),
        std::move(logging),
        std::move(statsFile));
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

// One can use either a service name and have the stats file inferred from it,
// or provide the stats file explicitly.
NCloud::NStorage::ICgroupStatsFetcherPtr BuildCgroupStatsFetcher(
    const TString& cpuWaitServiceName,
    const TString& cpuWaitFilename,
    const TLog& log,
    ILoggingServicePtr logging,
    TString componentName)
{
    if (cpuWaitServiceName.empty() && cpuWaitFilename.empty()) {
        const auto& Log = log;
        STORAGE_INFO(
            "CpuWaitServiceName and CpuWaitFilename are empty, can't build "
            "CgroupStatsFetcher");
        return CreateCgroupStatsFetcherStub();
    }

    TString statsFile =
        cpuWaitFilename.empty()
            ? NCloud::NStorage::BuildCpuWaitStatsFilename(cpuWaitServiceName)
            : cpuWaitFilename;

    return CreateCgroupStatsFetcher(
        std::move(componentName),
        std::move(logging),
        statsFile);
};

}   // namespace NCloud::NStorage
