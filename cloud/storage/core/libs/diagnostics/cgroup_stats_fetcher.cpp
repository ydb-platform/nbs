#include "stats_fetcher.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/system/file.h>

namespace NCloud::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCgroupStatsFetcher final
    : public IStatsFetcher
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
                auto errorMessage = TStringBuilder()
                                    << StatsFile << " : new value " << value
                                    << " is less than previous " << Last;
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStatsFetcherPtr CreateCgroupStatsFetcher(
    TString componentName,
    ILoggingServicePtr logging,
    TString statsFile)
{
    return std::make_shared<TCgroupStatsFetcher>(
        std::move(componentName),
        std::move(logging),
        std::move(statsFile));
}

}   // namespace NCloud::NStorage
