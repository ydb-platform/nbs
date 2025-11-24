#include "stats_fetcher.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/file.h>

namespace NCloud::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TCgroupStatsFetcher final
    : public IStatsFetcher
{
private:
    const TString ComponentName;

    const TString StatsFile;

    TFile CpuAcctWait;

    TDuration Last;

public:
    TCgroupStatsFetcher(
            TString componentName,
            TString statsFile)
        : ComponentName(std::move(componentName))
        , StatsFile(std::move(statsFile))
        , Last(TDuration::Zero())
    {
    }

    TResultOrError<TDuration> GetCpuWait() override
    {
        if (!CpuAcctWait.IsOpen()) {
            try {
                CpuAcctWait = TFile(
                    StatsFile,
                    EOpenModeFlag::OpenExisting | EOpenModeFlag::RdOnly);
            } catch (...) {
                return MakeError(E_INVALID_STATE, "Failed to open " + StatsFile);
            }
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
            auto errorMessage = TStringBuilder()
                                << "IO error for " << StatsFile
                                << " with exception "
                                << CurrentExceptionMessage().Quote();
            try {
                CpuAcctWait.Close();
            } catch (...) {
                errorMessage << "\nFollowed by file close error";
            }

            return MakeError(E_FAIL, std::move(errorMessage));
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStatsFetcherPtr CreateCgroupStatsFetcher(
    TString componentName,
    TString statsFile)
{
    return std::make_shared<TCgroupStatsFetcher>(
        std::move(componentName),
        std::move(statsFile));
}

}   // namespace NCloud::NStorage
