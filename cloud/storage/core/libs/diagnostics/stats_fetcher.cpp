#include "stats_fetcher.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/string/printf.h>

namespace NCloud::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TStatsFetcherStub final
    : public IStatsFetcher
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

IStatsFetcherPtr CreateStatsFetcherStub()
{
    return std::make_shared<TStatsFetcherStub>();
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

IStatsFetcherPtr BuildStatsFetcher(
    NProto::EStatsFetcherType statsFetcherType,
    TString cpuWaitFilename,
    const TLog& log,
    ILoggingServicePtr logging)
{
    switch (statsFetcherType) {
        case NCloud::NProto::CGROUP: {
            if (cpuWaitFilename.empty()) {
                const auto& Log = log;
                STORAGE_INFO(
                    "CpuWaitFilename is empty, can't build "
                    "CgroupStatsFetcher");
                return CreateStatsFetcherStub();
            }

            return CreateCgroupStatsFetcher(
                "STORAGE_STATS",
                std::move(logging),
                std::move(cpuWaitFilename));
        }
        case NCloud::NProto::TASKSTATS:
            return CreateTaskStatsFetcher(
                "STORAGE_STATS",
                std::move(logging),
                getpid());
    }
}

}   // namespace NCloud::NStorage
