#pragma once

#include "external_endpoint_stats.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/ptr.h>
#include <util/system/file.h>

#include <sys/types.h>

namespace NCloud::NBlockStore::NServer::NPrivate {

struct TChild
{
    pid_t Pid = 0;
    TFileHandle StdOut;
    TFileHandle StdErr;

public:
    explicit TChild(pid_t pid)
        : Pid{pid}
    {}

    TChild(pid_t pid, TFileHandle stdOut, TFileHandle stdErr)
        : Pid{pid}
        , StdOut{std::move(stdOut)}
        , StdErr{std::move(stdErr)}
    {}

    TChild(const TChild&) = delete;
    TChild& operator=(const TChild&) = delete;

    TChild(TChild&& rhs) noexcept
    {
        Swap(rhs);
    }

    TChild& operator=(TChild&& rhs) noexcept
    {
        Swap(rhs);

        return *this;
    }

    void Swap(TChild& rhs) noexcept
    {
        std::swap(Pid, rhs.Pid);
        StdOut.Swap(rhs.StdOut);
        StdErr.Swap(rhs.StdErr);
    }

    int SendSignal(int sig) const noexcept;
    void Terminate() const noexcept;
    void Kill() const noexcept;
    NProto::TError Wait() const;
};

TChild SpawnChild(
    const TString& binaryPath,
    TVector<TString> args,
    const TString& latencySli);

struct IEndpointProcess: TThrRefBase
{
    virtual void Stop() = 0;
    virtual NProto::TError Wait() = 0;
};

enum class EEndpointStartMode
{
    Initial,
    Restart,
};

TIntrusivePtr<IEndpointProcess> CreateEndpointProcess(
    TExecutor& executor,
    TString clientId,
    ILoggingServicePtr logging,
    TEndpointStats stats,
    TChild child,
    EEndpointStartMode mode = EEndpointStartMode::Initial);

}   // namespace NCloud::NBlockStore::NServer::NPrivate
