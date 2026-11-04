#include "tls_certificate_watcher.h"

#include "stable_read.h"

#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/task_queue.h>

#include <util/digest/city.h>
#include <util/generic/yexception.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>

#include <algorithm>

namespace NCloud {

using namespace NThreading;
using namespace NTlsUtils;

namespace {

////////////////////////////////////////////////////////////////////////////////

ui64 Fingerprint(TStringBuf content)
{
    // Dynamic counters export gauges as double. Keep only 53 bits to avoid
    // precision loss while preserving a high-quality fingerprint.
    return CityHash64(content) & ((1ULL << 53) - 1);
}

void KeepFirstError(
    NProto::TError& error,
    const TString& path,
    const NProto::TError& e)
{
    if (!HasError(error)) {
        error = MakeError(
            e.GetCode(),
            TStringBuilder() << "Failed to update " << path.Quote()
                             << ": " << e.GetMessage());
    }
}

template <typename T>
EStableReadDecision Decide(
    TStableRead<T>& stableRead,
    const T& current,
    const T& content,
    bool requireStableRead)
{
    if (requireStableRead) {
        return stableRead.Observe(current, content);
    }

    // Reads made in between periodic checks break the sequence of periodic
    // reads that the stable read relies on.
    stableRead.Reset();
    return content == current
        ? EStableReadDecision::Unchanged
        : EStableReadDecision::Apply;
}

////////////////////////////////////////////////////////////////////////////////

struct TIdentityEntry
{
    TCertificateFiles Files;
    TIdentityContent Content;
    TStableRead<TIdentityContent> StableRead;
    TVector<ICertificateWatcher::TIdentityCallback> Subscribers;
    NMonitoring::TDynamicCountersPtr Metrics;
};

struct TRootCaEntry
{
    TString Path;
    TString Content;
    TStableRead<TString> StableRead;
    TVector<ICertificateWatcher::TRootCaCallback> Subscribers;
    NMonitoring::TDynamicCountersPtr Metrics;
};

////////////////////////////////////////////////////////////////////////////////

class TCertificateWatcher final
    : public ICertificateWatcher
    , public std::enable_shared_from_this<TCertificateWatcher>
{
private:
    const ILoggingServicePtr Logging;
    const TString LogComponent;
    const ISchedulerPtr Scheduler;
    const ITaskQueuePtr TaskQueue;
    const NMonitoring::TDynamicCountersPtr Counters;
    const TDuration RefreshInterval;

    TLog Log;

    // Held while files are read; contends only with Watch* calls.
    TMutex EntriesMutex;
    TVector<TIdentityEntry> Identities;
    TVector<TRootCaEntry> RootCas;

    TMutex UpdateMutex;
    std::atomic<bool> Started = false;
    bool PeriodicScheduled = false;
    // Updates do not run concurrently and do not wait for each other.
    bool UpdateInProgress = false;
    // The pending on-demand request, or, while an update is in progress, the
    // result of that update.
    TPromise<NProto::TError> UpdatePromise;

public:
    TCertificateWatcher(
            ILoggingServicePtr logging,
            TString logComponent,
            ISchedulerPtr scheduler,
            ITaskQueuePtr taskQueue,
            NMonitoring::TDynamicCountersPtr counters,
            TDuration refreshInterval)
        : Logging(std::move(logging))
        , LogComponent(std::move(logComponent))
        , Scheduler(std::move(scheduler))
        , TaskQueue(std::move(taskQueue))
        , Counters(std::move(counters))
        , RefreshInterval(refreshInterval)
        , Log(Logging->CreateLog(LogComponent))
    {}

    ~TCertificateWatcher() override
    {
        Y_ABORT_UNLESS(Started.load() == false);
    }

    TIdentityContent WatchIdentity(
        const TCertificateFiles& files,
        TIdentityCallback onChange) override
    {
        TIdentityContent content;
        {
            TGuard<TMutex> lock(EntriesMutex);
            auto it = std::find_if(
                Identities.begin(),
                Identities.end(),
                [&](const TIdentityEntry& entry)
                { return entry.Files == files; });
            if (it == Identities.end()) {
                auto pairs = LoadCertificatePairs({files});
                Y_ENSURE(!pairs.empty(), "Empty certificate pair");
                auto& pair = pairs.front();
                Identities.push_back({
                    .Files = pair.Files,
                    .Metrics = CounterGroup(pair.Files.CertChainPath),
                });
                it = std::prev(Identities.end());

                auto validity = ValidateIdentity(pair.Content);
                if (HasError(validity.GetError())) {
                    STORAGE_WARN(
                        "Identity certificate "
                        << pair.Files.CertChainPath.Quote()
                        << " is loaded but not valid: "
                        << FormatError(validity.GetError()));
                }
                ApplyIdentity(*it, std::move(pair.Content));
            }
            it->Subscribers.push_back(std::move(onChange));
            content = it->Content;
        }
        EnsurePeriodicScheduled();
        return content;
    }

    TString WatchRootCa(const TString& path, TRootCaCallback onChange) override
    {
        TString content;
        {
            TGuard<TMutex> lock(EntriesMutex);
            auto it = std::find_if(
                RootCas.begin(),
                RootCas.end(),
                [&](const TRootCaEntry& entry) { return entry.Path == path; });
            if (it == RootCas.end()) {
                auto pair = LoadRootCaPair(path);
                Y_ENSURE(pair.RootCaPath, "Empty root certificate path");
                RootCas.push_back({
                    .Path = pair.RootCaPath,
                    .Content = std::move(pair.RootCa),
                    .Metrics = CounterGroup(pair.RootCaPath),
                });
                it = std::prev(RootCas.end());
                ReportRootCa(*it);
            }
            it->Subscribers.push_back(std::move(onChange));
            content = it->Content;
        }
        EnsurePeriodicScheduled();
        return content;
    }

    TFuture<NProto::TError> UpdateCertificates() override
    {
        TFuture<NProto::TError> future;
        {
            TGuard<TMutex> lock(UpdateMutex);
            if (!Started) {
                return MakeFuture(MakeError(
                    E_INVALID_STATE,
                    "Certificate watcher is not started"));
            }
            if (UpdatePromise.Initialized()) {
                return MakeFuture(MakeError(
                    E_TRY_AGAIN,
                    "Another certificate update is pending or in progress"));
            }
            UpdatePromise = NewPromise<NProto::TError>();
            future = UpdatePromise.GetFuture();
        }

        ScheduleUpdateAt(TInstant::Now(), /*periodic=*/false);
        return future;
    }

    void Start() override
    {
        {
            TGuard<TMutex> lock(UpdateMutex);
            if (Started.load()) {
                return;
            }
            Started.store(true);
        }
        EnsurePeriodicScheduled();
    }

    void Stop() override
    {
        TFuture<NProto::TError> update;
        {
            TGuard<TMutex> lock(UpdateMutex);
            if (!Started.load()) {
                return;
            }
            Started.store(false);
            PeriodicScheduled = false;

            if (UpdateInProgress) {
                update = UpdatePromise.GetFuture();
            } else if (UpdatePromise.Initialized()) {
                std::exchange(UpdatePromise, {}).SetValue(MakeError(
                    E_INVALID_STATE,
                    "Certificate watcher is stopped"));
            }
        }

        if (update.Initialized()) {
            update.Wait();
        }
    }

private:
    NMonitoring::TDynamicCountersPtr CounterGroup(const TString& path) const
    {
        return Counters ? Counters->GetSubgroup("path", path) : nullptr;
    }

    void EnsurePeriodicScheduled()
    {
        with_lock (EntriesMutex) {
            if (Identities.empty() && RootCas.empty()) {
                return;
            }
        }
        with_lock (UpdateMutex) {
            if (!Started || !RefreshInterval || PeriodicScheduled) {
                return;
            }
            PeriodicScheduled = true;
        }
        ScheduleUpdateAt(TInstant::Now() + RefreshInterval, /*periodic=*/true);
    }

    void ScheduleUpdateAt(TInstant deadline, bool periodic)
    {
        Scheduler->Schedule(deadline, [weak = weak_from_this(), periodic] {
            auto self = weak.lock();
            if (!self) {
                return;
            }
            self->TaskQueue->ExecuteSimple([weak = weak, periodic] {
                auto self = weak.lock();
                if (!self) {
                    return;
                }
                self->RunUpdate(periodic);
            });
        });
    }

    // Any update serves the pending on-demand request, if there is one: it
    // applies new content right away. An on-demand update that finds no
    // request has nothing to do, a periodic one that finds another update in
    // progress is skipped until the next interval.
    void RunUpdate(bool periodic)
    {
        bool run = false;
        bool requireStableRead = true;
        with_lock (UpdateMutex) {
            if (Started && !UpdateInProgress) {
                if (UpdatePromise.Initialized()) {
                    run = true;
                    requireStableRead = false;
                } else if (periodic) {
                    UpdatePromise = NewPromise<NProto::TError>();
                    run = true;
                }
                UpdateInProgress = run;
            }
        }

        if (run) {
            auto result = RefreshCertificates(requireStableRead);

            // Completed under UpdateMutex, so that Stop() does not return
            // before that.
            TGuard<TMutex> lock(UpdateMutex);
            UpdateInProgress = false;
            std::exchange(UpdatePromise, {}).SetValue(std::move(result));
        }

        if (periodic && Started) {
            ScheduleUpdateAt(TInstant::Now() + RefreshInterval, true);
        }
    }

    // Periodic checks apply new content after two of them read it unchanged
    // (see TStableRead), i.e. within two refresh intervals. On-demand updates
    // apply it right away. Content that fails to load or validate is logged
    // and the previous one is kept. Subscribers are called once the lock is
    // released.
    NProto::TError RefreshCertificates(bool requireStableRead)
    {
        NProto::TError error;
        TVector<std::function<void()>> notifications;
        {
            TGuard<TMutex> lock(EntriesMutex);
            for (auto& entry: RootCas) {
                if (RefreshRootCa(entry, requireStableRead, error)) {
                    for (const auto& subscriber: entry.Subscribers) {
                        notifications.push_back(
                            [subscriber, content = entry.Content]
                            { subscriber(content); });
                    }
                }
            }
            for (auto& entry: Identities) {
                if (RefreshIdentity(entry, requireStableRead, error)) {
                    for (const auto& subscriber: entry.Subscribers) {
                        notifications.push_back(
                            [subscriber, content = entry.Content]
                            { subscriber(content); });
                    }
                }
            }
        }

        for (const auto& notify: notifications) {
            notify();
        }
        return error;
    }

    // Returns true if the root certificate has been replaced.
    bool RefreshRootCa(
        TRootCaEntry& entry,
        bool requireStableRead,
        NProto::TError& error)
    {
        const auto& path = entry.Path;

        auto content = TryReadFile(path);
        if (HasError(content.GetError())) {
            entry.StableRead.Reset();
            STORAGE_WARN(
                "Root certificate update is skipped: "
                << FormatError(content.GetError()));
            KeepFirstError(error, path, content.GetError());
            return false;
        }

        switch (Decide(
            entry.StableRead,
            entry.Content,
            content.GetResult(),
            requireStableRead))
        {
            case EStableReadDecision::Unchanged:
                return false;
            case EStableReadDecision::Wait:
                STORAGE_INFO(
                    "New root certificate " << path.Quote()
                    << ", waiting for a stable read");
                return false;
            case EStableReadDecision::Apply:
                break;
        }

        auto validity = IsValidPemCertificate(content.GetResult());
        if (HasError(validity.GetError())) {
            STORAGE_WARN(
                "Root certificate update is skipped: "
                << FormatError(validity.GetError()));
            KeepFirstError(error, path, validity.GetError());
            return false;
        }

        entry.Content = content.ExtractResult();
        ReportRootCa(entry);
        STORAGE_INFO(
            "Root certificate " << path.Quote() << " has been updated");
        return true;
    }

    // Returns true if the certificate has been replaced.
    bool RefreshIdentity(
        TIdentityEntry& entry,
        bool requireStableRead,
        NProto::TError& error)
    {
        const auto& path = entry.Files.CertChainPath;

        auto content = ReadIdentity(entry.Files);
        if (HasError(content.GetError())) {
            entry.StableRead.Reset();
            STORAGE_WARN(
                "Identity certificate update is skipped for " << path.Quote()
                << ": " << FormatError(content.GetError()));
            KeepFirstError(error, path, content.GetError());
            return false;
        }

        switch (Decide(
            entry.StableRead,
            entry.Content,
            content.GetResult(),
            requireStableRead))
        {
            case EStableReadDecision::Unchanged:
                return false;
            case EStableReadDecision::Wait:
                STORAGE_INFO(
                    "New identity certificate " << path.Quote()
                    << ", waiting for a stable read");
                return false;
            case EStableReadDecision::Apply:
                break;
        }

        auto validity = ValidateIdentity(content.GetResult());
        if (HasError(validity.GetError())) {
            STORAGE_WARN(
                "Identity certificate update is skipped for " << path.Quote()
                << ": " << FormatError(validity.GetError()));
            KeepFirstError(error, path, validity.GetError());
            return false;
        }

        ApplyIdentity(entry, content.ExtractResult());
        return true;
    }

    void ReportRootCa(const TRootCaEntry& entry)
    {
        if (entry.Metrics) {
            *entry.Metrics->GetCounter("Fingerprint", false) =
                Fingerprint(entry.Content);
        }
    }

    void ApplyIdentity(TIdentityEntry& entry, TIdentityContent content)
    {
        const auto& path = entry.Files.CertChainPath;

        TInstant notValidAfter;
        auto notAfterTs = GetCertificateNotAfterTimestampSec(content.CertChain);
        if (HasError(notAfterTs)) {
            STORAGE_WARN(
                "Unable to parse certificate notAfter date for "
                << path.Quote() << ": " << FormatError(notAfterTs.GetError()));
        } else {
            notValidAfter = TInstant::Seconds(notAfterTs.ExtractResult());
        }

        entry.Content = std::move(content);
        if (entry.Metrics) {
            if (notValidAfter) {
                *entry.Metrics->GetCounter("ExpireTs", false) =
                    notValidAfter.Seconds();
            }
            *entry.Metrics->GetCounter("Fingerprint", false) =
                Fingerprint(entry.Content.CertChain);
        }
        STORAGE_INFO(
            "Identity certificate " << path.Quote() << " is loaded"
            << ", expires at " << notValidAfter);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ICertificateWatcherPtr CreateCertificateWatcher(
    ILoggingServicePtr logging,
    TString logComponent,
    ISchedulerPtr scheduler,
    ITaskQueuePtr taskQueue,
    NMonitoring::TDynamicCountersPtr counters,
    TDuration refreshInterval)
{
    return std::make_shared<TCertificateWatcher>(
        std::move(logging),
        std::move(logComponent),
        std::move(scheduler),
        std::move(taskQueue),
        std::move(counters),
        refreshInterval);
}

}   // namespace NCloud
