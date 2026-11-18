#include "tls_certificate_watcher.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/digest/city.h>
#include <util/folder/tempdir.h>
#include <util/generic/deque.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/fs.h>
#include <util/system/mutex.h>

#include <memory>

namespace NCloud {

using namespace NTlsUtils;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString ReadCertResource(TStringBuf relativePath)
{
    return NResource::Find(
        TStringBuilder() << "grpc/ut/certs/" << relativePath);
}

ui64 Fingerprint(TStringBuf content)
{
    return CityHash64(content) & ((1ULL << 53) - 1);
}

void WriteTextFile(const TString& path, const TString& content)
{
    TFileOutput out(path);
    out.Write(content.data(), content.size());
}

TCertificateFiles CreateCertificatePair(
    const TString& dirPath,
    const TString& prefix,
    const TString& privateKeyContent,
    const TString& certChainContent)
{
    const TString privateKeyPath = TStringBuilder()
        << dirPath << "/" << prefix << ".key";
    const TString certChainPath = TStringBuilder()
        << dirPath << "/" << prefix << ".crt";

    WriteTextFile(privateKeyPath, privateKeyContent);
    WriteTextFile(certChainPath, certChainContent);

    return {
        .PrivateKeyPath = privateKeyPath,
        .CertChainPath = certChainPath,
    };
}

// Retries while the watcher is busy with another update, e.g. a periodic one
// run by a real scheduler.
NProto::TError UpdateCertificatesSync(const ICertificateWatcherPtr& watcher)
{
    while (true) {
        auto error = watcher->UpdateCertificates().GetValueSync();
        if (error.GetCode() != E_TRY_AGAIN) {
            return error;
        }
        Sleep(TDuration::MilliSeconds(1));
    }
}

////////////////////////////////////////////////////////////////////////////////

class TManualScheduler final
    : public IScheduler
{
private:
    struct TScheduled
    {
        TDuration Delay;
        TCallback Callback;
    };

    TMutex Lock;
    TDeque<TScheduled> Pending;

public:
    void Start() override
    {}

    void Stop() override
    {}

    void Schedule(ITaskQueue*, TInstant deadline, TCallback callback) override
    {
        TGuard guard(Lock);
        Pending.push_back({
            .Delay = deadline - TInstant::Now(),
            .Callback = std::move(callback),
        });
    }

    size_t PendingCount()
    {
        TGuard guard(Lock);
        return Pending.size();
    }

    TVector<TDuration> PendingDelays()
    {
        TGuard guard(Lock);
        TVector<TDuration> delays;
        for (const auto& scheduled: Pending) {
            delays.push_back(scheduled.Delay);
        }
        return delays;
    }

    void RunPending()
    {
        RunPending([](const TDuration&) { return true; });
    }

    void RunNext()
    {
        TScheduled scheduled;
        {
            TGuard guard(Lock);
            UNIT_ASSERT(!Pending.empty());
            scheduled = std::move(Pending.front());
            Pending.pop_front();
        }
        scheduled.Callback();
    }

    void RunPendingWithin(TDuration maxDelay)
    {
        RunPending([=](const TDuration& delay) { return delay <= maxDelay; });
    }

private:
    template <typename TPredicate>
    void RunPending(TPredicate shouldRun)
    {
        TDeque<TScheduled> batch;
        {
            TGuard guard(Lock);
            TDeque<TScheduled> rest;
            for (auto& scheduled: Pending) {
                if (shouldRun(scheduled.Delay)) {
                    batch.push_back(std::move(scheduled));
                } else {
                    rest.push_back(std::move(scheduled));
                }
            }
            Pending.swap(rest);
        }
        for (auto& scheduled: batch) {
            scheduled.Callback();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// A watcher over a root CA and two identity pairs, driven by hand.
struct TWatcherContext
{
    TTempDir TempDir;

    TString RootPath;
    TCertificateFiles ServerPair;
    TCertificateFiles ClientPair;

    std::shared_ptr<TManualScheduler> Scheduler;
    NMonitoring::TDynamicCountersPtr Counters;
    ICertificateWatcherPtr Watcher;

    TVector<TIdentityContent> ServerUpdates;
    TVector<TIdentityContent> ClientUpdates;
    TVector<TString> RootUpdates;

    explicit TWatcherContext(
            TDuration refreshInterval = TDuration::Seconds(1),
            bool watchFiles = true)
        : RootPath(TStringBuilder() << TempDir.Name() << "/ca.crt")
        , ServerPair(CreateCertificatePair(
              TempDir.Name(),
              "server",
              ReadCertResource("server1.key"),
              ReadCertResource("server1.crt")))
        , ClientPair(CreateCertificatePair(
              TempDir.Name(),
              "client",
              ReadCertResource("server2.key"),
              ReadCertResource("server2.crt")))
        , Scheduler(std::make_shared<TManualScheduler>())
        , Counters(MakeIntrusive<NMonitoring::TDynamicCounters>())
    {
        WriteTextFile(RootPath, ReadCertResource("ca.crt"));

        Watcher = CreateCertificateWatcher(
            CreateLoggingService("console"),
            "TLS_CERTIFICATE_PROVIDER",
            Scheduler,
            CreateTaskQueueStub(),
            Counters,
            refreshInterval);

        if (watchFiles) {
            WatchAll();
        }
    }

    void WatchAll()
    {
        Watcher->WatchRootCa(
            RootPath,
            [this](const TString& rootCa) { RootUpdates.push_back(rootCa); });
        Watcher->WatchIdentity(
            ServerPair,
            [this](const TIdentityContent& content)
            { ServerUpdates.push_back(content); });
        Watcher->WatchIdentity(
            ClientPair,
            [this](const TIdentityContent& content)
            { ClientUpdates.push_back(content); });
    }

    // New content is applied by a periodic check only after it has been read
    // unchanged by two periodic checks in a row.
    void RunUntilStable() const
    {
        Scheduler->RunPending();
        Scheduler->RunPending();
    }

    void RotateServer(const TString& key, const TString& cert) const
    {
        WriteTextFile(ServerPair.PrivateKeyPath, ReadCertResource(key));
        WriteTextFile(ServerPair.CertChainPath, ReadCertResource(cert));
    }

    NMonitoring::TDynamicCountersPtr PathGroup(const TString& path) const
    {
        return Counters->FindSubgroup("path", path);
    }

    ui64 GetExpireTs(const TString& certPath) const
    {
        return Counters
            ->GetSubgroup("path", certPath)
            ->GetCounter("ExpireTs", false)
            ->Val();
    }

    ui64 GetFingerprint(const TString& path) const
    {
        return Counters
            ->GetSubgroup("path", path)
            ->GetCounter("Fingerprint", false)
            ->Val();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TTlsCertificateWatcherTest)
{
    Y_UNIT_TEST(ShouldLoadFilesOnWatch)
    {
        TWatcherContext context(TDuration::Seconds(1), /*watchFiles=*/false);

        const auto rootCa = context.Watcher->WatchRootCa(
            context.RootPath,
            [](const TString&) {});
        UNIT_ASSERT_VALUES_EQUAL(ReadCertResource("ca.crt"), rootCa);

        const auto server = context.Watcher->WatchIdentity(
            context.ServerPair,
            [](const TIdentityContent&) {});
        UNIT_ASSERT_VALUES_EQUAL(
            ReadCertResource("server1.key"),
            server.PrivateKey);
        UNIT_ASSERT_VALUES_EQUAL(
            ReadCertResource("server1.crt"),
            server.CertChain);
    }

    Y_UNIT_TEST(ShouldReportMetricsUnderFullPath)
    {
        TWatcherContext context;

        UNIT_ASSERT(context.GetExpireTs(context.ServerPair.CertChainPath) > 0);
        UNIT_ASSERT(context.GetExpireTs(context.ClientPair.CertChainPath) > 0);
        UNIT_ASSERT_VALUES_EQUAL(
            Fingerprint(ReadCertResource("server1.crt")),
            context.GetFingerprint(context.ServerPair.CertChainPath));
        UNIT_ASSERT_VALUES_EQUAL(
            Fingerprint(ReadCertResource("ca.crt")),
            context.GetFingerprint(context.RootPath));

        // The root CA has no expiration sensor.
        UNIT_ASSERT(!context.PathGroup(context.RootPath)
            ->FindCounter("ExpireTs"));
        // The private key is not a label.
        UNIT_ASSERT(!context.PathGroup(context.ServerPair.PrivateKeyPath));
    }

    Y_UNIT_TEST(ShouldShareEntryBetweenSubscribers)
    {
        TWatcherContext context;

        // The files are not read again: the second watch succeeds without
        // them.
        NFs::Remove(context.ServerPair.PrivateKeyPath);
        NFs::Remove(context.ServerPair.CertChainPath);
        const auto content = context.Watcher->WatchIdentity(
            context.ServerPair,
            [](const TIdentityContent&) {});
        UNIT_ASSERT_VALUES_EQUAL(
            ReadCertResource("server1.crt"),
            content.CertChain);

        NFs::Remove(context.RootPath);
        const auto rootCa = context.Watcher->WatchRootCa(
            context.RootPath,
            [](const TString&) {});
        UNIT_ASSERT_VALUES_EQUAL(ReadCertResource("ca.crt"), rootCa);
    }

    Y_UNIT_TEST(ShouldThrowOnWatchWhenFilesAreInvalid)
    {
        TWatcherContext context(TDuration::Seconds(1), /*watchFiles=*/false);

        WriteTextFile(context.ServerPair.CertChainPath, "broken certificate");
        UNIT_ASSERT_EXCEPTION(
            context.Watcher->WatchIdentity(
                context.ServerPair,
                [](const TIdentityContent&) {}),
            yexception);

        UNIT_ASSERT_EXCEPTION(
            context.Watcher->WatchIdentity(
                TCertificateFiles{
                    .PrivateKeyPath = "/nonexistent/server.key",
                    .CertChainPath = "/nonexistent/server.crt",
                },
                [](const TIdentityContent&) {}),
            yexception);

        WriteTextFile(context.RootPath, "broken root");
        UNIT_ASSERT_EXCEPTION(
            context.Watcher->WatchRootCa(
                context.RootPath,
                [](const TString&) {}),
            yexception);
        UNIT_ASSERT_EXCEPTION(
            context.Watcher->WatchRootCa(
                "/nonexistent/ca.crt",
                [](const TString&) {}),
            yexception);
    }

    Y_UNIT_TEST(ShouldWatchInvalidInitialChain)
    {
        TWatcherContext context(TDuration::Seconds(1), /*watchFiles=*/false);

        // The chain cannot be built. It is served as is and only reported,
        // so that the service is able to start.
        WriteTextFile(
            context.ServerPair.CertChainPath,
            ReadCertResource("server1.crt") + ReadCertResource("server3.crt"));

        const auto content = context.Watcher->WatchIdentity(
            context.ServerPair,
            [](const TIdentityContent&) {});
        UNIT_ASSERT(!content.CertChain.empty());
        UNIT_ASSERT(context.GetExpireTs(context.ServerPair.CertChainPath) > 0);
    }

    Y_UNIT_TEST(ShouldNotApplyRotatedCertificateUntilStableRead)
    {
        TWatcherContext context;
        context.Watcher->Start();
        Y_DEFER {
            context.Watcher->Stop();
        };

        const ui64 initial =
            context.GetExpireTs(context.ServerPair.CertChainPath);

        // Content seen once is not applied yet.
        context.RotateServer("server2.key", "server2.crt");
        context.Scheduler->RunPending();
        UNIT_ASSERT_VALUES_EQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));
        UNIT_ASSERT(context.ServerUpdates.empty());

        // Content changed again, so it is still not stable.
        context.RotateServer("server3.key", "server3.crt");
        context.Scheduler->RunPending();
        UNIT_ASSERT_VALUES_EQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));

        context.Scheduler->RunPending();
        UNIT_ASSERT_VALUES_UNEQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));
        UNIT_ASSERT_VALUES_EQUAL(1, context.ServerUpdates.size());
        UNIT_ASSERT_VALUES_EQUAL(
            ReadCertResource("server3.crt"),
            context.ServerUpdates[0].CertChain);
    }

    Y_UNIT_TEST(ShouldCheckFilesOncePerInterval)
    {
        const auto interval = TDuration::Hours(1);
        const auto tolerance = TDuration::Seconds(10);
        TWatcherContext context(interval);
        context.Watcher->Start();
        Y_DEFER {
            context.Watcher->Stop();
        };

        auto assertNextDelay = [&](TDuration expected)
        {
            const auto delays = context.Scheduler->PendingDelays();
            UNIT_ASSERT_VALUES_EQUAL(1, delays.size());
            UNIT_ASSERT_C(
                delays[0] <= expected && delays[0] + tolerance >= expected,
                delays[0]);
        };

        assertNextDelay(interval);

        context.Scheduler->RunPending();
        assertNextDelay(interval);

        context.RotateServer("server3.key", "server3.crt");
        context.Scheduler->RunPending();
        assertNextDelay(interval);

        context.Scheduler->RunPending();
        assertNextDelay(interval);
    }

    Y_UNIT_TEST(ShouldApplyNewContentOnDemandRightAway)
    {
        TWatcherContext context(TDuration::Hours(1));
        context.Watcher->Start();
        Y_DEFER {
            context.Watcher->Stop();
        };

        const ui64 initial =
            context.GetExpireTs(context.ServerPair.CertChainPath);

        context.RotateServer("server3.key", "server3.crt");
        auto future = context.Watcher->UpdateCertificates();
        context.Scheduler->RunPendingWithin(TDuration::Zero());

        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_C(
            !HasError(future.GetValue()),
            FormatError(future.GetValue()));
        UNIT_ASSERT_VALUES_UNEQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));
        UNIT_ASSERT_VALUES_EQUAL(1, context.ServerUpdates.size());
        // Only the periodic check is left.
        UNIT_ASSERT_VALUES_EQUAL(1, context.Scheduler->PendingCount());
    }

    Y_UNIT_TEST(ShouldValidateNewContentOnDemand)
    {
        TWatcherContext context(TDuration::Hours(1));
        context.Watcher->Start();
        Y_DEFER {
            context.Watcher->Stop();
        };

        const ui64 initial =
            context.GetExpireTs(context.ServerPair.CertChainPath);

        // Chain cannot be built.
        WriteTextFile(
            context.ServerPair.CertChainPath,
            ReadCertResource("server1.crt") + ReadCertResource("server3.crt"));
        auto future = context.Watcher->UpdateCertificates();
        context.Scheduler->RunPendingWithin(TDuration::Zero());

        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_STRING_CONTAINS(
            future.GetValue().GetMessage(),
            "chain cannot be built");
        UNIT_ASSERT_STRING_CONTAINS(
            future.GetValue().GetMessage(),
            context.ServerPair.CertChainPath);
        UNIT_ASSERT_VALUES_EQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));
        UNIT_ASSERT(context.ServerUpdates.empty());
    }

    Y_UNIT_TEST(ShouldRestartStableReadAfterOnDemandUpdate)
    {
        TWatcherContext context(TDuration::Hours(1));
        context.Watcher->Start();
        Y_DEFER {
            context.Watcher->Stop();
        };

        const ui64 initial =
            context.GetExpireTs(context.ServerPair.CertChainPath);

        context.RotateServer("server3.key", "server3.crt");
        context.Scheduler->RunPending();

        // An on-demand update in between sees the current content again.
        context.RotateServer("server1.key", "server1.crt");
        context.Watcher->UpdateCertificates();
        context.Scheduler->RunPendingWithin(TDuration::Zero());

        // The content seen by the first periodic check is not stable.
        context.RotateServer("server3.key", "server3.crt");
        context.Scheduler->RunPending();
        UNIT_ASSERT_VALUES_EQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));

        context.Scheduler->RunPending();
        UNIT_ASSERT_VALUES_UNEQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));
    }

    Y_UNIT_TEST(ShouldServePendingOnDemandUpdateByPeriodicCheck)
    {
        TWatcherContext context(TDuration::Hours(1));
        context.Watcher->Start();
        Y_DEFER {
            context.Watcher->Stop();
        };

        const ui64 initial =
            context.GetExpireTs(context.ServerPair.CertChainPath);

        context.RotateServer("server3.key", "server3.crt");
        auto future = context.Watcher->UpdateCertificates();

        // The periodic check scheduled at start runs before the on-demand
        // update and serves the pending request: the new content is applied
        // right away.
        context.Scheduler->RunNext();
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_C(
            !HasError(future.GetValue()),
            FormatError(future.GetValue()));
        UNIT_ASSERT_VALUES_UNEQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));

        // The on-demand update finds nothing to do, the next periodic check
        // is scheduled.
        context.Scheduler->RunNext();
        UNIT_ASSERT_VALUES_EQUAL(1, context.Scheduler->PendingCount());
    }

    Y_UNIT_TEST(ShouldCompletePendingOnDemandUpdateOnStop)
    {
        TWatcherContext context(TDuration::Hours(1));
        context.Watcher->Start();

        auto future = context.Watcher->UpdateCertificates();
        UNIT_ASSERT(!future.HasValue());

        context.Watcher->Stop();
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, future.GetValue().GetCode());
    }

    Y_UNIT_TEST(ShouldRejectOnDemandUpdateWhileAnotherIsPending)
    {
        TWatcherContext context(TDuration::Hours(1));
        context.Watcher->Start();
        Y_DEFER {
            context.Watcher->Stop();
        };

        auto first = context.Watcher->UpdateCertificates();
        auto second = context.Watcher->UpdateCertificates();
        UNIT_ASSERT(second.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(E_TRY_AGAIN, second.GetValue().GetCode());

        context.Scheduler->RunPendingWithin(TDuration::Zero());
        UNIT_ASSERT(first.HasValue());
        UNIT_ASSERT_C(
            !HasError(first.GetValue()),
            FormatError(first.GetValue()));
    }

    Y_UNIT_TEST(ShouldRejectOnDemandUpdateWhenNotStarted)
    {
        TWatcherContext context(TDuration::Hours(1));

        auto future = context.Watcher->UpdateCertificates();
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, future.GetValue().GetCode());

        context.Watcher->Start();
        context.Watcher->Stop();

        future = context.Watcher->UpdateCertificates();
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, future.GetValue().GetCode());
    }

    Y_UNIT_TEST(ShouldHandleOnDemandRequestsDuringPeriodicUpdates)
    {
        TTempDir tempDir;
        const TString rootPath = TStringBuilder()
            << tempDir.Name() << "/ca.crt";
        WriteTextFile(rootPath, ReadCertResource("ca.crt"));
        const auto pair = CreateCertificatePair(
            tempDir.Name(),
            "server",
            ReadCertResource("server1.key"),
            ReadCertResource("server1.crt"));

        auto scheduler = CreateScheduler();
        scheduler->Start();
        Y_DEFER {
            scheduler->Stop();
        };

        // Every task runs in its own thread, so on-demand updates race with
        // the periodic ones.
        auto watcher = CreateCertificateWatcher(
            CreateLoggingService("console"),
            "TLS_CERTIFICATE_PROVIDER",
            scheduler,
            CreateLongRunningTaskExecutor("TLS_UT"),
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            TDuration::MilliSeconds(1));
        watcher->WatchRootCa(rootPath, [](const TString&) {});
        watcher->WatchIdentity(pair, [](const TIdentityContent&) {});
        watcher->Start();
        Y_DEFER {
            watcher->Stop();
        };

        WriteTextFile(pair.PrivateKeyPath, ReadCertResource("server3.key"));
        WriteTextFile(pair.CertChainPath, ReadCertResource("server3.crt"));
        const auto error = UpdateCertificatesSync(watcher);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));

        auto pending = watcher->UpdateCertificates();
        watcher->Stop();
        UNIT_ASSERT(pending.HasValue());
    }

    Y_UNIT_TEST(ShouldAllowStoppingFromCompletionCallback)
    {
        TWatcherContext context(TDuration::Hours(1));
        context.Watcher->Start();

        // Hangs if Stop() waits for the update that runs the callback.
        bool stopped = false;
        context.Watcher->UpdateCertificates().Subscribe(
            [&](const auto&)
            {
                context.Watcher->Stop();
                stopped = true;
            });
        context.Scheduler->RunPendingWithin(TDuration::Zero());

        UNIT_ASSERT(stopped);
    }

    Y_UNIT_TEST(ShouldKeepCertificateWhenRefreshedFilesAreInvalid)
    {
        TWatcherContext context;
        context.Watcher->Start();
        Y_DEFER {
            context.Watcher->Stop();
        };

        const ui64 initial =
            context.GetExpireTs(context.ServerPair.CertChainPath);

        // Broken certificate file.
        WriteTextFile(context.ServerPair.CertChainPath, "broken");
        context.RunUntilStable();
        UNIT_ASSERT_VALUES_EQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));

        // Private key does not match the certificate.
        context.RotateServer("server3.key", "server1.crt");
        context.RunUntilStable();
        UNIT_ASSERT_VALUES_EQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));

        // Chain cannot be built.
        WriteTextFile(
            context.ServerPair.PrivateKeyPath,
            ReadCertResource("server1.key"));
        WriteTextFile(
            context.ServerPair.CertChainPath,
            ReadCertResource("server1.crt") + ReadCertResource("server3.crt"));
        context.RunUntilStable();
        UNIT_ASSERT_VALUES_EQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));
        UNIT_ASSERT(context.ServerUpdates.empty());

        // Valid content is applied.
        context.RotateServer("server3.key", "server3.crt");
        context.RunUntilStable();
        UNIT_ASSERT_VALUES_UNEQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));
        UNIT_ASSERT_VALUES_EQUAL(1, context.ServerUpdates.size());
    }

    Y_UNIT_TEST(ShouldRestartStableReadOnReadError)
    {
        TWatcherContext context;
        context.Watcher->Start();
        Y_DEFER {
            context.Watcher->Stop();
        };

        const ui64 initial =
            context.GetExpireTs(context.ServerPair.CertChainPath);

        context.RotateServer("server3.key", "server3.crt");
        context.Scheduler->RunPending();

        // A read error, e.g. in the middle of a non-atomic rotation, restarts
        // the hold.
        NFs::Remove(context.ServerPair.PrivateKeyPath);
        context.Scheduler->RunPending();

        WriteTextFile(
            context.ServerPair.PrivateKeyPath,
            ReadCertResource("server3.key"));
        context.Scheduler->RunPending();
        UNIT_ASSERT_VALUES_EQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));

        context.Scheduler->RunPending();
        UNIT_ASSERT_VALUES_UNEQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));
    }

    Y_UNIT_TEST(ShouldKeepRootCaWhenRefreshedFileIsInvalid)
    {
        TWatcherContext context;
        context.Watcher->Start();
        Y_DEFER {
            context.Watcher->Stop();
        };

        const auto initial = context.GetFingerprint(context.RootPath);

        WriteTextFile(context.RootPath, "not a certificate");
        context.RunUntilStable();
        UNIT_ASSERT_VALUES_EQUAL(
            initial,
            context.GetFingerprint(context.RootPath));
        UNIT_ASSERT(context.RootUpdates.empty());

        WriteTextFile(context.RootPath, ReadCertResource("server2.crt"));
        context.RunUntilStable();
        UNIT_ASSERT_VALUES_UNEQUAL(
            initial,
            context.GetFingerprint(context.RootPath));
        UNIT_ASSERT_VALUES_EQUAL(1, context.RootUpdates.size());
        UNIT_ASSERT_VALUES_EQUAL(
            ReadCertResource("server2.crt"),
            context.RootUpdates[0]);
    }

    Y_UNIT_TEST(ShouldNotifyOnlySubscribersOfChangedFile)
    {
        TWatcherContext context;
        context.Watcher->Start();
        Y_DEFER {
            context.Watcher->Stop();
        };

        WriteTextFile(context.RootPath, ReadCertResource("server2.crt"));
        context.RunUntilStable();
        UNIT_ASSERT_VALUES_EQUAL(1, context.RootUpdates.size());
        UNIT_ASSERT(context.ServerUpdates.empty());
        UNIT_ASSERT(context.ClientUpdates.empty());

        context.RotateServer("server3.key", "server3.crt");
        context.RunUntilStable();
        UNIT_ASSERT_VALUES_EQUAL(1, context.RootUpdates.size());
        UNIT_ASSERT_VALUES_EQUAL(1, context.ServerUpdates.size());
        UNIT_ASSERT(context.ClientUpdates.empty());
    }

    Y_UNIT_TEST(ShouldNotifyEverySubscriberOfSharedEntry)
    {
        TWatcherContext context;
        TVector<TIdentityContent> second;
        context.Watcher->WatchIdentity(
            context.ServerPair,
            [&](const TIdentityContent& content) { second.push_back(content); });
        context.Watcher->Start();
        Y_DEFER {
            context.Watcher->Stop();
        };

        context.RotateServer("server3.key", "server3.crt");
        context.RunUntilStable();
        UNIT_ASSERT_VALUES_EQUAL(1, context.ServerUpdates.size());
        UNIT_ASSERT_VALUES_EQUAL(1, second.size());
        UNIT_ASSERT_VALUES_EQUAL(
            context.ServerUpdates[0].CertChain,
            second[0].CertChain);
    }

    Y_UNIT_TEST(ShouldNotSchedulePeriodicCheckWithoutEntries)
    {
        TWatcherContext context(TDuration::Seconds(1), /*watchFiles=*/false);
        context.Watcher->Start();
        Y_DEFER {
            context.Watcher->Stop();
        };

        UNIT_ASSERT_VALUES_EQUAL(0, context.Scheduler->PendingCount());
    }

    Y_UNIT_TEST(ShouldSchedulePeriodicCheckOnFirstWatchAfterStart)
    {
        TWatcherContext context(TDuration::Seconds(1), /*watchFiles=*/false);
        context.Watcher->Start();
        Y_DEFER {
            context.Watcher->Stop();
        };

        context.WatchAll();
        UNIT_ASSERT_VALUES_EQUAL(1, context.Scheduler->PendingCount());

        // A second watch does not add a second periodic task.
        context.Watcher->WatchRootCa(context.RootPath, [](const TString&) {});
        UNIT_ASSERT_VALUES_EQUAL(1, context.Scheduler->PendingCount());

        context.RotateServer("server3.key", "server3.crt");
        context.RunUntilStable();
        UNIT_ASSERT_VALUES_EQUAL(1, context.ServerUpdates.size());
    }

    Y_UNIT_TEST(ShouldNotSchedulePeriodicCheckWithZeroInterval)
    {
        TWatcherContext context(TDuration::Zero());
        context.Watcher->Start();
        Y_DEFER {
            context.Watcher->Stop();
        };

        UNIT_ASSERT_VALUES_EQUAL(0, context.Scheduler->PendingCount());

        const ui64 initial =
            context.GetExpireTs(context.ServerPair.CertChainPath);
        context.RotateServer("server3.key", "server3.crt");
        auto future = context.Watcher->UpdateCertificates();
        context.Scheduler->RunPending();

        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_C(
            !HasError(future.GetValue()),
            FormatError(future.GetValue()));
        UNIT_ASSERT_VALUES_UNEQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));
        UNIT_ASSERT_VALUES_EQUAL(0, context.Scheduler->PendingCount());
    }
}

}   // namespace NCloud
