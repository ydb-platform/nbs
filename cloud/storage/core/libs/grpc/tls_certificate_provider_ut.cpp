#include "tls_certificate_provider.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/digest/city.h>
#include <util/datetime/base.h>
#include <util/folder/dirut.h>
#include <util/folder/tempdir.h>
#include <util/generic/deque.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/fs.h>
#include <util/system/mutex.h>

#include <sys/stat.h>

#include <memory>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString ReadCertResource(TStringBuf relativePath)
{
    return NResource::Find(
        TStringBuilder() << "grpc/ut/certs/" << relativePath);
}

ui64 RootCaFingerprint(TStringBuf rootCa)
{
    return CityHash64(rootCa) & ((1ULL << 53) - 1);
}

// Retries while the provider is busy with another update, e.g. a periodic one
// run by a real scheduler.
NProto::TError UpdateCertificatesSync(const ICertificateProviderPtr& provider)
{
    while (true) {
        auto error = provider->UpdateCertificates().GetValueSync();
        if (error.GetCode() != E_TRY_AGAIN) {
            return error;
        }
        Sleep(TDuration::MilliSeconds(1));
    }
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

struct TCertificateProviderTestContext
{
    TTempDir TempDir;

    TString RootPath;
    TCertificateFiles ServerPair;
    TCertificateFiles ClientPair;

    TString RootPem;
    TString ServerPem;
    TString ClientPem;

    ISchedulerPtr Scheduler;
    ICertificateProviderPtr Provider;
    NMonitoring::TDynamicCountersPtr RootCounters;
    NMonitoring::TDynamicCountersPtr ServerGroup;

    TCertificateProviderTestContext()
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
        , RootPem(ReadCertResource("ca.crt"))
        , ServerPem(ReadCertResource("server1.crt"))
        , ClientPem(ReadCertResource("server2.crt"))
        , Scheduler(CreateScheduler())
    {
        Scheduler->Start();
        RestoreFiles();

        RootCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        ServerGroup = RootCounters->GetSubgroup("component", "server");

        Provider = CreateCertificateProvider(
            CreateLoggingService("console"),
            "TLS_CERTIFICATE_PROVIDER",
            Scheduler,
            CreateTaskQueueStub(),
            ServerGroup,
            RootPath,
            TVector<TCertificateFiles>{ServerPair, ClientPair},
            TDuration::Seconds(1));
        UNIT_ASSERT(Provider);
        Provider->Start();
    }

    ~TCertificateProviderTestContext()
    {
        if (Provider) {
            Provider->Stop();
        }
        Scheduler->Stop();
    }

    void RestoreFiles() const
    {
        WriteTextFile(RootPath, RootPem);
        WriteTextFile(ServerPair.CertChainPath, ServerPem);
        WriteTextFile(ClientPair.CertChainPath, ClientPem);
    }

    ui64 GetExpireTs(const TString& certPath) const
    {
        auto certGroup = ServerGroup
            ->GetSubgroup("subsystem", "certificates")
            ->GetSubgroup("cert", GetBaseName(certPath));
        return certGroup
            ->GetCounter("ExpireTs", false)
            ->Val();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TManualScheduler final
    : public IScheduler
{
private:
    struct TScheduled
    {
        // How far ahead of the scheduling time the task was scheduled.
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

    // Runs the task that was scheduled first.
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

    // Runs only the tasks scheduled at most |maxDelay| ahead.
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

struct TManualProviderContext
{
    TTempDir TempDir;

    TString RootPath;
    TCertificateFiles ServerPair;
    TCertificateFiles ClientPair;

    std::shared_ptr<TManualScheduler> Scheduler;
    NMonitoring::TDynamicCountersPtr RootCounters;
    NMonitoring::TDynamicCountersPtr ServerGroup;
    ICertificateProviderPtr Provider;

    explicit TManualProviderContext(
            TDuration refreshInterval = TDuration::Seconds(1))
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
        , RootCounters(MakeIntrusive<NMonitoring::TDynamicCounters>())
        , ServerGroup(RootCounters->GetSubgroup("component", "server"))
    {
        WriteTextFile(RootPath, ReadCertResource("ca.crt"));

        Provider = CreatePeriodicCertificateProvider(
            CreateLoggingService("console"),
            "TLS_CERTIFICATE_PROVIDER",
            Scheduler,
            CreateTaskQueueStub(),
            ServerGroup,
            RootPath,
            TVector<TCertificateFiles>{ServerPair, ClientPair},
            refreshInterval);
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

    ui64 GetExpireTs(const TString& certPath) const
    {
        return ServerGroup
            ->GetSubgroup("subsystem", "certificates")
            ->GetSubgroup("cert", GetBaseName(certPath))
            ->GetCounter("ExpireTs", false)
            ->Val();
    }

    ui64 GetRootCaFingerprint() const
    {
        auto rootGroup = ServerGroup
            ->GetSubgroup("subsystem", "certificates")
            ->GetSubgroup("cert", GetBaseName(RootPath));
        return rootGroup->GetCounter("Fingerprint", false)->Val();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TTlsCertificateProviderTest)
{
    Y_UNIT_TEST(ShouldUpdateCertificates)
    {
        TCertificateProviderTestContext context;

        const ui64 initial =
            context.GetExpireTs(context.ServerPair.CertChainPath);
        UNIT_ASSERT(initial > 0);

        WriteTextFile(
            context.ServerPair.PrivateKeyPath,
            ReadCertResource("server3.key"));
        WriteTextFile(
            context.ServerPair.CertChainPath,
            ReadCertResource("server3.crt"));
        const auto error = UpdateCertificatesSync(context.Provider);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));

        UNIT_ASSERT_VALUES_UNEQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));
    }

    Y_UNIT_TEST(ShouldFailStartWithInvalidInitialCertificates)
    {
        TTempDir tempDir;
        const TString rootPath = TStringBuilder()
            << tempDir.Name() << "/ca.crt";
        const auto pair = CreateCertificatePair(
            tempDir.Name(),
            "server",
            ReadCertResource("server1.key"),
            "broken certificate");
        WriteTextFile(rootPath, "broken root");

        auto scheduler = CreateScheduler();
        scheduler->Start();
        Y_DEFER {
            scheduler->Stop();
        };

        UNIT_ASSERT_EXCEPTION(
            CreatePeriodicCertificateProvider(
                CreateLoggingService("console"),
                "TLS_CERTIFICATE_PROVIDER",
                scheduler,
                CreateTaskQueueStub(),
                MakeIntrusive<NMonitoring::TDynamicCounters>(),
                rootPath,
                TVector<TCertificateFiles>{pair},
                TDuration::Seconds(1)),
            yexception);
    }

    Y_UNIT_TEST(ShouldFailStartWhenInitialCertificateFilesMissing)
    {
        auto scheduler = CreateScheduler();
        scheduler->Start();
        Y_DEFER {
            scheduler->Stop();
        };

        UNIT_ASSERT_EXCEPTION(
            CreatePeriodicCertificateProvider(
                CreateLoggingService("console"),
                "TLS_CERTIFICATE_PROVIDER",
                scheduler,
                CreateTaskQueueStub(),
                MakeIntrusive<NMonitoring::TDynamicCounters>(),
                "/nonexistent/ca.crt",
                TVector<TCertificateFiles>{{
                    .PrivateKeyPath = "/nonexistent/server.key",
                    .CertChainPath = "/nonexistent/server.crt",
                }},
                TDuration::Seconds(1)),
            yexception);
    }

    Y_UNIT_TEST(ShouldSkipEmptyPairsInStaticProvider)
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

        TVector<TCertificateFiles> certs{
            {},
            pair,
            {},
        };

        auto provider = CreateStaticCertificateProvider(
            rootPath,
            std::move(certs));
        UNIT_ASSERT(provider);
        UNIT_ASSERT(provider->CreateSecureServerCredentials());
        UNIT_ASSERT(provider->CreateSecureClientCredentials());
    }

    Y_UNIT_TEST(ShouldRejectStaticProviderIncompletePair)
    {
        UNIT_ASSERT_EXCEPTION(
            CreateStaticCertificateProvider(
                {},
                {TCertificateFiles{.PrivateKeyPath = "/tmp/k"}}),
            yexception);

        UNIT_ASSERT_EXCEPTION(
            CreateStaticCertificateProvider(
                {},
                {TCertificateFiles{.CertChainPath = "/tmp/c"}}),
            yexception);
    }

    Y_UNIT_TEST(ShouldUsePeriodicProviderForRootOnlyTlsConfig)
    {
        TTempDir tempDir;
        const TString rootPath = TStringBuilder()
            << tempDir.Name() << "/ca.crt";
        WriteTextFile(rootPath, ReadCertResource("ca.crt"));

        auto scheduler =
            std::make_shared<TManualScheduler>();
        auto provider = CreateCertificateProvider(
            CreateLoggingService("console"),
            "TLS_CERTIFICATE_PROVIDER",
            scheduler,
            CreateTaskQueueStub(),
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            rootPath,
            TVector<TCertificateFiles>{{}},
            TDuration::Seconds(1));

        provider->Start();
        Y_DEFER {
            provider->Stop();
        };

        UNIT_ASSERT_VALUES_EQUAL(1, scheduler->PendingCount());
    }

    Y_UNIT_TEST(ShouldRefreshRootOnlyTlsConfigOnDemand)
    {
        TTempDir tempDir;
        const TString rootPath = TStringBuilder()
            << tempDir.Name() << "/ca.crt";
        WriteTextFile(rootPath, ReadCertResource("ca.crt"));

        auto scheduler =
            std::make_shared<TManualScheduler>();
        auto provider = CreateCertificateProvider(
            CreateLoggingService("console"),
            "TLS_CERTIFICATE_PROVIDER",
            scheduler,
            CreateTaskQueueStub(),
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            rootPath,
            TVector<TCertificateFiles>{},
            TDuration::Seconds(1));

        provider->Start();
        Y_DEFER {
            provider->Stop();
        };

        const auto initialPending = scheduler->PendingCount();
        provider->UpdateCertificates();
        UNIT_ASSERT_VALUES_EQUAL(initialPending + 1, scheduler->PendingCount());
    }

    Y_UNIT_TEST(ShouldNotApplyRotatedCertificateUntilStableRead)
    {
        TManualProviderContext context;
        context.Provider->Start();
        Y_DEFER {
            context.Provider->Stop();
        };

        const ui64 initial =
            context.GetExpireTs(context.ServerPair.CertChainPath);

        // Content seen once is not applied yet.
        context.RotateServer("server2.key", "server2.crt");
        context.Scheduler->RunPending();
        UNIT_ASSERT_VALUES_EQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));

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
    }

    Y_UNIT_TEST(ShouldCheckFilesOncePerInterval)
    {
        const auto interval = TDuration::Hours(1);
        const auto tolerance = TDuration::Seconds(10);
        TManualProviderContext context(interval);
        context.Provider->Start();
        Y_DEFER {
            context.Provider->Stop();
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

        // Unchanged files are checked once per interval.
        context.Scheduler->RunPending();
        assertNextDelay(interval);

        // New content does not change the schedule either.
        context.RotateServer("server3.key", "server3.crt");
        context.Scheduler->RunPending();
        assertNextDelay(interval);

        context.Scheduler->RunPending();
        assertNextDelay(interval);
    }

    Y_UNIT_TEST(ShouldApplyNewContentOnDemandRightAway)
    {
        TManualProviderContext context(TDuration::Hours(1));
        context.Provider->Start();
        Y_DEFER {
            context.Provider->Stop();
        };

        const ui64 initial =
            context.GetExpireTs(context.ServerPair.CertChainPath);

        context.RotateServer("server3.key", "server3.crt");
        auto future = context.Provider->UpdateCertificates();
        context.Scheduler->RunPendingWithin(TDuration::Zero());

        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_C(
            !HasError(future.GetValue()),
            FormatError(future.GetValue()));
        UNIT_ASSERT_VALUES_UNEQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));
        // Only the periodic check is left.
        UNIT_ASSERT_VALUES_EQUAL(1, context.Scheduler->PendingCount());
    }

    Y_UNIT_TEST(ShouldValidateNewContentOnDemand)
    {
        TManualProviderContext context(TDuration::Hours(1));
        context.Provider->Start();
        Y_DEFER {
            context.Provider->Stop();
        };

        const ui64 initial =
            context.GetExpireTs(context.ServerPair.CertChainPath);

        // Chain cannot be built.
        WriteTextFile(
            context.ServerPair.CertChainPath,
            ReadCertResource("server1.crt") + ReadCertResource("server3.crt"));
        auto future = context.Provider->UpdateCertificates();
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
    }

    Y_UNIT_TEST(ShouldRestartStableReadAfterOnDemandUpdate)
    {
        TManualProviderContext context(TDuration::Hours(1));
        context.Provider->Start();
        Y_DEFER {
            context.Provider->Stop();
        };

        const ui64 initial =
            context.GetExpireTs(context.ServerPair.CertChainPath);

        context.RotateServer("server3.key", "server3.crt");
        context.Scheduler->RunPending();

        // An on-demand update in between sees the current content again.
        context.RotateServer("server1.key", "server1.crt");
        context.Provider->UpdateCertificates();
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
        TManualProviderContext context(TDuration::Hours(1));
        context.Provider->Start();
        Y_DEFER {
            context.Provider->Stop();
        };

        const ui64 initial =
            context.GetExpireTs(context.ServerPair.CertChainPath);

        context.RotateServer("server3.key", "server3.crt");
        auto future = context.Provider->UpdateCertificates();

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
        TManualProviderContext context(TDuration::Hours(1));
        context.Provider->Start();

        auto future = context.Provider->UpdateCertificates();
        UNIT_ASSERT(!future.HasValue());

        context.Provider->Stop();
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, future.GetValue().GetCode());
    }

    Y_UNIT_TEST(ShouldRejectOnDemandUpdateWhileAnotherIsPending)
    {
        TManualProviderContext context(TDuration::Hours(1));
        context.Provider->Start();
        Y_DEFER {
            context.Provider->Stop();
        };

        auto first = context.Provider->UpdateCertificates();
        auto second = context.Provider->UpdateCertificates();
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
        TManualProviderContext context(TDuration::Hours(1));

        auto future = context.Provider->UpdateCertificates();
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, future.GetValue().GetCode());

        context.Provider->Start();
        context.Provider->Stop();

        future = context.Provider->UpdateCertificates();
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, future.GetValue().GetCode());
    }

    Y_UNIT_TEST(ShouldRunOnDemandUpdatesConcurrentlyWithPeriodicOnes)
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
        auto provider = CreatePeriodicCertificateProvider(
            CreateLoggingService("console"),
            "TLS_CERTIFICATE_PROVIDER",
            scheduler,
            CreateLongRunningTaskExecutor("TLS_UT"),
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            rootPath,
            TVector<TCertificateFiles>{pair},
            TDuration::MilliSeconds(1));
        provider->Start();
        Y_DEFER {
            provider->Stop();
        };

        WriteTextFile(pair.PrivateKeyPath, ReadCertResource("server3.key"));
        WriteTextFile(pair.CertChainPath, ReadCertResource("server3.crt"));
        const auto error = UpdateCertificatesSync(provider);
        UNIT_ASSERT_C(!HasError(error), FormatError(error));

        auto pending = provider->UpdateCertificates();
        provider->Stop();
        UNIT_ASSERT(pending.HasValue());
    }

    Y_UNIT_TEST(ShouldAllowStoppingFromCompletionCallback)
    {
        TManualProviderContext context(TDuration::Hours(1));
        context.Provider->Start();

        // Hangs if Stop() waits for the update that runs the callback.
        bool stopped = false;
        context.Provider->UpdateCertificates().Subscribe(
            [&](const auto&)
            {
                context.Provider->Stop();
                stopped = true;
            });
        context.Scheduler->RunPendingWithin(TDuration::Zero());

        UNIT_ASSERT(stopped);
    }

    Y_UNIT_TEST(ShouldKeepCertificateWhenRefreshedFilesAreInvalid)
    {
        TManualProviderContext context;
        context.Provider->Start();
        Y_DEFER {
            context.Provider->Stop();
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

        // Valid content is applied.
        context.RotateServer("server3.key", "server3.crt");
        context.RunUntilStable();
        UNIT_ASSERT_VALUES_UNEQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));
    }

    Y_UNIT_TEST(ShouldRestartStableReadOnReadError)
    {
        TManualProviderContext context;
        context.Provider->Start();
        Y_DEFER {
            context.Provider->Stop();
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
        TManualProviderContext context;
        context.Provider->Start();
        Y_DEFER {
            context.Provider->Stop();
        };

        const auto initial = context.GetRootCaFingerprint();

        WriteTextFile(context.RootPath, "not a certificate");
        context.RunUntilStable();
        UNIT_ASSERT_VALUES_EQUAL(initial, context.GetRootCaFingerprint());

        WriteTextFile(context.RootPath, ReadCertResource("server2.crt"));
        context.RunUntilStable();
        UNIT_ASSERT_VALUES_UNEQUAL(initial, context.GetRootCaFingerprint());
    }

    Y_UNIT_TEST(ShouldStartWithInvalidInitialChain)
    {
        TTempDir tempDir;
        const TString rootPath = TStringBuilder()
            << tempDir.Name() << "/ca.crt";
        WriteTextFile(rootPath, ReadCertResource("ca.crt"));

        // The chain cannot be built. It is served as is and only reported,
        // so that the service is able to start.
        const auto pair = CreateCertificatePair(
            tempDir.Name(),
            "server",
            ReadCertResource("server1.key"),
            ReadCertResource("server1.crt") + ReadCertResource("server3.crt"));

        auto scheduler = std::make_shared<TManualScheduler>();
        auto rootCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto serverGroup = rootCounters->GetSubgroup("component", "server");

        auto provider = CreatePeriodicCertificateProvider(
            CreateLoggingService("console"),
            "TLS_CERTIFICATE_PROVIDER",
            scheduler,
            CreateTaskQueueStub(),
            serverGroup,
            rootPath,
            TVector<TCertificateFiles>{pair},
            TDuration::Seconds(1));
        provider->Start();
        Y_DEFER {
            provider->Stop();
        };

        const auto expireTs = serverGroup
            ->GetSubgroup("subsystem", "certificates")
            ->GetSubgroup("cert", GetBaseName(pair.CertChainPath))
            ->GetCounter("ExpireTs", false)
            ->Val();
        UNIT_ASSERT(expireTs > 0);
        UNIT_ASSERT(provider->CreateSecureServerCredentials());
    }

    Y_UNIT_TEST(ShouldReportExpireTsCountersForStaticProvider)
    {
        TTempDir tempDir;
        const TString rootPath = tempDir.Path() / "ca.crt";
        WriteTextFile(rootPath, ReadCertResource("ca.crt"));

        const TVector certs{
            CreateCertificatePair(
                tempDir.Name(),
                "server",
                ReadCertResource("server1.key"),
                ReadCertResource("server1.crt")),
            CreateCertificatePair(
                tempDir.Name(),
                "client",
                ReadCertResource("server2.key"),
                ReadCertResource("server2.crt")),
        };

        auto rootCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        auto serverGroup = rootCounters->GetSubgroup("component", "server");

        auto expireTs = [&](const auto& cert)
        {
            auto certGroup =
                serverGroup->GetSubgroup("subsystem", "certificates")
                    ->GetSubgroup("cert", GetBaseName(cert.CertChainPath));
            return certGroup->GetCounter("ExpireTs", false)->Val();
        };
        auto rootFingerprint = [&]
        {
            auto rootGroup =
                serverGroup->GetSubgroup("subsystem", "certificates")
                    ->GetSubgroup("cert", GetBaseName(rootPath));
            return rootGroup->GetCounter("Fingerprint", false)->Val();
        };

        auto provider = CreateStaticCertificateProvider(
            rootPath,
            certs,
            CreateLoggingService("console"),
            "TLS_CERTIFICATE_PROVIDER",
            serverGroup);

        UNIT_ASSERT(provider);
        provider->Start();

        UNIT_ASSERT(expireTs(certs[0]) > 0);
        UNIT_ASSERT(expireTs(certs[1]) > 0);
        UNIT_ASSERT_VALUES_EQUAL(
            rootFingerprint(),
            RootCaFingerprint(ReadCertResource("ca.crt")));

        UNIT_ASSERT(provider->CreateSecureServerCredentials());
        UNIT_ASSERT(provider->CreateSecureClientCredentials());

        provider->Stop();
    }
}

}   // namespace NCloud
