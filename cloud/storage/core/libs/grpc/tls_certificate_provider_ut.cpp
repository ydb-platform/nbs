#include "tls_certificate_provider.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/timer_test.h>
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
            TDuration::Seconds(1),
            CreateWallClockTimer());
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

    const ITimerPtr Timer;

    TMutex Lock;
    TDeque<TScheduled> Pending;

public:
    explicit TManualScheduler(ITimerPtr timer)
        : Timer(std::move(timer))
    {}

    void Start() override
    {}

    void Stop() override
    {}

    void Schedule(ITaskQueue*, TInstant deadline, TCallback callback) override
    {
        TGuard guard(Lock);
        Pending.push_back({
            .Delay = deadline - Timer->Now(),
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

    const TDuration RefreshInterval;
    std::shared_ptr<TTestTimer> Timer;
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
        , RefreshInterval(refreshInterval)
        , Timer(std::make_shared<TTestTimer>())
        , Scheduler(std::make_shared<TManualScheduler>(Timer))
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
            refreshInterval,
            Timer);
    }

    // New content is applied only after it has stayed unchanged for half of
    // the refresh interval since it was first read.
    void RunUntilStable() const
    {
        Scheduler->RunPending();
        Timer->AdvanceTime(RefreshInterval / 2);
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

        context.Provider->UpdateCertificates().GetValueSync();

        UNIT_ASSERT(
            context.GetExpireTs(context.ServerPair.CertChainPath) > 0);
        UNIT_ASSERT(
            context.GetExpireTs(context.ClientPair.CertChainPath) > 0);
    }

    Y_UNIT_TEST(ShouldSkipUpdateIfRootCaIsInvalid)
    {
        TCertificateProviderTestContext context;
        context.Provider->UpdateCertificates().GetValueSync();

        UNIT_ASSERT(
            context.GetExpireTs(context.ServerPair.CertChainPath) > 0);
        const ui64 before = context.GetExpireTs(
            context.ServerPair.CertChainPath);

        WriteTextFile(context.RootPath, "not a certificate");

        context.Provider->UpdateCertificates().GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(
            before,
            context.GetExpireTs(context.ServerPair.CertChainPath));
    }

    Y_UNIT_TEST(ShouldSkipUpdateIfAnyIdentityPairBecomesInvalid)
    {
        TCertificateProviderTestContext context;

        context.Provider->UpdateCertificates().GetValueSync();
        UNIT_ASSERT(
            context.GetExpireTs(context.ClientPair.CertChainPath) > 0);
        const ui64 before = context.GetExpireTs(
            context.ClientPair.CertChainPath);

        WriteTextFile(
            context.ClientPair.CertChainPath,
            "broken certificate chain");

        context.Provider->UpdateCertificates().GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(
            before,
            context.GetExpireTs(context.ClientPair.CertChainPath));
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
                TDuration::Seconds(1),
                CreateWallClockTimer()),
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
                TDuration::Seconds(1),
                CreateWallClockTimer()),
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

    Y_UNIT_TEST(ShouldPickUpRotatedCertificateOnTimer)
    {
        TManualProviderContext context;
        context.Provider->Start();
        Y_DEFER {
            context.Provider->Stop();
        };

        const ui64 before =
            context.GetExpireTs(context.ServerPair.CertChainPath);
        UNIT_ASSERT(before > 0);

        context.RotateServer("server3.key", "server3.crt");
        context.RunUntilStable();

        const ui64 after =
            context.GetExpireTs(context.ServerPair.CertChainPath);
        UNIT_ASSERT(after > 0);
        UNIT_ASSERT_VALUES_UNEQUAL(before, after);
    }

    Y_UNIT_TEST(ShouldRecoverAfterIdentityBecomesValidAgain)
    {
        TManualProviderContext context;
        context.Provider->Start();
        Y_DEFER {
            context.Provider->Stop();
        };

        const ui64 initial =
            context.GetExpireTs(context.ServerPair.CertChainPath);
        UNIT_ASSERT(initial > 0);

        WriteTextFile(context.ServerPair.CertChainPath, "broken");
        context.RunUntilStable();
        UNIT_ASSERT_VALUES_EQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));

        context.RotateServer("server3.key", "server3.crt");
        context.RunUntilStable();

        const ui64 recovered =
            context.GetExpireTs(context.ServerPair.CertChainPath);
        UNIT_ASSERT(recovered > 0);
        UNIT_ASSERT_VALUES_UNEQUAL(initial, recovered);
    }

    Y_UNIT_TEST(ShouldUsePeriodicProviderForRootOnlyTlsConfig)
    {
        TTempDir tempDir;
        const TString rootPath = TStringBuilder()
            << tempDir.Name() << "/ca.crt";
        WriteTextFile(rootPath, ReadCertResource("ca.crt"));

        auto scheduler =
            std::make_shared<TManualScheduler>(CreateWallClockTimer());
        auto provider = CreateCertificateProvider(
            CreateLoggingService("console"),
            "TLS_CERTIFICATE_PROVIDER",
            scheduler,
            CreateTaskQueueStub(),
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            rootPath,
            TVector<TCertificateFiles>{{}},
            TDuration::Seconds(1),
            CreateWallClockTimer());

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
            std::make_shared<TManualScheduler>(CreateWallClockTimer());
        auto provider = CreateCertificateProvider(
            CreateLoggingService("console"),
            "TLS_CERTIFICATE_PROVIDER",
            scheduler,
            CreateTaskQueueStub(),
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            rootPath,
            TVector<TCertificateFiles>{},
            TDuration::Seconds(1),
            CreateWallClockTimer());

        provider->Start();
        Y_DEFER {
            provider->Stop();
        };

        const auto initialPending = scheduler->PendingCount();
        provider->UpdateCertificates();
        UNIT_ASSERT_VALUES_EQUAL(initialPending + 1, scheduler->PendingCount());
    }

    Y_UNIT_TEST(ShouldReportRootCaFingerprint)
    {
        TManualProviderContext context;
        context.Provider->Start();
        Y_DEFER {
            context.Provider->Stop();
        };

        const auto beforeFingerprint = context.GetRootCaFingerprint();

        WriteTextFile(context.RootPath, ReadCertResource("server2.crt"));
        context.RunUntilStable();

        const auto afterFingerprint = context.GetRootCaFingerprint();

        UNIT_ASSERT_VALUES_UNEQUAL(beforeFingerprint, afterFingerprint);
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
        context.Timer->AdvanceTime(context.RefreshInterval / 2);
        context.Scheduler->RunPending();
        UNIT_ASSERT_VALUES_EQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));

        // Reading it again right away does not count.
        context.Scheduler->RunPending();
        UNIT_ASSERT_VALUES_EQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));

        context.Timer->AdvanceTime(context.RefreshInterval / 2);
        context.Scheduler->RunPending();
        UNIT_ASSERT_VALUES_UNEQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));
    }

    Y_UNIT_TEST(ShouldRecheckNewContentAfterHalfInterval)
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

        // New content is re-checked after half an interval.
        context.RotateServer("server3.key", "server3.crt");
        context.Scheduler->RunPending();
        assertNextDelay(interval / 2);

        context.Timer->AdvanceTime(interval / 2);
        context.Scheduler->RunPending();
        assertNextDelay(interval);
    }

    Y_UNIT_TEST(ShouldConfirmNewContentAfterOnDemandUpdate)
    {
        const auto interval = TDuration::Hours(1);
        TManualProviderContext context(interval);
        context.Provider->Start();
        Y_DEFER {
            context.Provider->Stop();
        };

        const ui64 initial =
            context.GetExpireTs(context.ServerPair.CertChainPath);

        context.RotateServer("server3.key", "server3.crt");
        auto future = context.Provider->UpdateCertificates();
        context.Scheduler->RunPendingWithin(TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));
        // The update is not complete until the new content is applied.
        UNIT_ASSERT(!future.HasValue());

        // The periodic check and a confirmation after half an interval.
        const auto delays = context.Scheduler->PendingDelays();
        UNIT_ASSERT_VALUES_EQUAL(2, delays.size());
        UNIT_ASSERT_C(delays[1] <= interval / 2, delays[1]);

        context.Timer->AdvanceTime(interval / 2);
        context.Scheduler->RunPendingWithin(interval / 2);
        UNIT_ASSERT_VALUES_UNEQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(1, context.Scheduler->PendingCount());
    }

    Y_UNIT_TEST(ShouldNotApplyNewContentOnRepeatedOnDemandUpdates)
    {
        const auto interval = TDuration::Hours(1);
        TManualProviderContext context(interval);
        context.Provider->Start();
        Y_DEFER {
            context.Provider->Stop();
        };

        const ui64 initial =
            context.GetExpireTs(context.ServerPair.CertChainPath);

        context.RotateServer("server3.key", "server3.crt");
        auto first = context.Provider->UpdateCertificates();
        context.Scheduler->RunPendingWithin(TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(2, context.Scheduler->PendingCount());

        // A repeated request joins the pending one instead of reading the
        // files again right away.
        auto second = context.Provider->UpdateCertificates();
        UNIT_ASSERT_VALUES_EQUAL(2, context.Scheduler->PendingCount());
        context.Scheduler->RunPendingWithin(TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));
        UNIT_ASSERT(!first.HasValue());
        UNIT_ASSERT(!second.HasValue());

        context.Timer->AdvanceTime(interval / 2);
        context.Scheduler->RunPendingWithin(interval / 2);
        UNIT_ASSERT_VALUES_UNEQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));
        UNIT_ASSERT(first.HasValue());
        UNIT_ASSERT(second.HasValue());
    }

    Y_UNIT_TEST(ShouldHoldNewContentWhenPeriodicCheckFiresEarly)
    {
        const auto interval = TDuration::Hours(1);
        TManualProviderContext context(interval);
        context.Provider->Start();
        Y_DEFER {
            context.Provider->Stop();
        };

        const ui64 initial =
            context.GetExpireTs(context.ServerPair.CertChainPath);

        context.RotateServer("server3.key", "server3.crt");
        context.Scheduler->RunPending();

        // A check that happens to run right after the first read, e.g. a
        // periodic one scheduled before an on-demand update, does not count
        // as a stable read.
        context.Scheduler->RunPending();
        UNIT_ASSERT_VALUES_EQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));
        const auto delays = context.Scheduler->PendingDelays();
        UNIT_ASSERT_VALUES_EQUAL(1, delays.size());
        UNIT_ASSERT_C(delays[0] <= interval / 2, delays[0]);

        context.Timer->AdvanceTime(interval / 2);
        context.Scheduler->RunPending();
        UNIT_ASSERT_VALUES_UNEQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));
    }

    Y_UNIT_TEST(ShouldCompleteOnDemandUpdateOnStop)
    {
        TManualProviderContext context(TDuration::Hours(1));
        context.Provider->Start();

        context.RotateServer("server3.key", "server3.crt");
        auto future = context.Provider->UpdateCertificates();
        context.Scheduler->RunPendingWithin(TDuration::Zero());
        UNIT_ASSERT(!future.HasValue());

        context.Provider->Stop();
        UNIT_ASSERT(future.HasValue());
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
        context.Timer->AdvanceTime(context.RefreshInterval / 2);
        context.Scheduler->RunPending();
        UNIT_ASSERT_VALUES_EQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));

        context.Timer->AdvanceTime(context.RefreshInterval / 2);
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
