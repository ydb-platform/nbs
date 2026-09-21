#include "session.h"

#include "client.h"
#include "config.h"
#include "metric.h"

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service_test.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler_test.h>
#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/max_calculator.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>
#include <util/generic/guid.h>
#include <util/system/mutex.h>

#include <array>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

static const TString DefaultDiskId = "path_to_test_volume";
static const TString DefaultMountToken = "mountToken";
static constexpr ui32 DefaultBlocksCount = 1024;

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
    ITimerPtr Timer;
    ISchedulerPtr Scheduler;
    ILoggingServicePtr Logging;
    TClientAppConfigPtr Config;

    IBlockStorePtr Client;

    ISessionPtr Session;

public:
    TBootstrap(
            std::shared_ptr<TTestService> client,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            TString clientVersionInfo,
            ui64 mountSeqNumber)
        : Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , Logging(CreateLoggingService("console"))
        , Config(std::make_shared<TClientAppConfig>())
        , Client(std::move(client))
        , Session(CreateSession(
            Timer,
            Scheduler,
            Logging,
            CreateRequestStatsStub(),
            CreateVolumeStatsStub(),
            Client,
            Config,
            TSessionConfig{
                .DiskId = DefaultDiskId,
                .MountToken = DefaultMountToken,
                .ClientVersionInfo = std::move(clientVersionInfo),
                .MountSeqNumber = mountSeqNumber
            }))
    {}

    void Start()
    {
        if (Scheduler) {
            Scheduler->Start();
        }

        if (Logging) {
            Logging->Start();
        }

        if (Client) {
            Client->Start();
        }
    }

    void Stop()
    {
        if (Client) {
            Client->Stop();
        }

        if (Logging) {
            Logging->Stop();
        }

        if (Scheduler) {
            Scheduler->Stop();
        }
    }

    ITimerPtr GetTimer()
    {
        return Timer;
    }

    ISchedulerPtr GetScheduler()
    {
        return Scheduler;
    }

    ILoggingServicePtr GetLogging()
    {
        return Logging;
    }

    IBlockStorePtr GetClient()
    {
        return Client;
    }

    TClientAppConfigPtr GetConfig()
    {
        return Config;
    }

    ISession* GetSession()
    {
        return Session.get();
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TBootstrap> CreateBootstrap(
    std::shared_ptr<TTestService> client,
    ISchedulerPtr scheduler = {},
    TString clientVersionInfo = {},
    ui64 mountSeqNumber = 0)
{
    if (!scheduler) {
        scheduler = CreateScheduler();
    }

    return std::make_unique<TBootstrap>(
        std::move(client),
        CreateWallClockTimer(),
        std::move(scheduler),
        std::move(clientVersionInfo),
        mountSeqNumber);
}

////////////////////////////////////////////////////////////////////////////////

struct TCounters
{
    ui32 MountRequestsCounter = 0;
    ui32 UnmountRequestsCounter = 0;
    ui32 ReadBlocksRequestsCounter = 0;
    ui32 WriteBlocksRequestsCounter = 0;
    ui32 ZeroBlocksRequestsCounter = 0;
};

std::shared_ptr<TTestService> CreateClientForConsistencyCheck(
    TString& sessionId,
    THashMap<ui64, TString>& blocks,
    TCounters& counters)
{
    auto client = std::make_shared<TTestService>();

    client->MountVolumeHandler =
        [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
            UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

            NProto::TMountVolumeResponse response;
            response.SetSessionId(sessionId);

            auto& volume = *response.MutableVolume();
            volume.SetDiskId(request->GetDiskId());
            volume.SetBlockSize(DefaultBlockSize);
            volume.SetBlocksCount(1024);

            ++counters.MountRequestsCounter;
            return MakeFuture(response);
        };

    client->UnmountVolumeHandler =
        [&] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);

            ++counters.UnmountRequestsCounter;
            if (request->GetSessionId() != sessionId) {
                return MakeFuture<NProto::TUnmountVolumeResponse>(
                    TErrorResponse(E_BS_INVALID_SESSION));
            }

            return MakeFuture<NProto::TUnmountVolumeResponse>();
        };

    client->ReadBlocksLocalHandler =
        [&] (std::shared_ptr<NProto::TReadBlocksLocalRequest> request) {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);

            ++counters.ReadBlocksRequestsCounter;
            if (request->GetSessionId() != sessionId) {
                return MakeFuture<NProto::TReadBlocksLocalResponse>(
                    TErrorResponse(E_BS_INVALID_SESSION));
            }

            auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);
            const auto& sglist = guard.Get();

            for(ui64 i = 0; i < request->GetBlocksCount(); ++i) {
                auto* buf = blocks.FindPtr(request->GetStartIndex() + i);
                auto* dstPtr = const_cast<char*>(sglist[i].Data());
                if (buf) {
                    UNIT_ASSERT_VALUES_EQUAL(sglist[i].Size(), buf->size());
                    memcpy(dstPtr, buf->data(), buf->size());
                } else {
                    memset(dstPtr, 0, sglist[i].Size());
                }
            }

            return MakeFuture(NProto::TReadBlocksLocalResponse());
        };

    client->ZeroBlocksHandler =
        [&] (std::shared_ptr<NProto::TZeroBlocksRequest> request) {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);

            ++counters.ZeroBlocksRequestsCounter;
            if (request->GetSessionId() != sessionId) {
                return MakeFuture<NProto::TZeroBlocksResponse>(
                    TErrorResponse(E_BS_INVALID_SESSION));
            }

            ui64 startIndex = request->GetStartIndex();
            ui32 blocksCount = request->GetBlocksCount();

            for(ui64 i = startIndex; i < (startIndex + blocksCount); ++i) {
                blocks.erase(i);
            }

            return MakeFuture<NProto::TZeroBlocksResponse>();
        };

    client->WriteBlocksLocalHandler =
        [&] (std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) {
            UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);

            ++counters.WriteBlocksRequestsCounter;
            if (request->GetSessionId() != sessionId) {
                return MakeFuture<NProto::TWriteBlocksLocalResponse>(
                    TErrorResponse(E_BS_INVALID_SESSION));
            }

            ui64 startIndex = request->GetStartIndex();

            auto guard = request->Sglist.Acquire();
            UNIT_ASSERT(guard);
            const auto& sglist = guard.Get();

            size_t bufferIndex = 0;

            for(ui64 i = startIndex; i < (startIndex + sglist.size()); ++i) {
                blocks[i] = sglist[bufferIndex++].AsStringBuf();
            }

            return MakeFuture<NProto::TWriteBlocksLocalResponse>();
        };

    return client;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSessionTest)
{
    Y_UNIT_TEST(ShouldCompletePendingIOAfterSessionDestruction)
    {
        for (const bool destroySession: {false, true}) {
            for (const ui32 code: {S_OK, E_IO, E_CANCELLED, E_BS_INVALID_SESSION}) {
                // A live session retries INVALID_SESSION; that path is covered
                // separately by the remount tests.
                if (!destroySession && code == E_BS_INVALID_SESSION) {
                    continue;
                }

                auto completed = NewPromise<void>();
                auto client = std::make_shared<TTestService>();
                client->MountVolumeHandler = [](const auto& request) {
                    NProto::TMountVolumeResponse response;
                    response.SetSessionId("1");
                    response.MutableVolume()->SetDiskId(request->GetDiskId());
                    response.MutableVolume()->SetBlockSize(DefaultBlockSize);
                    response.MutableVolume()->SetBlocksCount(DefaultBlocksCount);
                    return MakeFuture(response);
                };
                client->UnmountVolumeHandler = [](const auto&) {
                    return MakeFuture<NProto::TUnmountVolumeResponse>();
                };
                client->ReadBlocksLocalHandler = [&](const auto&) {
                    return completed.GetFuture().Apply([code](const auto&) {
                        NProto::TReadBlocksLocalResponse response;
                        *response.MutableError() = MakeError(code);
                        return response;
                    });
                };
                client->WriteBlocksLocalHandler = [&](const auto&) {
                    return completed.GetFuture().Apply([code](const auto&) {
                        NProto::TWriteBlocksLocalResponse response;
                        *response.MutableError() = MakeError(code);
                        return response;
                    });
                };
                client->ZeroBlocksHandler = [&](const auto&) {
                    return completed.GetFuture().Apply([code](const auto&) {
                        NProto::TZeroBlocksResponse response;
                        *response.MutableError() = MakeError(code);
                        return response;
                    });
                };

                auto bootstrap = CreateBootstrap(
                    client,
                    std::make_shared<TTestScheduler>());
                auto* session = bootstrap->GetSession();
                UNIT_ASSERT(!HasError(session->MountVolume().GetValue()));
                auto read = session->ReadBlocksLocal(
                    MakeIntrusive<TCallContext>(),
                    std::make_shared<NProto::TReadBlocksLocalRequest>());
                auto monitoring = CreateMonitoringServiceStub();
                auto requestStats = CreateServerRequestStats(
                    monitoring->GetCounters(),
                    CreateWallClockTimer(),
                    EHistogramCounterOption::ReportMultipleCounters,
                    {});
                auto serverStats = CreateServerStats(
                    bootstrap->GetConfig(),
                    std::make_shared<TDiagnosticsConfig>(),
                    monitoring,
                    CreateProfileLogStub(),
                    requestStats,
                    CreateVolumeStatsStub());
                auto service = std::make_shared<TTestService>();
                service->WriteBlocksLocalHandler = [session](const auto& request) {
                    return session->WriteBlocksLocal(
                        MakeIntrusive<TCallContext>(),
                        request);
                };
                auto metricClient = CreateMetricClient(
                    service,
                    bootstrap->GetLogging(),
                    serverStats);
                auto callContext = MakeIntrusive<TCallContext>();
                auto write = metricClient->WriteBlocksLocal(
                    callContext,
                    std::make_shared<NProto::TWriteBlocksLocalRequest>());
                auto zero = session->ZeroBlocks(
                    MakeIntrusive<TCallContext>(),
                    std::make_shared<NProto::TZeroBlocksRequest>());

                if (destroySession) {
                    UNIT_ASSERT(!HasError(session->UnmountVolume().GetValue()));
                    bootstrap.reset();
                }
                // Destroying the session must not hide work still in progress.
                UNIT_ASSERT(!read.HasValue());
                UNIT_ASSERT(!write.HasValue());
                UNIT_ASSERT(!zero.HasValue());
                const auto collect = [&] {
                    return metricClient->CollectRequests(
                        [&](auto& context, auto volumeInfo, auto mediaKind,
                            auto requestType, auto time)
                        {
                            // No per-volume statistics: active work must still
                            // contribute to the aggregate server MaxTime.
                            UNIT_ASSERT(!volumeInfo);
                            serverStats->AddIncompleteRequest(
                                context, volumeInfo, mediaKind, requestType, time);
                        });
                };
                auto maxTime = monitoring->GetCounters()
                    ->GetSubgroup("request", "WriteBlocks")
                    ->GetCounter("MaxTime");
                ui64 previous = 0;
                for (ui32 seconds = 1; seconds <= 3; ++seconds) {
                    callContext->SetRequestStartedCycles(
                        GetCycleCount() -
                        DurationToCyclesSafe(TDuration::Seconds(seconds)));
                    UNIT_ASSERT_VALUES_EQUAL(1, collect());
                    serverStats->UpdateStats(true);
                    UNIT_ASSERT(maxTime->Val() > previous);
                    previous = maxTime->Val();
                }
                completed.SetValue();
                // Check readiness before GetValue: the regression must fail,
                // rather than hang forever on the old implementation.
                UNIT_ASSERT(read.HasValue());
                UNIT_ASSERT(write.HasValue());
                UNIT_ASSERT(zero.HasValue());
                const auto expected = destroySession ? E_REJECTED : code;
                UNIT_ASSERT_VALUES_EQUAL(
                    expected,
                    read.GetValue().GetError().GetCode());
                UNIT_ASSERT_VALUES_EQUAL(
                    expected,
                    write.GetValue().GetError().GetCode());
                UNIT_ASSERT_VALUES_EQUAL(
                    expected,
                    zero.GetValue().GetError().GetCode());
                UNIT_ASSERT_VALUES_EQUAL(0, collect());
                serverStats->UpdateStats(true);
                previous = maxTime->Val();
                for (size_t i = 0; i <= DEFAULT_BUCKET_COUNT; ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(0, collect());
                    serverStats->UpdateStats(true);
                    UNIT_ASSERT(maxTime->Val() <= previous);
                    previous = maxTime->Val();
                }
                UNIT_ASSERT_VALUES_EQUAL(0, maxTime->Val());

                if (destroySession) {
                    client->WriteBlocksLocalHandler = [](const auto&) {
                        return MakeFuture<NProto::TWriteBlocksLocalResponse>();
                    };
                    auto nextBootstrap = CreateBootstrap(
                        client,
                        std::make_shared<TTestScheduler>());
                    auto* nextSession = nextBootstrap->GetSession();
                    NProto::THeaders headers;
                    headers.SetClientId("next-client");
                    UNIT_ASSERT(!HasError(nextSession->MountVolume(
                        MakeIntrusive<TCallContext>(), headers).GetValue()));
                    service->WriteBlocksLocalHandler =
                        [nextSession](const auto& request) {
                            return nextSession->WriteBlocksLocal(
                                MakeIntrusive<TCallContext>(), request);
                        };
                    auto request =
                        std::make_shared<NProto::TWriteBlocksLocalRequest>();
                    *request->MutableHeaders() = headers;
                    auto next = metricClient->WriteBlocksLocal(
                        MakeIntrusive<TCallContext>(), request);
                    UNIT_ASSERT(next.HasValue());
                    UNIT_ASSERT(!HasError(next.GetValue()));
                    UNIT_ASSERT_VALUES_EQUAL(0, collect());
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldCompleteIOWaitingForMountAfterSessionDestruction)
    {
        // Both initial mount overloads, timer remount, and remount requested
        // by an INVALID_SESSION response must release their waiting I/O.
        for (ui32 mountKind = 0; mountKind != 4; ++mountKind) {
            for (const ui32 code: {S_OK, E_IO}) {
                auto completed = NewPromise<void>();
                auto client = std::make_shared<TTestService>();
                size_t mounts = 0;
                size_t writes = 0;
                client->MountVolumeHandler = [&](const auto& request) {
                    NProto::TMountVolumeResponse response;
                    response.SetSessionId(ToString(++mounts));
                    response.SetInactiveClientsTimeout(100);
                    response.MutableVolume()->SetDiskId(request->GetDiskId());
                    response.MutableVolume()->SetBlockSize(DefaultBlockSize);
                    response.MutableVolume()->SetBlocksCount(DefaultBlocksCount);
                    if (mountKind >= 2 && mounts == 1) {
                        return MakeFuture(response);
                    }
                    response.MutableError()->SetCode(code);
                    return completed.GetFuture().Apply([response](const auto&) {
                        return response;
                    });
                };
                client->WriteBlocksLocalHandler = [&](const auto&) {
                    ++writes;
                    if (mountKind == 3 && writes == 1) {
                        return MakeFuture<NProto::TWriteBlocksLocalResponse>(
                            TErrorResponse(E_BS_INVALID_SESSION));
                    }
                    return MakeFuture<NProto::TWriteBlocksLocalResponse>();
                };
                auto scheduler = std::make_shared<TTestScheduler>();
                auto bootstrap = CreateBootstrap(client, scheduler);
                auto* session = bootstrap->GetSession();
                auto mount = mountKind == 1
                    ? session->MountVolume(
                          NProto::VOLUME_ACCESS_READ_WRITE,
                          NProto::VOLUME_MOUNT_LOCAL,
                          1)
                    : session->MountVolume();
                if (mountKind == 2) {
                    UNIT_ASSERT(mount.HasValue());
                    scheduler->RunAllScheduledTasks();
                    mount = session->EnsureVolumeMounted();
                }
                auto write = session->WriteBlocksLocal(
                    MakeIntrusive<TCallContext>(),
                    std::make_shared<NProto::TWriteBlocksLocalRequest>());
                if (mountKind == 3) {
                    mount = session->EnsureVolumeMounted();
                }
                bootstrap.reset();
                UNIT_ASSERT(!mount.HasValue());
                UNIT_ASSERT(!write.HasValue());
                completed.SetValue();
                UNIT_ASSERT(mount.HasValue());
                UNIT_ASSERT(write.HasValue());
                UNIT_ASSERT_VALUES_EQUAL(
                    E_REJECTED,
                    mount.GetValue().GetError().GetCode());
                UNIT_ASSERT_VALUES_EQUAL(
                    E_REJECTED,
                    write.GetValue().GetError().GetCode());
                UNIT_ASSERT_VALUES_EQUAL(mountKind == 3 ? 1 : 0, writes);
            }
        }
    }

    Y_UNIT_TEST(ShouldCompleteUnmountAfterSessionDestruction)
    {
        auto completed = NewPromise<NProto::TUnmountVolumeResponse>();
        auto client = std::make_shared<TTestService>();
        client->MountVolumeHandler = [](const auto&) {
            NProto::TMountVolumeResponse response;
            response.SetSessionId("1");
            response.MutableVolume()->SetBlockSize(DefaultBlockSize);
            response.MutableVolume()->SetBlocksCount(DefaultBlocksCount);
            return MakeFuture(response);
        };
        client->UnmountVolumeHandler = [&](const auto&) {
            return completed.GetFuture();
        };
        auto bootstrap = CreateBootstrap(
            client,
            std::make_shared<TTestScheduler>());
        auto* session = bootstrap->GetSession();
        UNIT_ASSERT(!HasError(session->MountVolume().GetValue()));
        auto unmount = session->UnmountVolume();
        auto mount = session->MountVolume();
        bootstrap.reset();
        UNIT_ASSERT(!unmount.HasValue());
        UNIT_ASSERT(!mount.HasValue());
        completed.SetValue({});
        UNIT_ASSERT(unmount.HasValue());
        UNIT_ASSERT(mount.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            unmount.GetValue().GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            mount.GetValue().GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldMountVolume)
    {
        auto client = std::make_shared<TTestService>();

        client->MountVolumeHandler =
            [] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

                NProto::TMountVolumeResponse response;
                response.SetSessionId("1");

                auto& volume = *response.MutableVolume();
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(DefaultBlockSize);
                volume.SetBlocksCount(DefaultBlocksCount);

                return MakeFuture(response);
            };

        client->UnmountVolumeHandler =
            [] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetSessionId(), "1");
                return MakeFuture<NProto::TUnmountVolumeResponse>();
            };

        auto bootstrap = CreateBootstrap(client);

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        {
            auto res = session->UnmountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        bootstrap->Stop();
    }

    static NProto::TReadBlocksLocalResponse ReadBlocks(
        ISession* session,
        TVector<TString>& blocks,
        ui32 blockSize = DefaultBlockSize,
        ui32 startIndex = 0,
        ui64 blocksCount = 1)
    {
        auto sglist = ResizeBlocks(
            blocks,
            blocksCount,
            TString::TUninitialized(blockSize));

        auto request = std::make_shared<NProto::TReadBlocksLocalRequest>();
        request->SetStartIndex(startIndex);
        request->SetBlocksCount(blocksCount);
        request->SetCheckpointId(TString());
        request->SetBlockSize(blockSize);
        request->Sglist = TGuardedSgList(std::move(sglist));

        return session->ReadBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request)
        ).GetValueSync();
    }

    static NProto::TReadBlocksLocalResponse ReadBlocks(
        ISession* session,
        ui32 blockSize = DefaultBlockSize,
        ui32 startIndex = 0,
        ui64 blocksCount = 1)
    {
        TVector<TString> blocks;
        return ReadBlocks(session, blocks, blockSize, startIndex, blocksCount);
    }

    static NProto::TWriteBlocksLocalResponse WriteBlocks(
        ISession* session,
        ui32 blockSize = DefaultBlockSize,
        ui32 startIndex = 0,
        ui64 blocksCount = 1,
        ui8 fill = 1)
    {
        TVector<TString> vec;
        auto sglist = ResizeBlocks(vec, blocksCount, TString(blockSize, fill));

        auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        request->SetStartIndex(startIndex);
        request->BlocksCount = blocksCount;
        request->SetBlockSize(blockSize);
        request->Sglist = TGuardedSgList(std::move(sglist));

        return session->WriteBlocksLocal(
            MakeIntrusive<TCallContext>(),
            std::move(request)
        ).GetValueSync();
    }

    static NProto::TZeroBlocksResponse ZeroBlocks(
        ISession* session,
        ui32 startIndex = 0,
        ui64 blocksCount = 1)
    {
        auto request = std::make_shared<NProto::TZeroBlocksRequest>();
        request->SetStartIndex(startIndex);
        request->SetBlocksCount(blocksCount);

        return session->ZeroBlocks(
            MakeIntrusive<TCallContext>(),
            std::move(request)
        ).GetValueSync();
    }

    static void ReadWriteBlocksTestCommon(ui32 blockSize)
    {
        auto client = std::make_shared<TTestService>();

        client->MountVolumeHandler =
            [=] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

                NProto::TMountVolumeResponse response;
                response.SetSessionId("1");

                auto& volume = *response.MutableVolume();
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(blockSize);
                volume.SetBlocksCount(1024);

                return MakeFuture(response);
            };

        client->UnmountVolumeHandler =
            [] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetSessionId(), "1");
                return MakeFuture<NProto::TUnmountVolumeResponse>();
            };

        client->ReadBlocksLocalHandler =
            [] (std::shared_ptr<NProto::TReadBlocksLocalRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetSessionId(), "1");

                // simulate zero response
                auto guard = request->Sglist.Acquire();
                UNIT_ASSERT(guard);
                const auto& sglist = guard.Get();
                for (size_t i = 0; i < request->GetBlocksCount(); ++i) {
                    memset(const_cast<char*>(sglist[i].Data()), 0, sglist[i].Size());
                }

                return MakeFuture(NProto::TReadBlocksLocalResponse());
            };

        client->WriteBlocksLocalHandler =
            [] (std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetSessionId(), "1");
                return MakeFuture<NProto::TWriteBlocksLocalResponse>();
            };

        client->ZeroBlocksHandler =
            [] (std::shared_ptr<NProto::TZeroBlocksRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetSessionId(), "1");
                return MakeFuture<NProto::TZeroBlocksResponse>();
            };

        auto bootstrap = CreateBootstrap(client);

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        {
            auto res = ReadBlocks(session, blockSize);
            UNIT_ASSERT_C(!HasError(res), res);
        }

        {
            auto res = WriteBlocks(session, blockSize);
            UNIT_ASSERT_C(!HasError(res), res);
        }

        {
            auto res = ZeroBlocks(session);
            UNIT_ASSERT_C(!HasError(res), res);
        }

        {
            auto res = session->UnmountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldReadWriteBlocks)
    {
        ReadWriteBlocksTestCommon(DefaultBlockSize);
    }

    // NBS-209
    Y_UNIT_TEST(ShouldReadWriteNonDefaultBlockSize)
    {
        UNIT_ASSERT(DefaultBlockSize != 512); // Fix me if default block size changes
        ReadWriteBlocksTestCommon(512);
    }

    Y_UNIT_TEST(ShouldRemountVolumeIfClientReportsInvalidSession)
    {
        auto client = std::make_shared<TTestService>();

        size_t sessionNum = 0;
        client->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

                NProto::TMountVolumeResponse response;
                response.SetSessionId(ToString(++sessionNum));

                auto& volume = *response.MutableVolume();
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(DefaultBlockSize);
                volume.SetBlocksCount(DefaultBlocksCount);

                return MakeFuture(response);
            };

        client->UnmountVolumeHandler =
            [] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetSessionId(), "2");

                return MakeFuture<NProto::TUnmountVolumeResponse>();
            };

        client->WriteBlocksLocalHandler =
            [] (std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                if (request->GetSessionId() == "1") {
                    return MakeFuture<NProto::TWriteBlocksLocalResponse>(
                        TErrorResponse(E_BS_INVALID_SESSION));
                }

                return MakeFuture<NProto::TWriteBlocksLocalResponse>();
            };

        auto bootstrap = CreateBootstrap(client);

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
            UNIT_ASSERT_EQUAL(sessionNum, 1);
        }

        {
            auto res = WriteBlocks(session);
            UNIT_ASSERT_C(!HasError(res), res);
            UNIT_ASSERT_EQUAL(sessionNum, 2);
        }

        {
            auto res = session->UnmountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldRemountPeriodicallyToPreventUnmountDueToInactivity)
    {
        auto client = std::make_shared<TTestService>();

        auto timeout = TDuration::MilliSeconds(100);

        size_t sessionNum = 0;
        client->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

                NProto::TMountVolumeResponse response;
                response.SetSessionId(ToString(++sessionNum));
                response.SetInactiveClientsTimeout(timeout.MilliSeconds());

                auto& volume = *response.MutableVolume();
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(DefaultBlockSize);
                volume.SetBlocksCount(DefaultBlocksCount);

                return MakeFuture(response);
            };

        client->UnmountVolumeHandler =
            [] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TUnmountVolumeResponse>();
            };

        auto scheduler = std::make_shared<TTestScheduler>();
        auto bootstrap = CreateBootstrap(client, scheduler);

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
            UNIT_ASSERT_EQUAL(sessionNum, 1);
        }

        scheduler->RunAllScheduledTasks();

        // Run again to ensure remount causes scheduling of another remount
        scheduler->RunAllScheduledTasks();

        {
            auto res = session->UnmountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        bootstrap->Stop();

        UNIT_ASSERT_C(
            sessionNum == 3,
            TStringBuilder()
                << "Expected session number 3, got " << sessionNum);
    }

    Y_UNIT_TEST(ShouldNotFailRemountIfBlocksCountChanges)
    {
        auto client = std::make_shared<TTestService>();

        size_t sessionNum = 0;
        client->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

                NProto::TMountVolumeResponse response;
                response.SetSessionId(ToString(++sessionNum));

                auto& volume = *response.MutableVolume();
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(DefaultBlockSize);
                volume.SetBlocksCount(sessionNum*DefaultBlocksCount);

                return MakeFuture(response);
            };

        client->UnmountVolumeHandler =
            [] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TUnmountVolumeResponse>();
            };

        auto bootstrap = CreateBootstrap(client, {});

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
            UNIT_ASSERT_EQUAL(sessionNum, 1);
        }

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
            UNIT_ASSERT_EQUAL(sessionNum, 2);
        }

        {
            auto res = session->UnmountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldAllowMountForMountedVolume)
    {
        auto client = std::make_shared<TTestService>();

        size_t sessionNum = 0;
        client->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

                NProto::TMountVolumeResponse response;
                response.SetSessionId(ToString(++sessionNum));

                auto& volume = *response.MutableVolume();
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(DefaultBlockSize);
                volume.SetBlocksCount(DefaultBlocksCount);

                return MakeFuture(response);
            };

        client->UnmountVolumeHandler =
            [] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TUnmountVolumeResponse>();
            };

        auto bootstrap = CreateBootstrap(client, {});

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
            UNIT_ASSERT_EQUAL(sessionNum, 1);
        }

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
            UNIT_ASSERT_EQUAL(sessionNum, 2);
        }

        {
            auto res = session->UnmountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldStopAutomaticRemountAfterUnmount)
    {
        auto client = std::make_shared<TTestService>();

        auto timeout = TDuration::Seconds(1);

        size_t sessionNum = 0;
        client->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

                NProto::TMountVolumeResponse response;
                response.SetSessionId(ToString(++sessionNum));
                response.SetInactiveClientsTimeout(timeout.MilliSeconds());

                auto& volume = *response.MutableVolume();
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(4*1024);
                volume.SetBlocksCount(1024);

                return MakeFuture(response);
            };

        client->UnmountVolumeHandler =
            [] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TUnmountVolumeResponse>();
            };

        auto scheduler = std::make_shared<TTestScheduler>();
        auto bootstrap = CreateBootstrap(client, scheduler);

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
            UNIT_ASSERT_EQUAL(sessionNum, 1);
        }

        scheduler->RunAllScheduledTasks();

        {
            auto res = session->UnmountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        UNIT_ASSERT(sessionNum > 1);
        size_t sessionNumRightAfterUnmount = sessionNum;

        scheduler->RunAllScheduledTasks();

        UNIT_ASSERT(sessionNum == sessionNumRightAfterUnmount);

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldCorrectlyReportErrorsAfterRemount)
    {
        auto client = std::make_shared<TTestService>();

        size_t sessionNum = 0;
        client->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

                NProto::TMountVolumeResponse response;
                response.SetSessionId(ToString(++sessionNum));

                auto& volume = *response.MutableVolume();
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(4*1024);
                volume.SetBlocksCount(1024);

                return MakeFuture(response);
            };

        client->UnmountVolumeHandler =
            [] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetSessionId(), "2");

                return MakeFuture<NProto::TUnmountVolumeResponse>();
            };

        client->WriteBlocksLocalHandler =
            [] (std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                if (request->GetSessionId() == "1") {
                    return MakeFuture<NProto::TWriteBlocksLocalResponse>(
                        TErrorResponse(E_BS_INVALID_SESSION));
                } else {
                    return MakeFuture<NProto::TWriteBlocksLocalResponse>(
                        TErrorResponse(E_ARGUMENT));
                }

                return MakeFuture<NProto::TWriteBlocksLocalResponse>();
            };

        auto bootstrap = CreateBootstrap(client);

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
            UNIT_ASSERT_EQUAL(sessionNum, 1);
        }

        {
            auto res = WriteBlocks(session);
            UNIT_ASSERT(res.GetError().GetCode() == E_ARGUMENT);
            UNIT_ASSERT_EQUAL(sessionNum, 2);
        }

        {
            auto res = session->UnmountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldRejectReadWriteZeroBlocksRequestsForUnmountedVolume)
    {
        auto client = std::make_shared<TTestService>();

        client->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

                NProto::TMountVolumeResponse response;

                auto& volume = *response.MutableVolume();
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(4*1024);
                volume.SetBlocksCount(1024);

                return MakeFuture(response);
            };

        client->UnmountVolumeHandler =
            [] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                return MakeFuture<NProto::TUnmountVolumeResponse>();
            };

        client->ReadBlocksLocalHandler =
            [] (std::shared_ptr<NProto::TReadBlocksLocalRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);

                // simulate zero response
                auto guard = request->Sglist.Acquire();
                UNIT_ASSERT(guard);
                const auto& sglist = guard.Get();
                for (size_t i = 0; i < request->GetBlocksCount(); ++i) {
                    memset(const_cast<char*>(sglist[i].Data()), 0, sglist[i].Size());
                }

                return MakeFuture(NProto::TReadBlocksLocalResponse());
            };

        client->WriteBlocksLocalHandler =
            [] (std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                return MakeFuture<NProto::TWriteBlocksLocalResponse>();
            };

        client->ZeroBlocksHandler =
            [] (std::shared_ptr<NProto::TZeroBlocksRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                return MakeFuture<NProto::TZeroBlocksResponse>();
            };

        auto bootstrap = CreateBootstrap(client);

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        // Should return errors for not yet mounted volume
        {
            auto res = ReadBlocks(session);
            UNIT_ASSERT(res.GetError().GetCode() == E_INVALID_STATE);
        }

        {
            auto res = WriteBlocks(session);
            UNIT_ASSERT(res.GetError().GetCode() == E_INVALID_STATE);
        }

        {
            auto res = ZeroBlocks(session);
            UNIT_ASSERT(res.GetError().GetCode() == E_INVALID_STATE);
        }

        // Same should work for unmounted volume
        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        {
            auto res = session->UnmountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        {
            auto res = ReadBlocks(session);
            UNIT_ASSERT(res.GetError().GetCode() == E_INVALID_STATE);
        }

        {
            auto res = WriteBlocks(session);
            UNIT_ASSERT(res.GetError().GetCode() == E_INVALID_STATE);
        }

        {
            auto res = ZeroBlocks(session);
            UNIT_ASSERT(res.GetError().GetCode() == E_INVALID_STATE);
        }

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldUseOriginalMountRequestHeadersForRemounts)
    {
        auto client = std::make_shared<TTestService>();

        size_t sessionNum = 0;
        TString idempotenceId = "test_idempotence_id";
        TString traceId = "test_trace_id";

        client->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);
                UNIT_ASSERT_EQUAL(
                    request->GetHeaders().GetIdempotenceId(),
                    idempotenceId);
                UNIT_ASSERT_EQUAL(
                    request->GetHeaders().GetTraceId(),
                    traceId);

                NProto::TMountVolumeResponse response;
                response.SetSessionId(ToString(++sessionNum));

                auto& volume = *response.MutableVolume();
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(DefaultBlockSize);
                volume.SetBlocksCount(DefaultBlocksCount);

                return MakeFuture(response);
            };

        client->UnmountVolumeHandler =
            [] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetSessionId(), "2");

                return MakeFuture<NProto::TUnmountVolumeResponse>();
            };

        client->WriteBlocksLocalHandler =
            [] (std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                if (request->GetSessionId() == "1") {
                    return MakeFuture<NProto::TWriteBlocksLocalResponse>(
                        TErrorResponse(E_BS_INVALID_SESSION));
                }

                return MakeFuture<NProto::TWriteBlocksLocalResponse>();
            };

        auto bootstrap = CreateBootstrap(client);

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        {
            NProto::THeaders headers;
            headers.SetIdempotenceId(idempotenceId);
            headers.SetTraceId(traceId);

            auto ctx = MakeIntrusive<TCallContext>();
            auto res = session->MountVolume(ctx, headers).GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
            UNIT_ASSERT_EQUAL(sessionNum, 1);
        }

        {
            auto res = WriteBlocks(session);
            UNIT_ASSERT_C(!HasError(res), res);
            UNIT_ASSERT_EQUAL(sessionNum, 2);
        }

        {
            auto res = session->UnmountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        bootstrap->Stop();
    }

    // NBS-420
    Y_UNIT_TEST(ShouldHaveProperNumberOfBlocksInWriteBlocksWhenRemountOccurs)
    {
        auto client = std::make_shared<TTestService>();

        size_t sessionNum = 0;
        client->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

                NProto::TMountVolumeResponse response;
                response.SetSessionId(ToString(++sessionNum));

                auto& volume = *response.MutableVolume();
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(DefaultBlockSize);
                volume.SetBlocksCount(DefaultBlocksCount);

                return MakeFuture(response);
            };

        client->UnmountVolumeHandler =
            [] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetSessionId(), "5");

                return MakeFuture<NProto::TUnmountVolumeResponse>();
            };

        client->WriteBlocksLocalHandler =
            [] (std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT(request->GetBlocks().GetBuffers().empty());
                auto guard = request->Sglist.Acquire();
                UNIT_ASSERT(guard);
                UNIT_ASSERT_VALUES_EQUAL(guard.Get().size(), 1);

                // simulate dangerous behaviour in grpc client
                auto& buffers = *request->MutableBlocks()->MutableBuffers();
                buffers.Add();

                if (request->GetSessionId() != "5") {
                    return MakeFuture<NProto::TWriteBlocksLocalResponse>(
                        TErrorResponse(E_BS_INVALID_SESSION));
                }

                return MakeFuture<NProto::TWriteBlocksLocalResponse>();
            };

        auto bootstrap = CreateBootstrap(client);

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
            UNIT_ASSERT_EQUAL(sessionNum, 1);
        }

        {
            auto res = WriteBlocks(session);
            UNIT_ASSERT_C(!HasError(res), res);
            UNIT_ASSERT_EQUAL(sessionNum, 5);
        }

        {
            auto res = session->UnmountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldReadZerosFromUninitializedDataWhenRemountOccurs)
    {
        TString sessionId = "1";
        THashMap<ui64, TString> blocks;
        TCounters counters;
        auto client = CreateClientForConsistencyCheck(sessionId, blocks, counters);

        auto bootstrap = CreateBootstrap(client);

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res.GetError().GetMessage());
        }

        {
            TVector<TString> blocks;
            ui32 startIndex = 0;
            ui64 blocksCount = 5;

            // Trigger E_BS_INVALID_SESSION and remount
            sessionId = "2";

            auto res = ReadBlocks(
                session,
                blocks,
                DefaultBlockSize,
                startIndex,
                blocksCount);
            UNIT_ASSERT_C(!HasError(res), res.GetError().GetMessage());
            UNIT_ASSERT_EQUAL(blocks.size(), blocksCount);
            TString expected(DefaultBlockSize, 0);
            for (ui64 i = 0; i < blocksCount; ++i) {
                UNIT_ASSERT_EQUAL(blocks[i].size(), DefaultBlockSize);
                UNIT_ASSERT_EQUAL(blocks[i], expected);
            }
        }

        {
            auto res = session->UnmountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res.GetError().GetMessage());
        }

        bootstrap->Stop();

        UNIT_ASSERT_EQUAL(counters.MountRequestsCounter, 2); // 1 mount + 1 remount
        UNIT_ASSERT_EQUAL(counters.ReadBlocksRequestsCounter, 2); // 1 request + 1 remount
        UNIT_ASSERT_EQUAL(counters.WriteBlocksRequestsCounter, 0);
        UNIT_ASSERT_EQUAL(counters.ZeroBlocksRequestsCounter, 0);
        UNIT_ASSERT_EQUAL(counters.UnmountRequestsCounter, 1);
    }

    Y_UNIT_TEST(ShouldWriteBlocksConsistentlyWhenRemountOccurs)
    {
        TString sessionId = "1";
        THashMap<ui64, TString> blocks;
        TCounters counters;
        auto client = CreateClientForConsistencyCheck(sessionId, blocks, counters);

        auto bootstrap = CreateBootstrap(client);

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res.GetError().GetMessage());
        }

        {
            ui32 startIndex = 0;
            const ui64 blocksCount = 5;
            std::array<ui8, blocksCount> fills{ 1, 2, 3, 4, 5 };

            // Write blocks with different contents
            for (ui64 i = 0; i < blocksCount; ++i) {
                if (i % 2 == 0) {
                    // Trigger E_BS_INVALID_SESSION and remount
                    sessionId = CreateGuidAsString();
                }

                auto res = WriteBlocks(
                    session,
                    DefaultBlockSize,
                    startIndex + i,
                    1, // blocks count
                    fills[i]);
                UNIT_ASSERT_C(!HasError(res), res.GetError().GetMessage());
            }

            // Read written blocks back, verify consistency
            TVector<TString> blocks;
            auto res = ReadBlocks(
                session,
                blocks,
                DefaultBlockSize,
                startIndex,
                blocksCount);
            UNIT_ASSERT_C(!HasError(res), res.GetError().GetMessage());
            UNIT_ASSERT_EQUAL(blocks.size(), blocksCount);

            for (ui64 i = 0; i < blocksCount; ++i) {
                UNIT_ASSERT_EQUAL(blocks[i].size(), DefaultBlockSize);
                TString expected(DefaultBlockSize, fills[i]);
                UNIT_ASSERT_EQUAL(blocks[i], expected);
            }
        }

        {
            auto res = session->UnmountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res.GetError().GetMessage());
        }

        bootstrap->Stop();

        UNIT_ASSERT_EQUAL(counters.MountRequestsCounter, 4); // 1 mount + 3 remounts
        UNIT_ASSERT_EQUAL(counters.ReadBlocksRequestsCounter, 1);
        UNIT_ASSERT_EQUAL(counters.WriteBlocksRequestsCounter, 8); // 5 requests + 3 remounts
        UNIT_ASSERT_EQUAL(counters.ZeroBlocksRequestsCounter, 0);
        UNIT_ASSERT_EQUAL(counters.UnmountRequestsCounter, 1);
    }

    Y_UNIT_TEST(ShouldZeroBlocksConsistentlyWhenRemountOccurs)
    {
        TString sessionId = "1";
        THashMap<ui64, TString> blocks;
        TCounters counters;
        auto client = CreateClientForConsistencyCheck(sessionId, blocks, counters);

        auto bootstrap = CreateBootstrap(client);

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res.GetError().GetMessage());
        }

        {
            ui32 startIndex = 0;
            const ui64 blocksCount = 5;
            std::array<ui8, blocksCount> fills{ 1, 2, 3, 4, 5 };

            // Write blocks with different contents
            for (ui64 i = 0; i < blocksCount; ++i) {
                auto res = WriteBlocks(
                    session,
                    DefaultBlockSize,
                    startIndex + i,
                    1, // blocks count
                    fills[i]);
                UNIT_ASSERT_C(!HasError(res), res.GetError().GetMessage());
            }

            // Zero some blocks
            for (ui64 i = 1; i < (blocksCount - 1); ++i) {
                if (i % 2 == 1) {
                    // Trigger E_BS_INVALID_SESSION and remount
                    sessionId = CreateGuidAsString();
                }

                auto res = ZeroBlocks(session, startIndex + i, 1);
                UNIT_ASSERT_C(!HasError(res), res.GetError().GetMessage());
            }

            // Read written and partially zeroed blocks back, verify consistency
            TVector<TString> blocks;
            auto res = ReadBlocks(
                session,
                blocks,
                DefaultBlockSize,
                startIndex,
                blocksCount);
            UNIT_ASSERT_C(!HasError(res), res.GetError().GetMessage());
            UNIT_ASSERT_EQUAL(blocks.size(), blocksCount);

            for (ui64 i = 0; i < blocksCount; ++i) {
                UNIT_ASSERT_EQUAL(blocks[i].size(), DefaultBlockSize);
                ui8 fill = 0;
                if (i == 0 || i == blocksCount-1) {
                    fill = fills[i];
                }
                TString expected(DefaultBlockSize, fill);
                UNIT_ASSERT_EQUAL(blocks[i], expected);
            }
        }

        {
            auto res = session->UnmountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res.GetError().GetMessage());
        }

        bootstrap->Stop();

        UNIT_ASSERT_EQUAL(counters.MountRequestsCounter, 3); // 1 mount + 2 remounts
        UNIT_ASSERT_EQUAL(counters.ReadBlocksRequestsCounter, 1);
        UNIT_ASSERT_EQUAL(counters.WriteBlocksRequestsCounter, 5); // 5 requests
        UNIT_ASSERT_EQUAL(counters.ZeroBlocksRequestsCounter, 5); // 3 requests + 2 remounts
        UNIT_ASSERT_EQUAL(counters.UnmountRequestsCounter, 1);
    }

    Y_UNIT_TEST(ShouldPropagateClientVersionInfoWhenMountingVolume)
    {
        TString clientVersionInfo = "trunk@9999999";

        auto client = std::make_shared<TTestService>();
        client->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);
                UNIT_ASSERT_EQUAL(request->GetClientVersionInfo(), clientVersionInfo);

                NProto::TMountVolumeResponse response;
                response.SetSessionId(CreateGuidAsString());

                auto& volume = *response.MutableVolume();
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(DefaultBlockSize);
                volume.SetBlocksCount(DefaultBlocksCount);

                return MakeFuture(response);
            };

        client->UnmountVolumeHandler =
            [] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);

                return MakeFuture<NProto::TUnmountVolumeResponse>();
            };

        auto bootstrap = CreateBootstrap(client, {}, clientVersionInfo);

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        {
            auto res = session->UnmountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldPostponeUnmountIfPeriodicalRemountIsInProgress)
    {
        auto client = std::make_shared<TTestService>();

        auto timeout = TDuration::MilliSeconds(100);

        size_t mountNum = 0;
        auto remountResponse = NewPromise<NProto::TMountVolumeResponse>();
        client->MountVolumeHandler =
            [&](std::shared_ptr <NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

                NProto::TMountVolumeResponse response;
                response.SetSessionId("session");
                response.SetInactiveClientsTimeout(timeout.MilliSeconds());

                auto& volume = *response.MutableVolume();
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(DefaultBlockSize);
                volume.SetBlocksCount(DefaultBlocksCount);

                if (!mountNum++) {
                    return MakeFuture(response);
                } else {
                    return remountResponse.GetFuture();
                }
            };

        client->UnmountVolumeHandler =
            [](std::shared_ptr <NProto::TUnmountVolumeRequest> request) {
                Y_UNUSED(request);
                return MakeFuture<NProto::TUnmountVolumeResponse>();
            };

        auto scheduler = std::make_shared<TTestScheduler>();
        auto bootstrap = CreateBootstrap(client, scheduler);

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
            UNIT_ASSERT_EQUAL(mountNum, 1);
        }

        scheduler->RunAllScheduledTasks();

        {
            auto unmountResponse = session->UnmountVolume();

            NProto::TMountVolumeResponse response;
            response.SetSessionId("session");
            response.SetInactiveClientsTimeout(timeout.MilliSeconds());

            auto& volume = *response.MutableVolume();
            volume.SetDiskId(DefaultDiskId);
            volume.SetBlockSize(DefaultBlockSize);
            volume.SetBlocksCount(DefaultBlocksCount);
            remountResponse.SetValue(std::move(response));

            auto res = unmountResponse.GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldPostponeMountIfPeriodicalRemountIsInProgress)
    {
        auto client = std::make_shared<TTestService>();

        auto timeout = TDuration::MilliSeconds(100);

        size_t mountNum = 0;
        auto remountResponse = NewPromise<NProto::TMountVolumeResponse>();
        client->MountVolumeHandler =
            [&](std::shared_ptr <NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

                NProto::TMountVolumeResponse response;
                response.SetSessionId("session");
                response.SetInactiveClientsTimeout(timeout.MilliSeconds());

                auto& volume = *response.MutableVolume();
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(DefaultBlockSize);
                volume.SetBlocksCount(DefaultBlocksCount);

                if (!mountNum++) {
                    return MakeFuture(response);
                } else {
                    return remountResponse.GetFuture();
                }
            };

        auto scheduler = std::make_shared<TTestScheduler>();
        auto bootstrap = CreateBootstrap(client, scheduler);

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
            UNIT_ASSERT_EQUAL(mountNum, 1);
        }

        scheduler->RunAllScheduledTasks();

        {
            auto mountResponse = session->MountVolume();

            NProto::TMountVolumeResponse response;
            response.SetSessionId("session");
            response.SetInactiveClientsTimeout(timeout.MilliSeconds());

            auto& volume = *response.MutableVolume();
            volume.SetDiskId(DefaultDiskId);
            volume.SetBlockSize(DefaultBlockSize);
            volume.SetBlocksCount(DefaultBlocksCount);
            remountResponse.SetValue(std::move(response));

            auto res = mountResponse.GetValueSync();
            UNIT_ASSERT_EQUAL(mountNum, 3);
        }

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldPostponeUnmountIfUnmountIsInProgress)
    {
        auto client = std::make_shared<TTestService>();

        auto timeout = TDuration::MilliSeconds(100);

        client->MountVolumeHandler =
            [&](std::shared_ptr <NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

                NProto::TMountVolumeResponse response;
                response.SetSessionId("session");
                response.SetInactiveClientsTimeout(timeout.MilliSeconds());

                auto& volume = *response.MutableVolume();
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(DefaultBlockSize);
                volume.SetBlocksCount(DefaultBlocksCount);

                return MakeFuture(response);
            };

        size_t unmountNum = 0;
        auto unmountResponse = NewPromise<NProto::TUnmountVolumeResponse>();
        client->UnmountVolumeHandler =
            [&](std::shared_ptr <NProto::TUnmountVolumeRequest> request) {
                Y_UNUSED(request);
                if (!unmountNum++) {
                    return unmountResponse.GetFuture();
                } else {
                    return MakeFuture<NProto::TUnmountVolumeResponse>();
                }
            };


        auto scheduler = std::make_shared<TTestScheduler>();
        auto bootstrap = CreateBootstrap(client, scheduler);

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        scheduler->RunAllScheduledTasks();

        {
            session->UnmountVolume();
            auto unmountResponsePostPosponed = session->UnmountVolume();

            NProto::TUnmountVolumeResponse response;
            unmountResponse.SetValue(std::move(response));

            auto res = unmountResponsePostPosponed.GetValueSync();
            UNIT_ASSERT(res.GetError().GetCode() == S_ALREADY);
        }

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldPostponeMountIfUnmountIsInProgress)
    {
        auto client = std::make_shared<TTestService>();

        auto timeout = TDuration::MilliSeconds(100);

        client->MountVolumeHandler =
            [&](std::shared_ptr <NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);

                NProto::TMountVolumeResponse response;
                response.SetSessionId("session");
                response.SetInactiveClientsTimeout(timeout.MilliSeconds());

                auto& volume = *response.MutableVolume();
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(DefaultBlockSize);
                volume.SetBlocksCount(DefaultBlocksCount);

                return MakeFuture(response);
            };

        size_t unmountNum = 0;
        auto unmountResponse = NewPromise<NProto::TUnmountVolumeResponse>();
        client->UnmountVolumeHandler =
            [&](std::shared_ptr <NProto::TUnmountVolumeRequest> request) {
                Y_UNUSED(request);
                if (!unmountNum++) {
                    return unmountResponse.GetFuture();
                } else {
                    return MakeFuture<NProto::TUnmountVolumeResponse>();
                }
            };


        auto scheduler = std::make_shared<TTestScheduler>();
        auto bootstrap = CreateBootstrap(client, scheduler);

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        scheduler->RunAllScheduledTasks();

        {
            session->UnmountVolume();
            auto mountResponsePostPosponed = session->MountVolume();

            NProto::TUnmountVolumeResponse response;
            unmountResponse.SetValue(std::move(response));

            auto res = mountResponsePostPosponed.GetValueSync();
            UNIT_ASSERT(res.GetError().GetCode() == S_OK);
        }

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldPassMountSeqNumberInMountVolume)
    {
        constexpr ui64 mountSeqNumber = 4;
        auto client = std::make_shared<TTestService>();

        client->MountVolumeHandler =
            [] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetToken(), DefaultMountToken);
                UNIT_ASSERT_EQUAL(request->GetMountSeqNumber(), mountSeqNumber);

                NProto::TMountVolumeResponse response;
                response.SetSessionId("1");

                auto& volume = *response.MutableVolume();
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(DefaultBlockSize);
                volume.SetBlocksCount(DefaultBlocksCount);

                return MakeFuture(response);
            };

        client->UnmountVolumeHandler =
            [] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                UNIT_ASSERT_EQUAL(request->GetDiskId(), DefaultDiskId);
                UNIT_ASSERT_EQUAL(request->GetSessionId(), "1");
                return MakeFuture<NProto::TUnmountVolumeResponse>();
            };

        auto bootstrap = CreateBootstrap(client, {}, {}, mountSeqNumber);

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        {
            auto res = session->UnmountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res);
        }

        bootstrap->Stop();
    }

    Y_UNIT_TEST(ShouldHandleFrozenRemountRequestAfterSessionDestruction)
    {
        auto promise = NewPromise<void>();
        promise.SetValue();

        size_t mountCount = 0;

        auto client = std::make_shared<TTestService>();

        client->MountVolumeHandler =
            [&] (std::shared_ptr<NProto::TMountVolumeRequest> request) {
                ++mountCount;

                NProto::TMountVolumeResponse response;
                response.SetSessionId("1");
                response.SetInactiveClientsTimeout(100);

                auto& volume = *response.MutableVolume();
                volume.SetDiskId(request->GetDiskId());
                volume.SetBlockSize(DefaultBlockSize);
                volume.SetBlocksCount(DefaultBlocksCount);

                return promise.GetFuture().Apply([=] (const auto&) {
                    return response;
                });
            };

        client->UnmountVolumeHandler =
            [&] (std::shared_ptr<NProto::TUnmountVolumeRequest> request) {
                Y_UNUSED(request);
                return promise.GetFuture().Apply([=] (const auto&) {
                    return NProto::TUnmountVolumeResponse();
                });
            };

        {
            auto scheduler = std::make_shared<TTestScheduler>();
            auto bootstrap = CreateBootstrap(client, scheduler);

            auto session = bootstrap->GetSession();

            bootstrap->Start();

            {
                auto res = session->MountVolume().GetValueSync();
                UNIT_ASSERT_C(!HasError(res), res);
            }

            // Freeze request handlers
            promise = NewPromise<void>();

            UNIT_ASSERT_VALUES_EQUAL(1, mountCount);
            scheduler->RunAllScheduledTasks();
            UNIT_ASSERT_VALUES_EQUAL(2, mountCount);
            scheduler->RunAllScheduledTasks();
            scheduler->RunAllScheduledTasks();
            scheduler->RunAllScheduledTasks();
            UNIT_ASSERT_VALUES_EQUAL(2, mountCount);

            bootstrap->Stop();
        }

        // Unfreeze request handlers
        promise.SetValue();
    }

    Y_UNIT_TEST(ShouldPassErrorFlags)
    {
        auto client = std::make_shared<TTestService>();

        size_t sessionNum = 0;
        client->MountVolumeHandler =
            [&](std::shared_ptr<NProto::TMountVolumeRequest> request)
        {
            NProto::TMountVolumeResponse response;
            response.SetSessionId(ToString(++sessionNum));

            auto& volume = *response.MutableVolume();
            volume.SetDiskId(request->GetDiskId());
            volume.SetBlockSize(4 * 1024);
            volume.SetBlocksCount(1024);

            return MakeFuture(response);
        };

        client->UnmountVolumeHandler =
            [](std::shared_ptr<NProto::TUnmountVolumeRequest> request)
        {
            Y_UNUSED(request);
            return MakeFuture<NProto::TUnmountVolumeResponse>();
        };

        client->WriteBlocksLocalHandler =
            [](std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
        {
            Y_UNUSED(request);

            NProto::TError result = MakeError(
                E_IO_SILENT,
                "IO error",
                NCloud::NProto::EF_HW_PROBLEMS_DETECTED);
            return MakeFuture<NProto::TWriteBlocksLocalResponse>(
                TErrorResponse(std::move(result)));
        };

        auto bootstrap = CreateBootstrap(client);

        auto session = bootstrap->GetSession();

        bootstrap->Start();

        {
            auto res = session->MountVolume().GetValueSync();
            UNIT_ASSERT_C(!HasError(res), res.GetError().GetMessage());
        }

        {
            auto res = WriteBlocks(session);
            UNIT_ASSERT(res.GetError().GetCode() == E_IO_SILENT);
            UNIT_ASSERT(
                res.GetError().GetFlags() ==
                NCloud::NProto::EF_HW_PROBLEMS_DETECTED);
        }

        bootstrap->Stop();
    }
}

}   // namespace NCloud::NBlockStore::NClient
