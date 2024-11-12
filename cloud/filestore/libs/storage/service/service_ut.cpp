#include "service.h"
#include "service_private.h"

#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/model/utils.h>
#include <cloud/filestore/libs/storage/testlib/service_client.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>
#include <cloud/filestore/private/api/protos/actions.pb.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

#include <contrib/ydb/core/base/hive.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestProfileLog
    : public IProfileLog
{
public:
    TMap<ui32, TVector<TRecord>> Requests;

    void Start() override
    {}

    void Stop() override
    {}

    void Write(TRecord record) override
    {
        UNIT_ASSERT(record.Request.HasRequestType());
        Requests[record.Request.GetRequestType()].push_back(std::move(record));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStorageServiceTest)
{
    Y_UNIT_TEST(ShouldCreateFileStore)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);

        auto response = service.GetFileStoreInfo("test")->Record.GetFileStore();
        UNIT_ASSERT_VALUES_EQUAL("test", response.GetFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL("test", response.GetCloudId());
        UNIT_ASSERT_VALUES_EQUAL("test", response.GetFolderId());
        UNIT_ASSERT_VALUES_EQUAL(1'000, response.GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, response.GetBlockSize());
        UNIT_ASSERT_VALUES_EQUAL(1, response.GetConfigVersion());

        const auto& profile = response.GetPerformanceProfile();
        UNIT_ASSERT(!profile.GetThrottlingEnabled());
        UNIT_ASSERT_VALUES_EQUAL(100, profile.GetMaxReadIops());
        UNIT_ASSERT_VALUES_EQUAL(300, profile.GetMaxWriteIops());
        UNIT_ASSERT_VALUES_EQUAL(30_MB, profile.GetMaxReadBandwidth());
        UNIT_ASSERT_VALUES_EQUAL(30_MB, profile.GetMaxWriteBandwidth());
        UNIT_ASSERT_VALUES_EQUAL(128_MB, profile.GetMaxPostponedWeight());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(20).MilliSeconds(),
            profile.GetMaxPostponedTime());
        UNIT_ASSERT_VALUES_EQUAL(1024, profile.GetMaxPostponedCount());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Minutes(30).MilliSeconds(),
            profile.GetBoostTime());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Hours(12).MilliSeconds(),
            profile.GetBoostRefillTime());
        UNIT_ASSERT_VALUES_EQUAL(400, profile.GetBoostPercentage());
        UNIT_ASSERT_VALUES_EQUAL(10, profile.GetBurstPercentage());
        UNIT_ASSERT_VALUES_EQUAL(20, profile.GetMaxWriteCostMultiplier());
        UNIT_ASSERT_VALUES_EQUAL(
            4_KB,
            profile.GetDefaultPostponedRequestWeight());

        service.DestroyFileStore("test");
        service.AssertGetFileStoreInfoFailed("test");
    }

    Y_UNIT_TEST(ShouldAlterFileStore)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);
        service.AlterFileStore("test", "yyyy", "zzzz");

        auto response = service.GetFileStoreInfo("test")->Record.GetFileStore();
        UNIT_ASSERT_VALUES_EQUAL("test", response.GetFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL("yyyy", response.GetCloudId());
        UNIT_ASSERT_VALUES_EQUAL("zzzz", response.GetFolderId());
        UNIT_ASSERT_VALUES_EQUAL(1'000, response.GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, response.GetBlockSize());
        UNIT_ASSERT_VALUES_EQUAL(2, response.GetConfigVersion());

        const auto& profile = response.GetPerformanceProfile();
        UNIT_ASSERT(!profile.GetThrottlingEnabled());
        UNIT_ASSERT_VALUES_EQUAL(100, profile.GetMaxReadIops());
        UNIT_ASSERT_VALUES_EQUAL(300, profile.GetMaxWriteIops());
        UNIT_ASSERT_VALUES_EQUAL(30_MB, profile.GetMaxReadBandwidth());
        UNIT_ASSERT_VALUES_EQUAL(30_MB, profile.GetMaxWriteBandwidth());
        UNIT_ASSERT_VALUES_EQUAL(128_MB, profile.GetMaxPostponedWeight());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(20).MilliSeconds(),
            profile.GetMaxPostponedTime());
        UNIT_ASSERT_VALUES_EQUAL(1024, profile.GetMaxPostponedCount());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Minutes(30).MilliSeconds(),
            profile.GetBoostTime());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Hours(12).MilliSeconds(),
            profile.GetBoostRefillTime());
        UNIT_ASSERT_VALUES_EQUAL(400, profile.GetBoostPercentage());
        UNIT_ASSERT_VALUES_EQUAL(10, profile.GetBurstPercentage());
        UNIT_ASSERT_VALUES_EQUAL(20, profile.GetMaxWriteCostMultiplier());
        UNIT_ASSERT_VALUES_EQUAL(
            4_KB,
            profile.GetDefaultPostponedRequestWeight());
    }

    Y_UNIT_TEST(ShouldResizeFileStore)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);
        service.ResizeFileStore("test", 100'000'000);

        auto response = service.GetFileStoreInfo("test")->Record.GetFileStore();
        UNIT_ASSERT_VALUES_EQUAL("test", response.GetFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL("test", response.GetCloudId());
        UNIT_ASSERT_VALUES_EQUAL("test", response.GetFolderId());
        UNIT_ASSERT_VALUES_EQUAL(100'000'000, response.GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, response.GetBlockSize());
        UNIT_ASSERT_VALUES_EQUAL(2, response.GetConfigVersion());

        const auto& profile = response.GetPerformanceProfile();
        UNIT_ASSERT(!profile.GetThrottlingEnabled());
        UNIT_ASSERT_VALUES_EQUAL(200, profile.GetMaxReadIops());
        UNIT_ASSERT_VALUES_EQUAL(600, profile.GetMaxWriteIops());
        UNIT_ASSERT_VALUES_EQUAL(60_MB, profile.GetMaxReadBandwidth());
        UNIT_ASSERT_VALUES_EQUAL(60_MB, profile.GetMaxWriteBandwidth());
        UNIT_ASSERT_VALUES_EQUAL(128_MB, profile.GetMaxPostponedWeight());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(20).MilliSeconds(),
            profile.GetMaxPostponedTime());
        UNIT_ASSERT_VALUES_EQUAL(1024, profile.GetMaxPostponedCount());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Minutes(30).MilliSeconds(),
            profile.GetBoostTime());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Hours(12).MilliSeconds(),
            profile.GetBoostRefillTime());
        UNIT_ASSERT_VALUES_EQUAL(200, profile.GetBoostPercentage());
        UNIT_ASSERT_VALUES_EQUAL(10, profile.GetBurstPercentage());
        UNIT_ASSERT_VALUES_EQUAL(20, profile.GetMaxWriteCostMultiplier());
        UNIT_ASSERT_VALUES_EQUAL(
            4_KB,
            profile.GetDefaultPostponedRequestWeight());

        service.AssertResizeFileStoreFailed("test", 1'000);
        service.AssertResizeFileStoreFailed("test", 0);
    }

    Y_UNIT_TEST(ShouldDownsizeFileStore)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 100'000'000);

        {
            service.SendResizeFileStoreRequest("test", 10'000'000);
            auto response = service.RecvResizeFileStoreResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetStatus(),
                response->GetErrorReason());

            service.SendResizeFileStoreRequest("test", 10'000'000, true);
            response = service.RecvResizeFileStoreResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }

        auto response = service.GetFileStoreInfo("test")->Record.GetFileStore();
        UNIT_ASSERT_VALUES_EQUAL("test", response.GetFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL("test", response.GetCloudId());
        UNIT_ASSERT_VALUES_EQUAL("test", response.GetFolderId());
        UNIT_ASSERT_VALUES_EQUAL(100'000'00, response.GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, response.GetBlockSize());
        UNIT_ASSERT_VALUES_EQUAL(2, response.GetConfigVersion());

        const auto& profile = response.GetPerformanceProfile();
        UNIT_ASSERT(!profile.GetThrottlingEnabled());
        UNIT_ASSERT_VALUES_EQUAL(200, profile.GetMaxReadIops());
        UNIT_ASSERT_VALUES_EQUAL(600, profile.GetMaxWriteIops());
        UNIT_ASSERT_VALUES_EQUAL(60_MB, profile.GetMaxReadBandwidth());
        UNIT_ASSERT_VALUES_EQUAL(60_MB, profile.GetMaxWriteBandwidth());
        UNIT_ASSERT_VALUES_EQUAL(128_MB, profile.GetMaxPostponedWeight());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(20).MilliSeconds(),
            profile.GetMaxPostponedTime());
        UNIT_ASSERT_VALUES_EQUAL(1024, profile.GetMaxPostponedCount());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Minutes(30).MilliSeconds(),
            profile.GetBoostTime());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Hours(12).MilliSeconds(),
            profile.GetBoostRefillTime());
        UNIT_ASSERT_VALUES_EQUAL(400, profile.GetBoostPercentage());
        UNIT_ASSERT_VALUES_EQUAL(10, profile.GetBurstPercentage());
        UNIT_ASSERT_VALUES_EQUAL(20, profile.GetMaxWriteCostMultiplier());
        UNIT_ASSERT_VALUES_EQUAL(
            4_KB,
            profile.GetDefaultPostponedRequestWeight());
    }

    Y_UNIT_TEST(ShouldResizeFileStoreWithCustomPerformanceProfile)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const char* fsId = "test";
        const auto initialBlockCount = 1'000;
        const auto blockCount = 100'000'000;
        const auto customMaxReadIops = 111;
        const auto customMaxWriteIops = 222;
        service.CreateFileStore("test", initialBlockCount);
        auto resizeRequest = service.CreateResizeFileStoreRequest(
            "test",
            blockCount);
        resizeRequest->Record.MutablePerformanceProfile()->SetMaxReadIops(
            customMaxReadIops);
        service.SendRequest(MakeStorageServiceId(), std::move(resizeRequest));
        auto resizeResponse = service.RecvResizeFileStoreResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            resizeResponse->GetStatus(),
            resizeResponse->GetErrorReason());

        auto response = service.GetFileStoreInfo(fsId)->Record.GetFileStore();
        UNIT_ASSERT_VALUES_EQUAL(fsId, response.GetFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL(blockCount, response.GetBlocksCount());

        auto profile = response.GetPerformanceProfile();
        UNIT_ASSERT(!profile.GetThrottlingEnabled());
        // autocalculated
        UNIT_ASSERT_VALUES_EQUAL(600, profile.GetMaxWriteIops());
        // custom
        UNIT_ASSERT_VALUES_EQUAL(customMaxReadIops, profile.GetMaxReadIops());

        resizeRequest = service.CreateResizeFileStoreRequest(
            "test",
            blockCount);
        resizeRequest->Record.MutablePerformanceProfile()->SetMaxWriteIops(
            customMaxWriteIops);

        service.SendRequest(MakeStorageServiceId(), std::move(resizeRequest));
        resizeResponse = service.RecvResizeFileStoreResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            resizeResponse->GetStatus(),
            resizeResponse->GetErrorReason());

        response = service.GetFileStoreInfo(fsId)->Record.GetFileStore();
        UNIT_ASSERT_VALUES_EQUAL(fsId, response.GetFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL(blockCount, response.GetBlocksCount());

        profile = response.GetPerformanceProfile();
        UNIT_ASSERT(!profile.GetThrottlingEnabled());
        // custom
        UNIT_ASSERT_VALUES_EQUAL(customMaxWriteIops, profile.GetMaxWriteIops());
        // autocalculated
        UNIT_ASSERT_VALUES_EQUAL(200, profile.GetMaxReadIops());
    }

    Y_UNIT_TEST(ShouldResizeFileStoreAndAddChannels)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");
        ui32 nodeIdx = env.CreateNode("nfs");

        auto& runtime = env.GetRuntime();

        ui32 createChannelsCount = 0;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvStorageSSProxy::EvModifySchemeRequest: {
                        auto* msg = event->Get<TEvStorageSSProxy::TEvModifySchemeRequest>();
                        if (msg->ModifyScheme.GetOperationType() ==
                            NKikimrSchemeOp::ESchemeOpCreateFileStore)
                        {
                            const auto& request = msg->ModifyScheme.GetCreateFileStore();
                            const auto& config = request.GetConfig();
                            createChannelsCount = config.ExplicitChannelProfilesSize();
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);
        UNIT_ASSERT(createChannelsCount > 0);

        ui32 alterChannelsCount = 0;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvStorageSSProxy::EvModifySchemeRequest: {
                        auto* msg = event->Get<TEvStorageSSProxy::TEvModifySchemeRequest>();
                        if (msg->ModifyScheme.GetOperationType() ==
                            NKikimrSchemeOp::ESchemeOpAlterFileStore)
                        {
                            const auto& request = msg->ModifyScheme.GetAlterFileStore();
                            const auto& config = request.GetConfig();
                            alterChannelsCount = config.ExplicitChannelProfilesSize();
                        }
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        service.ResizeFileStore("test", 4_TB / DefaultBlockSize);
        UNIT_ASSERT(alterChannelsCount > 0);
        UNIT_ASSERT(alterChannelsCount > createChannelsCount);
    }

    Y_UNIT_TEST(ShouldFailAlterIfDescribeFails)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");
        ui32 nodeIdx = env.CreateNode("nfs");

        auto& runtime = env.GetRuntime();

        TServiceClient service(runtime, nodeIdx);
        service.CreateFileStore("test", 1000);

        auto error = MakeError(E_ARGUMENT, "Error");
        runtime.SetObserverFunc( [nodeIdx, error, &runtime] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeFileStoreRequest: {
                        auto response = std::make_unique<TEvSSProxy::TEvDescribeFileStoreResponse>(
                            error);
                        runtime.Send(
                            new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                response.release(),
                                0, // flags
                                event->Cookie),
                            nodeIdx);
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        service.AssertAlterFileStoreFailed("test", "xxxx", "yyyy");
    }

    Y_UNIT_TEST(ShouldDescribeModel)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const auto size1 = 1_GB/DefaultBlockSize;

        auto response1 = service.DescribeFileStoreModel(size1);
        auto& model1 = response1->Record.GetFileStoreModel();
        UNIT_ASSERT_VALUES_EQUAL(model1.GetBlocksCount(), size1);
        UNIT_ASSERT_VALUES_EQUAL(model1.GetBlockSize(), DefaultBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(model1.GetChannelsCount(), 7);

        auto& profile1 = model1.GetPerformanceProfile();
        UNIT_ASSERT(!profile1.GetThrottlingEnabled());
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetMaxReadIops(), 100);
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetMaxWriteIops(), 300);
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetMaxReadBandwidth(), 30_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetMaxWriteBandwidth(), 30_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetMaxPostponedWeight(), 128_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetMaxPostponedTime(), TDuration::Seconds(20).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetMaxPostponedCount(), 1024);
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetBoostTime(), TDuration::Minutes(30).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetBoostRefillTime(), TDuration::Hours(12).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetBoostPercentage(), 400);
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetBurstPercentage(), 10);
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetMaxWriteCostMultiplier(), 20);
        UNIT_ASSERT_VALUES_EQUAL(profile1.GetDefaultPostponedRequestWeight(), 4_KB);

        const auto size2 = 4_TB/DefaultBlockSize;
        auto response2 = service.DescribeFileStoreModel(size2);
        auto& model2 = response2->Record.GetFileStoreModel();
        UNIT_ASSERT_VALUES_EQUAL(model2.GetBlocksCount(), size2);
        UNIT_ASSERT_VALUES_EQUAL(model2.GetBlockSize(), DefaultBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(model2.GetChannelsCount(), 19);

        auto& profile2 = model2.GetPerformanceProfile();
        UNIT_ASSERT(!profile2.GetThrottlingEnabled());
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetMaxReadIops(), 300);
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetMaxWriteIops(), 4800);
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetMaxReadBandwidth(), 240_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetMaxWriteBandwidth(), 240_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetMaxPostponedWeight(), 128_MB);
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetMaxPostponedTime(), TDuration::Seconds(20).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetMaxPostponedCount(), 1024);
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetBoostTime(), TDuration::Minutes(30).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetBoostRefillTime(), TDuration::Hours(12).MilliSeconds());
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetBoostPercentage(), 25);
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetBurstPercentage(), 10);
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetMaxWriteCostMultiplier(), 20);
        UNIT_ASSERT_VALUES_EQUAL(profile2.GetDefaultPostponedRequestWeight(), 4_KB);


        service.AssertDescribeFileStoreModelFailed(0);
        service.AssertDescribeFileStoreModelFailed(1000, 0);
    }

    Y_UNIT_TEST(ShouldCreateSession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto headers = service.InitSession("test", "client");

        service.PingSession(headers);
        service.DestroySession(headers);
    }

    Y_UNIT_TEST(ShouldReturnFileStoreInfoWhenCreateSession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000, DefaultBlockSize, NProto::EStorageMediaKind::STORAGE_MEDIA_SSD);

        auto response = service.CreateSession(THeaders{"test", "client", ""});

        UNIT_ASSERT(response->Record.HasFileStore());
        UNIT_ASSERT_EQUAL(
            NProto::EStorageMediaKind::STORAGE_MEDIA_SSD,
            static_cast<NProto::EStorageMediaKind>(response->Record.GetFileStore().GetStorageMediaKind()));
    }

    Y_UNIT_TEST(ShouldRestoreSessionIfPipeFailed)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto& runtime = env.GetRuntime();

        bool fail = true;
        TActorId worker;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (!fail) {
                    return TTestActorRuntime::DefaultObserverFunc(event);
                }

                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeFileStoreRequest: {
                        worker = event->Sender;
                        break;
                    }
                    case TEvTabletPipe::EvClientConnected: {
                        if (fail && worker && event->Recipient == worker) {
                            auto* msg = event->Get<TEvTabletPipe::TEvClientConnected>();
                            const_cast<NKikimrProto::EReplyStatus&>(msg->Status) = NKikimrProto::ERROR;
                        } else {
                            fail = false;
                        }
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        THeaders headers = {"test", "client", ""};
        service.CreateSession(headers);
    }

    Y_UNIT_TEST(ShouldRestoreSessionIfPipeDisconnected)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto& runtime = env.GetRuntime();

        TActorId worker;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeFileStoreRequest: {
                        worker = event->Sender;
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        THeaders headers = {"test", "client", ""};
        service.CreateSession(headers);

        auto msg = std::make_unique<TEvTabletPipe::TEvClientDestroyed>(
            static_cast<ui64>(0),
            TActorId(),
            TActorId());

        runtime.Send(
            new IEventHandle(
                // send back
                worker,
                TActorId(),
                msg.release(),
                0, // flags
                0),
            nodeIdx);

        TDispatchOptions options;
        options.FinalEvents = {
            TDispatchOptions::TFinalEventCondition(
                TEvIndexTablet::EvCreateSessionRequest)
        };
        env.GetRuntime().DispatchEvents(options, TDuration::Seconds(1));
    }

    Y_UNIT_TEST(ShouldRestoreSessionIfCreateFailed)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto& runtime = env.GetRuntime();

        bool fail = true;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (!fail) {
                    return TTestActorRuntime::DefaultObserverFunc(event);
                }

                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvCreateSessionRequest: {
                        fail = false;
                        auto response = std::make_unique<TEvIndexTablet::TEvCreateSessionResponse>(
                            MakeError(E_REJECTED, "xxx"));

                        runtime.Send(
                            new IEventHandle(
                                // send back
                                event->Sender,
                                event->Sender,
                                response.release(),
                                0, // flags
                                event->Cookie),
                            nodeIdx);

                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        THeaders headers = {"test", "client", ""};
        service.AssertCreateSessionFailed(headers);
        service.CreateSession(headers);
    }

    Y_UNIT_TEST(ShouldFailIfCreateSessionFailed)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto& runtime = env.GetRuntime();

        bool fail = true;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                if (!fail) {
                    return TTestActorRuntime::DefaultObserverFunc(event);
                }

                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvCreateSessionRequest: {
                        fail = false;
                        auto response = std::make_unique<TEvIndexTablet::TEvCreateSessionResponse>(
                            MakeError(E_REJECTED, "xxx"));

                        runtime.Send(
                            new IEventHandle(
                                // send back
                                event->Sender,
                                event->Sender,
                                response.release(),
                                0, // flags
                                event->Cookie),
                            nodeIdx);

                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        THeaders headers = {"test", "client", ""};
        service.AssertCreateSessionFailed(headers);
        service.CreateSession(headers);
    }

    Y_UNIT_TEST(ShouldCleanUpIfSessionFailed)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        auto& runtime = env.GetRuntime();

        ui64 tabletId = -1;
        TActorId session;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) mutable {
            switch (event->GetTypeRewrite()) {
                case TEvSSProxy::EvDescribeFileStoreResponse: {
                    const auto* msg = event->Get<TEvSSProxy::TEvDescribeFileStoreResponse>();
                    const auto& desc = msg->PathDescription.GetFileStoreDescription();
                    tabletId = desc.GetIndexTabletId();

                    return TTestActorRuntime::EEventAction::PROCESS;
                }
                case TEvIndexTablet::EvCreateSessionRequest: {
                    session = event->Sender;
                    return TTestActorRuntime::EEventAction::PROCESS;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto headers = service.InitSession("test", "client");
        UNIT_ASSERT(headers.SessionId);
        UNIT_ASSERT(tabletId != -1llu);
        UNIT_ASSERT(session);

        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

        // explicitly fail session actor, proper way is to catch
        // IndexTablet::CreateSession request via observer func and respond w error
        // but for some reason runtime doesn't catch this event during tablet restart
        // though it actually happens and session resotres by the end of restart
        runtime.Send(
            new IEventHandle(
                // send back
                session,
                session,
                new TEvents::TEvPoisonPill(),
                0, // flags
                0),
            nodeIdx);

        TIndexTabletClient tablet(runtime, nodeIdx, tabletId);
        tablet.RebootTablet();

        auto response = service.AssertCreateNodeFailed(
            headers,
            TCreateNodeArgs::File(RootNodeId, "aaa"));

        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetCode(), (ui32)E_FS_INVALID_SESSION);

        service.CreateSession(headers);
        service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "aaa"));
    }

    Y_UNIT_TEST(ShouldRestoreClientSession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto headers = service.InitSession(
            "test",
            "client",
            "",         // checkpointId
            true        // restoreClientSession
        );
        UNIT_ASSERT_VALUES_UNEQUAL("", headers.SessionId);

        auto headers2 = service.InitSession(
            "test",
            "client",
            "",         // checkpointId
            true        // restoreClientSession
        );
        UNIT_ASSERT_VALUES_EQUAL(headers.SessionId, headers2.SessionId);

        auto headers3 = service.InitSession(
            "test",
            "client",
            "",         // checkpointId
            false       // restoreClientSession
        );
        UNIT_ASSERT_VALUES_UNEQUAL(headers.SessionId, headers3.SessionId);

        service.DestroySession(headers);
    }

    Y_UNIT_TEST(ShouldNotPingAndDestroyInvalidSession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto headers = service.InitSession("test", "client");

        THeaders invalidFileSystem = {"xxx", "client", headers.SessionId};
        THeaders invalidClient = {"test", "invalid client", headers.SessionId};
        THeaders invalidSession = {"test", "client", "invalid session"};

        // FIXME
        // service.AssertPingSessionFailed(invalidFileSystem);
        service.AssertPingSessionFailed(invalidClient);
        service.AssertPingSessionFailed(invalidSession);

        service.AssertDestroySessionFailed(invalidFileSystem);
        service.AssertDestroySessionFailed(invalidClient);
        // fail safe
        service.DestroySession(invalidSession);
    }

    Y_UNIT_TEST(ShouldForwardRequests)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto headers = service.InitSession("test", "client");

        auto request = service.CreateCreateNodeRequest(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file"));

        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        service.SendCreateNodeRequest(std::move(request));

        auto response = service.RecvCreateNodeResponse();
        UNIT_ASSERT(response);
        UNIT_ASSERT_C(SUCCEEDED(response->GetStatus()), response->GetErrorReason().c_str());
    }

    Y_UNIT_TEST(ShouldNotForwardRequestsWithInvalidSession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto headers = service.InitSession("test", "client");

        THeaders invalidFileSystem = {"xxx", "client", headers.SessionId};
        THeaders invalidClient = {"test", "invalid client", headers.SessionId};
        THeaders invalidSession = {"test", "client", "invalid session"};

        auto nodeArgs = TCreateNodeArgs::File(RootNodeId, "file");

        service.AssertCreateNodeFailed(invalidFileSystem, nodeArgs);
        service.AssertCreateNodeFailed(invalidClient, nodeArgs);
        service.AssertCreateNodeFailed(invalidSession, nodeArgs);

        // sanity check
        service.CreateNode(headers, nodeArgs);
    }

    Y_UNIT_TEST(ShouldGetSessionEvents)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto headers = service.InitSession("test", "client");

        service.SubscribeSession(headers);
        service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"));

        auto response = service.GetSessionEvents(headers);

        const auto& events = response->Record.GetEvents();
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
    }

    Y_UNIT_TEST(ShouldGetSessionEventsStream)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto headers = service.InitSession("test", "client");

        service.SubscribeSession(headers);
        service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file1"));

        {
            auto response = service.GetSessionEventsStream(headers);

            const auto& events = response->Record.GetEvents();
            UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
        }

        service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file2"));

        {
            auto response = service.RecvResponse<TEvService::TEvGetSessionEventsResponse>();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                response->GetErrorReason());

            const auto& events = response->Record.GetEvents();
            UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
        }
    }

    Y_UNIT_TEST(ShouldListFileStores)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        TServiceClient service(env.GetRuntime(), nodeIdx);

        TVector<TString> expected = {"dir/fs1", "dir/fs2", "dir1/fs", "dir2/fs"};
        for (const auto& id : expected) {
            service.CreateFileStore(id, 1000);
        }

        auto response = service.ListFileStores();
        const auto& proto = response->Record.GetFileStores();

        TVector<TString> filestores;
        Copy(proto.begin(), proto.end(), std::back_inserter(filestores));
        Sort(filestores);

        UNIT_ASSERT_VALUES_EQUAL(filestores, expected);

        auto counters = env.GetCounters()
            ->FindSubgroup("component", "service")
            ->FindSubgroup("request", "ListFileStores");
        counters->OutputPlainText(Cerr);
        UNIT_ASSERT_EQUAL(1, counters->GetCounter("Count")->GetAtomic());
        UNIT_ASSERT_EQUAL(0, counters->GetCounter("InProgress")->GetAtomic());
    }

    Y_UNIT_TEST(ShouldFailListFileStoresIfDescribeSchemeFails)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("fs1", 10000);
        service.CreateFileStore("fs2", 10000);

        auto error = MakeError(E_ARGUMENT, "Error");

        auto& runtime = env.GetRuntime();
        runtime.SetObserverFunc( [nodeIdx, error, &runtime] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvStorageSSProxy::EvDescribeSchemeRequest: {
                        auto response = std::make_unique<TEvStorageSSProxy::TEvDescribeSchemeResponse>(
                            error);
                        runtime.Send(
                            new IEventHandle(
                                event->Sender,
                                event->Recipient,
                                response.release(),
                                0, // flags
                                event->Cookie),
                            nodeIdx);
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto response = service.AssertListFileStoresFailed();
        UNIT_ASSERT(response->GetStatus() == error.GetCode());
        UNIT_ASSERT(response->GetErrorReason() == error.GetMessage());
    }

    Y_UNIT_TEST(ShouldProfileRequests)
    {
        const auto profileLog = std::make_shared<TTestProfileLog>();
        TTestEnv env({}, {}, {}, profileLog);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);

        profileLog->Start();

        service.CreateFileStore("test", 1'000);
        UNIT_ASSERT_VALUES_EQUAL(0, profileLog->Requests.size());

        service.AlterFileStore("test", "yyyy", "zzzz");
        UNIT_ASSERT_VALUES_EQUAL(0, profileLog->Requests.size());

        service.ResizeFileStore("test", 100'000'000);
        UNIT_ASSERT_VALUES_EQUAL(0, profileLog->Requests.size());

        service.DescribeFileStoreModel(1_GB/DefaultBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(0, profileLog->Requests.size());

        service.ListFileStores();
        UNIT_ASSERT_VALUES_EQUAL(0, profileLog->Requests.size());

        auto headers = service.InitSession("test", "client");
        UNIT_ASSERT_VALUES_EQUAL(0, profileLog->Requests.size());

        service.PingSession(headers);
        UNIT_ASSERT_VALUES_EQUAL(0, profileLog->Requests.size());

        service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"));
        UNIT_ASSERT_VALUES_EQUAL(1, profileLog->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            profileLog->Requests[static_cast<ui32>(EFileStoreRequest::CreateNode)].size());

        service.ListNodes(headers, 1);
        UNIT_ASSERT_VALUES_EQUAL(2, profileLog->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            profileLog->Requests[static_cast<ui32>(EFileStoreRequest::ListNodes)].size());

        service.DestroySession(headers);
        UNIT_ASSERT_VALUES_EQUAL(2, profileLog->Requests.size());

        service.DestroyFileStore("test");
        UNIT_ASSERT_VALUES_EQUAL(2, profileLog->Requests.size());

        profileLog->Stop();
    }

    Y_UNIT_TEST(ShouldSupportInterHostMigration)
    {
        TTestEnvConfig cfg;
        cfg.StaticNodes = 1;
        cfg.DynamicNodes = 2;
        TTestEnv env(cfg);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx1 = env.CreateNode("nfs");
        ui32 nodeIdx2 = env.CreateNode("nfs");

        TServiceClient service1(env.GetRuntime(), nodeIdx1);
        TServiceClient service2(env.GetRuntime(), nodeIdx2);

        service1.CreateFileStore("test", 1'000);
        auto headers1 = service1.InitSession("test", "client");
        service1.PingSession(headers1);

        service1.CreateNode(headers1, TCreateNodeArgs::File(RootNodeId, "file"));
        service1.ListNodes(headers1, 1);

        auto headers2 = service2.InitSession("test", "client", "", false, 1, true);
        service2.PingSession(headers2);

        headers2 = service2.InitSession("test", "client", "", true, 1);
        service2.PingSession(headers2);

        service2.CreateNode(headers2, TCreateNodeArgs::File(RootNodeId, "file2"));
        service2.ListNodes(headers2, 1);

        service1.DestroySession(headers1);

        service2.CreateNode(headers2, TCreateNodeArgs::File(RootNodeId, "file3"));
        service2.DestroySession(headers2);
    }

    Y_UNIT_TEST(ShouldSupportIntraHostMigration)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateFileStore("test", 1'000);
        auto headers1 = service.InitSession("test", "client");
        service.PingSession(headers1);

        service.CreateNode(headers1, TCreateNodeArgs::File(RootNodeId, "file"));
        service.ListNodes(headers1, 1);

        auto headers2 = service.InitSession("test", "client", "", true, 1, true);
        service.PingSession(headers2);

        headers2 = service.InitSession("test", "client", "", true, 1);
        service.PingSession(headers2);

        service.CreateNode(headers2, TCreateNodeArgs::File(RootNodeId, "file2"));
        service.ListNodes(headers2, 1);

        service.DestroySession(headers1);

        service.CreateNode(headers2, TCreateNodeArgs::File(RootNodeId, "file3"));
    }

    Y_UNIT_TEST(ShouldProperlyDeleteSubsessions)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateFileStore("test", 1'000);

        auto headers1 = service.InitSession("test", "client");
        service.PingSession(headers1);
        service.CreateNode(headers1, TCreateNodeArgs::File(RootNodeId, "file"));
        service.ListNodes(headers1, 1);

        auto headers2 = service.InitSession("test", "client", "", true, 1, true);
        service.PingSession(headers2);

        service.DestroySession(headers1);

        service.CreateNode(headers2, TCreateNodeArgs::File(RootNodeId, "file3"));
    }

    Y_UNIT_TEST(ShouldProperlyDeleteCounters)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        auto counters = env.GetCounters();
        counters = counters->FindSubgroup("component", "service_fs");
        UNIT_ASSERT(counters);
        counters = counters->FindSubgroup("host", "cluster");
        UNIT_ASSERT(counters);

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateFileStore("test", 1'000);

        auto headers1 = service.InitSession("test", "client");
        service.PingSession(headers1);

        {
            auto counter = counters->FindSubgroup("filesystem", "test");
            UNIT_ASSERT(counter);

            counter = counter->FindSubgroup("client", "client");
            UNIT_ASSERT(counter);
        }

        auto headers2 = service.InitSession("test", "client", "", true, 1, true);
        service.PingSession(headers2);

        {
            auto counter = counters->FindSubgroup("filesystem", "test");
            UNIT_ASSERT(counter);

            counter = counter->FindSubgroup("client", "client");
            UNIT_ASSERT(counter);
        }

        service.DestroySession(headers1);

        {
            auto counter = counters->FindSubgroup("filesystem", "test");
            UNIT_ASSERT(counter);

            counter = counter->FindSubgroup("client", "client");
            UNIT_ASSERT(counter);
        }

        service.DestroySession(headers2);

        {
            auto counter = counters->FindSubgroup("filesystem", "test");
            UNIT_ASSERT(counter);

            counter = counter->FindSubgroup("client", "client");
            UNIT_ASSERT(!counter);
        }
    }

    Y_UNIT_TEST(ShouldRejectParallelCreateOrDestroyRequests)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto& runtime = env.GetRuntime();

        TActorId worker;
        TAutoPtr<IEventHandle> resp;
        bool fail = false;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvCreateSessionRequest: {
                        worker = event->Sender;
                        break;
                    }
                    case TEvIndexTablet::EvCreateSessionResponse: {
                        if (!resp && fail) {
                            resp = event;
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        THeaders headers = {"test", "client", ""};
        auto response = service.CreateSession(headers);
        auto sessionId = response->Record.GetSession().GetSessionId();
        headers.SessionId = sessionId;

        fail = true;
        service.SendCreateSessionRequest(headers);

        service.AssertDestroySessionFailed(headers);

        runtime.Send(resp.Release(), nodeIdx);

        service.DestroySession(headers);
    }

    Y_UNIT_TEST(ShouldNotDestroyWholeSessionIfSubSessionFailes)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto& runtime = env.GetRuntime();

        TActorId worker;
        bool fail = false;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvCreateSessionResponse: {
                        if (fail) {
                            auto response = std::make_unique<TEvIndexTablet::TEvCreateSessionResponse>(
                                MakeError(E_REJECTED, "xxx"));
                            fail = false;
                            runtime.Send(
                                new IEventHandle(
                                    // send back
                                    event->Recipient,
                                    event->Recipient,
                                    response.release(),
                                    0, // flags
                                    event->Cookie),
                                nodeIdx);

                            return TTestActorRuntime::EEventAction::DROP;
                        }
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        THeaders headers = {"test", "client", ""};
        auto response = service.CreateSession(headers);
        auto sessionId = response->Record.GetSession().GetSessionId();
        headers.SessionId = sessionId;

        service.PingSession(headers);

        fail = true;
        headers.SessionSeqNo = 1;
        service.AssertCreateSessionFailed(headers, "", true, 1);
        service.AssertPingSessionFailed(headers);

        headers.SessionSeqNo = 0;
        service.PingSession(headers);
    }

    Y_UNIT_TEST(ShouldUpdateSessionStateWhenRestoringSession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateFileStore("test", 1'000);
        auto headers = service.InitSession("test", "client");
        service.PingSession(headers);

        service.ResetSession(headers, "123");

        service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"));
        service.ListNodes(headers, 1);

        THeaders headers1;
        auto response1 = service.InitSession(headers1, "test", "client", "", true, 1, true);
        UNIT_ASSERT_VALUES_EQUAL(response1->Record.GetSession().GetSessionState(), "123");
        service.PingSession(headers1);

        THeaders headers2;
        auto response2 = service.InitSession(headers2, "test", "client", "", true, 1, false);
        UNIT_ASSERT_VALUES_EQUAL(response2->Record.GetSession().GetSessionState(), "123");
        service.PingSession(headers2);

        service.CreateNode(headers2, TCreateNodeArgs::File(RootNodeId, "file2"));
        service.ListNodes(headers2, 1);

        service.DestroySession(headers);

        service.CreateNode(headers2, TCreateNodeArgs::File(RootNodeId, "file3"));
    }

    Y_UNIT_TEST(ShouldGetStorageConfigValues)
    {
        NProto::TStorageConfig config;
        config.SetCompactionThreshold(1000);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);

        NProtoPrivate::TGetStorageConfigFieldsRequest request;
        request.SetFileSystemId("test");
        request.AddStorageConfigFields("Unknown");
        request.AddStorageConfigFields("SSDBoostTime");
        request.AddStorageConfigFields("CompactionThreshold");

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);
        auto jsonResponse = service.ExecuteAction("getstorageconfigfields", buf);
        NProtoPrivate::TGetStorageConfigFieldsResponse response;
        UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
            jsonResponse->Record.GetOutput(), &response).ok());

        const auto& storageValues = response.GetStorageConfigFieldsToValues();

        UNIT_ASSERT_VALUES_EQUAL(
            storageValues.at("SSDBoostTime"),
            "Default");
        UNIT_ASSERT_VALUES_EQUAL(
            storageValues.at("Unknown"),
            "Not found");
        UNIT_ASSERT_VALUES_EQUAL(
            storageValues.at("CompactionThreshold"),
            "1000");
    }

    NProtoPrivate::TChangeStorageConfigResponse ExecuteChangeStorageConfig(
        NProto::TStorageConfig config,
        TServiceClient& service,
        bool mergeWithConfig = false)
    {
        NProtoPrivate::TChangeStorageConfigRequest request;
        request.SetFileSystemId("test");

        *request.MutableStorageConfig() = std::move(config);
        request.SetMergeWithStorageConfigFromTabletDB(mergeWithConfig);

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);

        auto jsonResponse = service.ExecuteAction(
            "changestorageconfig", buf);
        NProtoPrivate::TChangeStorageConfigResponse response;
        UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
            jsonResponse->Record.GetOutput(), &response).ok());
        return response;
    }

    void CheckStorageConfigValues(
        TVector<TString> names,
        THashMap<TString, TString> answer,
        TServiceClient& service)
    {

        NProtoPrivate::TGetStorageConfigFieldsRequest request;
        request.SetFileSystemId("test");
        for (const auto& name: names) {
            request.AddStorageConfigFields(name);
        }

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);
        auto jsonResponse = service.ExecuteAction("getstorageconfigfields", buf);
        NProtoPrivate::TGetStorageConfigFieldsResponse response;
        UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
            jsonResponse->Record.GetOutput(), &response).ok());

        auto storageValues = response.GetStorageConfigFieldsToValues();

        for (const auto& [name, value] : answer) {
            UNIT_ASSERT_VALUES_EQUAL(storageValues[name], value);
        }
    }

    Y_UNIT_TEST(ShouldChangeStorageConfig)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);

        CheckStorageConfigValues(
            {"CleanupThresholdForBackpressure"},
            {{"CleanupThresholdForBackpressure", "Default"}},
            service);

        {
            // Check that new config was set
            NProto::TStorageConfig newConfig;
            newConfig.SetCleanupThresholdForBackpressure(5);
            const auto response = ExecuteChangeStorageConfig(
                std::move(newConfig), service);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetCleanupThresholdForBackpressure(),
                5);
            TDispatchOptions options;
            env.GetRuntime().DispatchEvents(options, TDuration::Seconds(2));
        }

        CheckStorageConfigValues(
            {"CleanupThresholdForBackpressure"},
            {{"CleanupThresholdForBackpressure", "5"}},
            service);

        {
            // Check that configs are merged, when
            // MergeWithStorageConfigFromTabletDB is true
            NProto::TStorageConfig newConfig;
            newConfig.SetCompactionThresholdForBackpressure(10);
            const auto response = ExecuteChangeStorageConfig(
                std::move(newConfig), service, true);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetCleanupThresholdForBackpressure(),
                5);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetCompactionThresholdForBackpressure(),
                10);
            TDispatchOptions options;
            env.GetRuntime().DispatchEvents(options, TDuration::Seconds(2));
        }

        CheckStorageConfigValues(
            {"CleanupThresholdForBackpressure",
            "CompactionThresholdForBackpressure"},
            {
                {"CleanupThresholdForBackpressure", "5"},
                {"CompactionThresholdForBackpressure", "10"}
            },
            service);

        {
            // Check that configs aren't merged, when
            // MergeWithStorageConfigFromTabletDB is false
            NProto::TStorageConfig newConfig;
            const auto response = ExecuteChangeStorageConfig(
                std::move(newConfig), service, false);
            UNIT_ASSERT(
                !response.GetStorageConfig().GetCleanupThresholdForBackpressure());
            UNIT_ASSERT(
                !response.GetStorageConfig().GetCompactionThresholdForBackpressure());
            TDispatchOptions options;
            env.GetRuntime().DispatchEvents(options, TDuration::Seconds(2));
        }

        CheckStorageConfigValues(
            {"CleanupThresholdForBackpressure", "CompactionThresholdForBackpressure"},
            {
                {"CleanupThresholdForBackpressure", "Default"},
                {"CompactionThresholdForBackpressure", "Default"}
            },
            service);
    }

    Y_UNIT_TEST(ShouldDescribeSessions)
    {
        NProto::TStorageConfig config;
        config.SetCompactionThreshold(1000);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        // delaying pipe creation response
        ui64 tabletId = -1;
        env.GetRuntime().SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeFileStoreResponse: {
                        using TResponse =
                            TEvSSProxy::TEvDescribeFileStoreResponse;
                        const auto* msg = event->Get<TResponse>();
                        const auto& desc =
                            msg->PathDescription.GetFileStoreDescription();
                        tabletId = desc.GetIndexTabletId();
                        break;
                    }
                }

                return false;
            });

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);

        THeaders headers = {"test", "client", "session", 3};
        service.CreateSession(
            headers,
            "", // checkpointId
            false, // restoreClientSession
            headers.SessionSeqNo);
        service.ResetSession(headers, "some_state");

        headers = {"test", "client2", "session2", 4};
        service.CreateSession(
            headers,
            "", // checkpointId
            false, // restoreClientSession
            headers.SessionSeqNo);
        service.ResetSession(headers, "some_state2");

        NProtoPrivate::TDescribeSessionsRequest request;
        request.SetFileSystemId("test");

        {
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("describesessions", buf);
            NProtoPrivate::TDescribeSessionsResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());

            const auto& sessions = response.GetSessions();
            UNIT_ASSERT_VALUES_EQUAL(2, sessions.size());

            UNIT_ASSERT_VALUES_EQUAL("session", sessions[0].GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL("client", sessions[0].GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(
                "some_state",
                sessions[0].GetSessionState());
            UNIT_ASSERT_VALUES_EQUAL(3, sessions[0].GetMaxSeqNo());
            UNIT_ASSERT_VALUES_EQUAL(3, sessions[0].GetMaxRwSeqNo());
            UNIT_ASSERT(!sessions[0].GetIsOrphan());

            UNIT_ASSERT_VALUES_EQUAL("session2", sessions[1].GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL("client2", sessions[1].GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(
                "some_state2",
                sessions[1].GetSessionState());
            UNIT_ASSERT_VALUES_EQUAL(4, sessions[1].GetMaxSeqNo());
            UNIT_ASSERT_VALUES_EQUAL(4, sessions[1].GetMaxRwSeqNo());
            UNIT_ASSERT(!sessions[1].GetIsOrphan());
        }

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        // rebooting tablet to destroy the pipe
        tablet.RebootTablet();

        {
            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("describesessions", buf);
            NProtoPrivate::TDescribeSessionsResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());

            const auto& sessions = response.GetSessions();
            UNIT_ASSERT_VALUES_EQUAL(2, sessions.size());

            UNIT_ASSERT_VALUES_EQUAL("session", sessions[0].GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL("client", sessions[0].GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(
                "some_state",
                sessions[0].GetSessionState());
            UNIT_ASSERT_VALUES_EQUAL(3, sessions[0].GetMaxSeqNo());
            UNIT_ASSERT_VALUES_EQUAL(3, sessions[0].GetMaxRwSeqNo());
            UNIT_ASSERT(sessions[0].GetIsOrphan());

            UNIT_ASSERT_VALUES_EQUAL("session2", sessions[1].GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL("client2", sessions[1].GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(
                "some_state2",
                sessions[1].GetSessionState());
            UNIT_ASSERT_VALUES_EQUAL(4, sessions[1].GetMaxSeqNo());
            UNIT_ASSERT_VALUES_EQUAL(4, sessions[1].GetMaxRwSeqNo());
            UNIT_ASSERT(sessions[1].GetIsOrphan());
        }
    }

    Y_UNIT_TEST(ShouldValidateBlockSize)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const ui32 blocks = 1024 * 1024;
        service.SendCreateFileStoreRequest("fs", blocks, 2_KB);

        auto response = service.RecvCreateFileStoreResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            response->GetStatus(),
            response->GetErrorReason());

        service.SendCreateFileStoreRequest("fs", blocks, 256_KB);

        response = service.RecvCreateFileStoreResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            response->GetStatus(),
            response->GetErrorReason());

        service.SendCreateFileStoreRequest("fs", blocks, 132_KB);

        response = service.RecvCreateFileStoreResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            response->GetStatus(),
            response->GetErrorReason());

        service.SendCreateFileStoreRequest("fs", blocks, 128_KB);

        response = service.RecvCreateFileStoreResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldProperlyProcessSlowPipeCreation)
    {
        NProto::TStorageConfig config;
        config.SetIdleSessionTimeout(5'000); // 5s
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        auto& runtime = env.GetRuntime();

        // enabling scheduling for all actors
        runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId) {
                Y_UNUSED(parentId);
                runtime.EnableScheduleForActor(actorId);
            });

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        THeaders headers = {"test", "client", "", 0};

        // delaying pipe creation response
        ui64 tabletId = -1;
        bool caughtClientConnected = false;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) mutable {
            switch (event->GetTypeRewrite()) {
                case TEvSSProxy::EvDescribeFileStoreResponse: {
                    const auto* msg = event->Get<TEvSSProxy::TEvDescribeFileStoreResponse>();
                    const auto& desc = msg->PathDescription.GetFileStoreDescription();
                    tabletId = desc.GetIndexTabletId();

                    return TTestActorRuntime::EEventAction::PROCESS;
                }
                case TEvTabletPipe::EvClientConnected: {
                    const auto* msg = event->Get<TEvTabletPipe::TEvClientConnected>();
                    if (msg->TabletId == tabletId) {
                        if (!caughtClientConnected) {
                            runtime.Schedule(event, TDuration::Seconds(10), nodeIdx);
                            caughtClientConnected = true;
                            return TTestActorRuntime::EEventAction::DROP;
                        }
                    }

                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        // creating session
        service.SendCreateSessionRequest(headers);
        auto response = service.RecvCreateSessionResponse();
        headers.SessionId = response->Record.GetSession().GetSessionId();
        // immediately pinging session to signal that it's not idle
        service.PingSession(headers);

        // just checking that we observed the events that we are expecting
        UNIT_ASSERT_VALUES_UNEQUAL(-1llu, tabletId);
        UNIT_ASSERT(caughtClientConnected);

        // no need to intercept those events anymore
        runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

        bool pipeRestored = false;
        runtime.SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvTabletPipe::EvClientConnected: {
                        const auto* msg =
                            event->Get<TEvTabletPipe::TEvClientConnected>();
                        if (msg->TabletId == tabletId) {
                            pipeRestored = true;
                        }

                        break;
                    }
                }

                return false;
            });

        TIndexTabletClient tablet(runtime, nodeIdx, tabletId);
        // rebooting tablet to destroy the pipe
        tablet.RebootTablet();

        // checking that pipe was reestablished successfully
        UNIT_ASSERT(pipeRestored);

        service.DestroySession(headers);
    }

    Y_UNIT_TEST(ShouldProperlyProcessSlowSessionCreation)
    {
        NProto::TStorageConfig config;
        config.SetIdleSessionTimeout(5'000); // 5s
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        auto& runtime = env.GetRuntime();

        // enabling scheduling for all actors
        runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId) {
                Y_UNUSED(parentId);
                runtime.EnableScheduleForActor(actorId);
            });

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        THeaders headers = {"test", "client", "", 0};

        // delaying session creation response
        bool rescheduled = false;
        ui32 createSessionResponses = 0;
        TActorId createSessionActor;

        runtime.SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeFileStoreResponse: {
                        createSessionActor = event->Recipient;

                        break;
                    }
                    case TEvIndexTablet::EvCreateSessionResponse: {
                        ++createSessionResponses;

                        if (!rescheduled) {
                            runtime.Schedule(event, TDuration::Seconds(10), nodeIdx);
                            rescheduled = true;
                            return true;
                        }

                        break;
                    }
                }

                return false;
            });

        // creating session
        service.SendCreateSessionRequest(headers);
        auto response = service.RecvCreateSessionResponse();
        headers.SessionId = response->Record.GetSession().GetSessionId();
        // immediately pinging session to signal that it's not idle
        service.PingSession(headers);

        // just checking that we observed the events that we are expecting
        UNIT_ASSERT(rescheduled);
        UNIT_ASSERT_VALUES_EQUAL(1, createSessionResponses);

        // can't call RebootTablet here because it resets our registration
        // observer and thus disables wakeup event scheduling
        auto msg = std::make_unique<TEvTabletPipe::TEvClientDestroyed>(
            static_cast<ui64>(0),
            TActorId(),
            TActorId());

        runtime.Send(
            new IEventHandle(
                createSessionActor,
                runtime.AllocateEdgeActor(nodeIdx),
                msg.release(),
                0, // flags
                0),
            nodeIdx);

        runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        runtime.DispatchEvents({}, TDuration::MilliSeconds(100));

        // checking that session was recreated
        UNIT_ASSERT_VALUES_EQUAL(2, createSessionResponses);

        service.DestroySession(headers);
    }

    Y_UNIT_TEST(UnsuccessfulSessionActorShouldStopWorking)
    {
        NProto::TStorageConfig config;
        config.SetIdleSessionTimeout(5'000); // 5s
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        auto& runtime = env.GetRuntime();

        // enabling scheduling for all actors
        runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId) {
                Y_UNUSED(parentId);
                runtime.EnableScheduleForActor(actorId);
            });

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        THeaders headers = {"test", "client", "", 0};

        ui32 sessionCreated = 0;
        bool rescheduled = false;

        runtime.SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvIndexTablet::EvCreateSessionResponse: {
                    if (!rescheduled) {
                        auto* msg = event->Get<TEvIndexTablet::TEvCreateSessionResponse>();
                        *msg->Record.MutableError() = MakeError(E_TIMEOUT, "timeout");

                        runtime.Schedule(event, TDuration::Seconds(10), nodeIdx);
                        rescheduled = true;
                        return true;
                    }

                    break;
                }

                case TEvServicePrivate::EvSessionCreated: {
                    ++sessionCreated;

                    break;
                }
            }

            return false;
        });

        // creating session
        service.SendCreateSessionRequest(headers);
        runtime.DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT(rescheduled);
        runtime.AdvanceCurrentTime(TDuration::Seconds(5));
        auto response = service.RecvCreateSessionResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_TIMEOUT,
            response->GetStatus(),
            response->GetErrorReason());

        runtime.AdvanceCurrentTime(TDuration::Seconds(5));
        runtime.DispatchEvents({}, TDuration::MilliSeconds(100));

        // we should have observed exactly 1 CreateSessionResponse
        // if we observe more than 1 it means that our CreateSessionActor
        // remained active after the first failure
        UNIT_ASSERT_VALUES_EQUAL(1, sessionCreated);

        // this time session creation should be successful
        service.SendCreateSessionRequest(headers);
        response = service.RecvCreateSessionResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetErrorReason());

        UNIT_ASSERT_VALUES_EQUAL(2, sessionCreated);
    }

    Y_UNIT_TEST(ShouldFillOriginFqdnWhenCreatingSession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto& runtime = env.GetRuntime();
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) mutable {
            switch (event->GetTypeRewrite()) {
                case TEvIndexTablet::EvCreateSessionRequest: {
                    const auto* msg =
                        event->Get<TEvIndexTablet::TEvCreateSessionRequest>();
                    UNIT_ASSERT_VALUES_UNEQUAL("", GetOriginFqdn(msg->Record));
                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        THeaders headers = {"test", "client", "", 0};
        service.CreateSession(headers);
    }

    void CheckTwoStageReads(NProto::EStorageMediaKind mediaKind, bool disableForHdd)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const TString fs = "test";
        service.CreateFileStore(fs, 1000, DefaultBlockSize, mediaKind);

        {
            NProto::TStorageConfig newConfig;
            newConfig.SetTwoStageReadEnabled(true);
            newConfig.SetTwoStageReadDisabledForHDD(disableForHdd);
            const auto response =
                ExecuteChangeStorageConfig(std::move(newConfig), service);
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                response.GetStorageConfig().GetTwoStageReadEnabled());
            UNIT_ASSERT_VALUES_EQUAL(
                disableForHdd,
                response.GetStorageConfig().GetTwoStageReadDisabledForHDD());

            TDispatchOptions options;
            env.GetRuntime().DispatchEvents(options, TDuration::Seconds(1));
        }

        auto headers = service.InitSession(fs, "client");

        ui64 nodeId =
            service
                .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
                ->Record.GetNode()
                .GetId();

        ui64 handle =
            service
                .CreateHandle(headers, fs, nodeId, "", TCreateHandleArgs::RDWR)
                ->Record.GetHandle();

        // fresh bytes
        auto data = TString(100, 'x') + TString(200, 'y') + TString(300, 'z');
        service.WriteData(headers, fs, nodeId, handle, 0, data);
        auto readDataResult =
            service.ReadData(headers, fs, nodeId, handle, 0, data.size());
        UNIT_ASSERT_VALUES_EQUAL(data, readDataResult->Record.GetBuffer());

        // fresh blocks - adding multiple adjacent blocks is important here to
        // catch some subtle bugs
        data = TString(8_KB, 'a');
        service.WriteData(headers, fs, nodeId, handle, 0, data);
        readDataResult =
            service.ReadData(headers, fs, nodeId, handle, 0, data.size());
        UNIT_ASSERT_VALUES_EQUAL(data, readDataResult->Record.GetBuffer());

        // blobs
        data = TString(1_MB, 'b');
        service.WriteData(headers, fs, nodeId, handle, 0, data);
        readDataResult =
            service.ReadData(headers, fs, nodeId, handle, 0, data.size());
        UNIT_ASSERT_VALUES_EQUAL(data, readDataResult->Record.GetBuffer());

        readDataResult = service.ReadData(
            headers,
            fs,
            nodeId,
            handle,
            DefaultBlockSize,
            data.size() - DefaultBlockSize);
        UNIT_ASSERT_VALUES_EQUAL(
            data.substr(DefaultBlockSize),
            readDataResult->Record.GetBuffer());

        // mix
        auto patch = TString(4_KB, 'c');
        const ui32 patchOffset = 20_KB;
        service.WriteData(headers, fs, nodeId, handle, patchOffset, patch);
        readDataResult =
            service.ReadData(headers, fs, nodeId, handle, 0, data.size());
        memcpy(data.begin() + patchOffset, patch.data(), patch.size());
        UNIT_ASSERT_VALUES_EQUAL(data, readDataResult->Record.GetBuffer());

        auto counters = env.GetCounters()
            ->FindSubgroup("component", "service_fs")
            ->FindSubgroup("host", "cluster")
            ->FindSubgroup("filesystem", fs)
            ->FindSubgroup("client", "client");
        {
            auto subgroup = counters->FindSubgroup("request", "DescribeData");
            UNIT_ASSERT(subgroup);
            UNIT_ASSERT_VALUES_EQUAL(
                5,
                subgroup->GetCounter("Count")->GetAtomic());
        }
        {
            auto subgroup = counters->FindSubgroup("request", "ReadData");
            UNIT_ASSERT(subgroup);
            UNIT_ASSERT_VALUES_EQUAL(
                5,
                subgroup->GetCounter("Count")->GetAtomic());
        }
        {
            auto subgroup = counters->FindSubgroup("request", "ReadBlob");
            UNIT_ASSERT(subgroup);
            // Read is performed thrice
            UNIT_ASSERT_VALUES_EQUAL(
                3,
                subgroup->GetCounter("Count")->GetAtomic());
        }
    }

    Y_UNIT_TEST(ShouldPerformTwoStageReadsHdd)
    {
        CheckTwoStageReads(NProto::STORAGE_MEDIA_HDD, false);
    }

    Y_UNIT_TEST(ShouldPerformTwoStageReadsHybrid)
    {
        CheckTwoStageReads(NProto::STORAGE_MEDIA_HYBRID, false);
    }

    Y_UNIT_TEST(ShouldPerformTwoStageReadsSsd)
    {
        CheckTwoStageReads(NProto::STORAGE_MEDIA_SSD, false);
    }

    Y_UNIT_TEST(ShouldFallbackToReadDataIfDescribeDataFails)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const TString fs = "test";
        service.CreateFileStore(fs, 1000);

        NProto::TError error;
        error.SetCode(E_REJECTED);
        ui32 describeDataResponses = 0;
        ui32 readDataResponses = 0;

        env.GetRuntime().SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvIndexTablet::EvDescribeDataResponse: {
                    using TResponse = TEvIndexTablet::TEvDescribeDataResponse;
                    auto* msg = event->template Get<TResponse>();
                    msg->Record.MutableError()->CopyFrom(error);
                    ++describeDataResponses;
                    return false;
                }

                case TEvService::EvReadDataResponse: {
                    ++readDataResponses;
                    return false;
                }
            }

            return false;
        });

        {
            NProto::TStorageConfig newConfig;
            newConfig.SetTwoStageReadEnabled(true);
            const auto response =
                ExecuteChangeStorageConfig(std::move(newConfig), service);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetTwoStageReadEnabled(),
                true);
            TDispatchOptions options;
            env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        }

        auto headers = service.InitSession(fs, "client");

        ui64 nodeId =
            service
                .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
                ->Record.GetNode()
                .GetId();

        ui64 handle =
            service
                .CreateHandle(headers, fs, nodeId, "", TCreateHandleArgs::RDWR)
                ->Record.GetHandle();

        TString data(4_KB, 'A');
        service.WriteData(headers, fs, nodeId, handle, 0, data);
        auto readDataResult =
            service.ReadData(headers, fs, nodeId, handle, 0, data.size());
        UNIT_ASSERT_VALUES_EQUAL(readDataResult->Record.GetBuffer(), data);
        UNIT_ASSERT_VALUES_EQUAL(2, describeDataResponses);
        UNIT_ASSERT_VALUES_EQUAL(4, readDataResponses);
    }

    Y_UNIT_TEST(ShouldFallbackToReadDataIfEvGetFails)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const TString fs = "test";
        service.CreateFileStore(fs, 1000);

        ui32 evGets = 0;
        ui32 describeDataResponses = 0;
        ui32 readDataResponses = 0;

        env.GetRuntime().SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);

            switch (event->GetTypeRewrite()) {
                case TEvBlobStorage::EvGetResult: {
                    using TResponse = TEvBlobStorage::TEvGetResult;
                    auto* msg = event->template Get<TResponse>();
                    ui32 bytes = 0;
                    for (size_t i = 0; i < msg->ResponseSz; ++i) {
                        const auto& response = msg->Responses[i];
                        bytes += response.Buffer.GetSize();
                    }
                    if (bytes == 256_KB) {
                        if (evGets == 0) {
                            msg->Status = NKikimrProto::ERROR;
                        }
                        ++evGets;
                    }
                    return false;
                }

                case TEvIndexTablet::EvDescribeDataResponse: {
                    ++describeDataResponses;
                    return false;
                }

                case TEvService::EvReadDataResponse: {
                    ++readDataResponses;
                    return false;
                }
            }

            return false;
        });

        {
            NProto::TStorageConfig newConfig;
            newConfig.SetTwoStageReadEnabled(true);
            const auto response =
                ExecuteChangeStorageConfig(std::move(newConfig), service);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetTwoStageReadEnabled(),
                true);
            TDispatchOptions options;
            env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        }

        auto headers = service.InitSession(fs, "client");

        ui64 nodeId =
            service
                .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
                ->Record.GetNode()
                .GetId();

        ui64 handle =
            service
                .CreateHandle(headers, fs, nodeId, "", TCreateHandleArgs::RDWR)
                ->Record.GetHandle();

        TString data(1_MB, 'A');
        service.WriteData(headers, fs, nodeId, handle, 0, data);
        auto readDataResult =
            service.ReadData(headers, fs, nodeId, handle, 0, data.size());
        UNIT_ASSERT_VALUES_EQUAL(readDataResult->Record.GetBuffer(), data);
        UNIT_ASSERT_VALUES_EQUAL(2, describeDataResponses);
        UNIT_ASSERT_VALUES_EQUAL(8, evGets);
        UNIT_ASSERT_VALUES_EQUAL(4, readDataResponses);
    }

    Y_UNIT_TEST(ShouldReassignTablet)
    {
        NProto::TStorageConfig config;
        config.SetCompactionThreshold(1000);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        ui64 tabletId = 0;
        ui64 reassignedTabletId = 0;
        TVector<ui32> reassignedChannels;
        env.GetRuntime().SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
            switch (event->GetTypeRewrite()) {
                case TEvSSProxy::EvDescribeFileStoreResponse: {
                    const auto* msg =
                        event->Get<TEvSSProxy::TEvDescribeFileStoreResponse>();
                    const auto& desc =
                        msg->PathDescription.GetFileStoreDescription();
                    tabletId = desc.GetIndexTabletId();

                    break;
                }

                case NKikimr::TEvHive::EvReassignTablet: {
                    const auto* msg =
                        event->Get<NKikimr::TEvHive::TEvReassignTablet>();
                    reassignedTabletId = msg->Record.GetTabletID();
                    reassignedChannels = {
                        msg->Record.GetChannels().begin(),
                        msg->Record.GetChannels().end()};

                    break;
                }
            }

            return false;
        });

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1000);

        auto headers = service.InitSession("test", "client");
        UNIT_ASSERT(headers.SessionId);
        UNIT_ASSERT(tabletId);

        NProtoPrivate::TReassignTabletRequest request;
        request.SetTabletId(tabletId);
        request.AddChannels(1);
        request.AddChannels(4);

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);
        auto jsonResponse = service.ExecuteAction("reassigntablet", buf);
        NProtoPrivate::TReassignTabletResponse response;
        UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
            jsonResponse->Record.GetOutput(), &response).ok());

        UNIT_ASSERT_VALUES_EQUAL(tabletId, reassignedTabletId);
        UNIT_ASSERT_VALUES_EQUAL(2, reassignedChannels.size());
        UNIT_ASSERT_VALUES_EQUAL(1, reassignedChannels[0]);
        UNIT_ASSERT_VALUES_EQUAL(4, reassignedChannels[1]);
    }

    TString GenerateValidateData(ui32 size)
    {
        TString data(size, 0);
        for (ui32 i = 0; i < size; ++i) {
            data[i] = 'A' + (i % ('Z' - 'A' + 1));
        }
        return data;
    }

    void CheckThreeStageWrites(NProto::EStorageMediaKind kind, bool disableForHdd)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const TString fs = "test";
        service.CreateFileStore(fs, 1000, DefaultBlockSize, kind);

        {
            NProto::TStorageConfig newConfig;
            newConfig.SetThreeStageWriteEnabled(true);
            newConfig.SetThreeStageWriteThreshold(1);
            newConfig.SetThreeStageWriteDisabledForHDD(disableForHdd);
            const auto response =
                ExecuteChangeStorageConfig(std::move(newConfig), service);
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                response.GetStorageConfig().GetThreeStageWriteEnabled());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                response.GetStorageConfig().GetThreeStageWriteThreshold());
            UNIT_ASSERT_VALUES_EQUAL(
                disableForHdd,
                response.GetStorageConfig().GetThreeStageWriteDisabledForHDD());

            TDispatchOptions options;
            env.GetRuntime().DispatchEvents(options, TDuration::Seconds(1));
        }

        auto headers = service.InitSession(fs, "client");
        ui64 nodeId = service
            .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
            ->Record.GetNode()
            .GetId();
        ui64 handle = service
            .CreateHandle(headers, fs, nodeId, "", TCreateHandleArgs::RDWR)
            ->Record.GetHandle();

        ui32 putRequestCount = 0;
        TActorId worker;
        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvGenerateBlobIdsRequest: {
                        if (!worker) {
                            worker = event->Sender;
                        }
                        break;
                    }
                    case TEvBlobStorage::EvPut: {
                        if (event->Sender == worker &&
                            event->Recipient.IsService() &&
                            event->Recipient.ServiceId().StartsWith("bsproxy"))
                        {
                            auto* msg =
                                event->template Get<TEvBlobStorage::TEvPut>();
                            UNIT_ASSERT_VALUES_EQUAL(
                                NKikimrBlobStorage::UserData,
                                msg->HandleClass);
                            ++putRequestCount;
                        }
                        break;
                    }
                }
                return false;
            });

        auto& runtime = env.GetRuntime();

        auto validateWriteData =
            [&](ui64 offset, ui64 size, ui32 expectedPutCount)
        {
            auto data = GenerateValidateData(size);

            service.WriteData(headers, fs, nodeId, handle, offset, data);
            auto readDataResult =
                service
                    .ReadData(headers, fs, nodeId, handle, offset, data.size());
            // clang-format off
            UNIT_ASSERT_VALUES_EQUAL(data, readDataResult->Record.GetBuffer());
            UNIT_ASSERT_VALUES_EQUAL(2, runtime.GetCounter(TEvIndexTablet::EvGenerateBlobIdsRequest));
            UNIT_ASSERT_VALUES_EQUAL(2, runtime.GetCounter(TEvIndexTablet::EvAddDataRequest));
            UNIT_ASSERT_VALUES_EQUAL(1, runtime.GetCounter(TEvIndexTabletPrivate::EvAddBlobRequest));
            UNIT_ASSERT_VALUES_EQUAL(0, runtime.GetCounter(TEvIndexTabletPrivate::EvWriteBlobRequest));
            UNIT_ASSERT_VALUES_EQUAL(1, runtime.GetCounter(TEvService::EvWriteDataResponse));
            UNIT_ASSERT_VALUES_EQUAL(expectedPutCount, putRequestCount);
            // clang-format on
            runtime.ClearCounters();
            putRequestCount = 0;
            worker = TActorId();
        };

        validateWriteData(0, DefaultBlockSize, 1);
        validateWriteData(DefaultBlockSize, DefaultBlockSize, 1);
        validateWriteData(0, DefaultBlockSize * BlockGroupSize, 1);
        validateWriteData(0, DefaultBlockSize * BlockGroupSize * 2, 2);
        validateWriteData(
            DefaultBlockSize,
            DefaultBlockSize * BlockGroupSize * 10,
            11);
        validateWriteData(0, DefaultBlockSize * BlockGroupSize * 3, 3);
        // Currently the data is written from 0th to (1 + BlockGroupSize * 10) = 641th block
        // Therefore, the next write should fail
        auto stat =
            service.GetNodeAttr(headers, fs, RootNodeId, "file")->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(641 * DefaultBlockSize, stat.GetSize());

        auto data =
            GenerateValidateData(DefaultBlockSize * 360);

        auto response =
            service.AssertWriteDataFailed(headers, fs, nodeId, handle, DefaultBlockSize * 641, data);
        auto error = STATUS_FROM_CODE(response->GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL((ui32)NProto::E_FS_NOSPC, error);

        auto counters = env.GetCounters()
            ->FindSubgroup("component", "service_fs")
            ->FindSubgroup("host", "cluster")
            ->FindSubgroup("filesystem", fs)
            ->FindSubgroup("client", "client");
        {
            auto subgroup = counters->FindSubgroup("request", "GenerateBlobIds");
            UNIT_ASSERT(subgroup);
            UNIT_ASSERT_VALUES_EQUAL(
                7,
                subgroup->GetCounter("Count")->GetAtomic());
        }
        {
            auto subgroup = counters->FindSubgroup("request", "AddData");
            UNIT_ASSERT(subgroup);
            // Out of 7 writes, only the last one must have failed
            UNIT_ASSERT_VALUES_EQUAL(
                6,
                subgroup->GetCounter("Count")->GetAtomic());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                subgroup->GetCounter("Errors")->GetAtomic());
        }
        {
            auto subgroup = counters->FindSubgroup("request", "WriteData");
            UNIT_ASSERT(subgroup);
            UNIT_ASSERT_VALUES_EQUAL(
                7,
                subgroup->GetCounter("Count")->GetAtomic());
        }
        {
            auto subgroup = counters->FindSubgroup("request", "WriteBlob");
            UNIT_ASSERT(subgroup);
            // Total number of put requests should have been 1 + 1 + 1 + 2 + 11
            // + 3 + ceil(360 / 64) = 25
            UNIT_ASSERT_VALUES_EQUAL(
                25,
                subgroup->GetCounter("Count")->GetAtomic());
        }
    }

    Y_UNIT_TEST(ShouldPerformThreeStageWritesHdd)
    {
        CheckThreeStageWrites(NProto::STORAGE_MEDIA_HDD, false);
    }

    Y_UNIT_TEST(ShouldPerformThreeStageWritesSsd)
    {
        CheckThreeStageWrites(NProto::STORAGE_MEDIA_SSD, false);
    }

    Y_UNIT_TEST(ShouldPerformThreeStageWritesHybrid)
    {
        CheckThreeStageWrites(NProto::STORAGE_MEDIA_HYBRID, false);
    }

    Y_UNIT_TEST(ShouldNotUseThreeStageWriteForSmallOrUnalignedRequests)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const TString fs = "test";
        service.CreateFileStore(fs, 1000);

        {
            NProto::TStorageConfig newConfig;
            newConfig.SetThreeStageWriteEnabled(true);
            const auto response =
                ExecuteChangeStorageConfig(std::move(newConfig), service);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetThreeStageWriteEnabled(),
                true);
            TDispatchOptions options;
            env.GetRuntime().DispatchEvents(options, TDuration::Seconds(1));
        }

        auto headers = service.InitSession(fs, "client");
        ui64 nodeId = service
            .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
            ->Record.GetNode()
            .GetId();
        ui64 handle = service
            .CreateHandle(headers, fs, nodeId, "", TCreateHandleArgs::RDWR)
            ->Record.GetHandle();

        auto& runtime = env.GetRuntime();

        auto validateWriteData =
            [&](ui64 offset, ui64 size, ui64 expectedFilesize)
        {
            auto data = GenerateValidateData(size);

            service.WriteData(headers, fs, nodeId, handle, offset, data);
            auto readDataResult =
                service
                    .ReadData(headers, fs, nodeId, handle, offset, data.size());
            // clang-format off
            UNIT_ASSERT_VALUES_EQUAL(readDataResult->Record.GetBuffer(), data);
            UNIT_ASSERT_VALUES_EQUAL(0, runtime.GetCounter(TEvIndexTablet::EvGenerateBlobIdsRequest));
            UNIT_ASSERT_VALUES_EQUAL(0, runtime.GetCounter(TEvIndexTablet::EvAddDataRequest));
            UNIT_ASSERT_VALUES_EQUAL(3, runtime.GetCounter(TEvService::EvWriteDataRequest));
            // clang-format on
            runtime.ClearCounters();

            auto stat =
                service.GetNodeAttr(headers, fs, RootNodeId, "file")->Record.GetNode();
            UNIT_ASSERT_VALUES_EQUAL(expectedFilesize, stat.GetSize());
        };

        validateWriteData(0, 4_KB, 4_KB);
        validateWriteData(4_KB, 4_KB, 8_KB);
        validateWriteData(1, 128_KB, 1 + 128_KB);
    }

    Y_UNIT_TEST(ShouldUseThreeStageWriteForLargeUnalignedRequestsIfEnabled)
    {
        NProto::TStorageConfig config;
        config.SetThreeStageWriteEnabled(true);
        config.SetThreeStageWriteThreshold(4_KB);
        config.SetUnalignedThreeStageWriteEnabled(true);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const TString fs = "test";
        service.CreateFileStore(fs, 1000);

        auto headers = service.InitSession(fs, "client");
        ui64 nodeId = service
            .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
            ->Record.GetNode()
            .GetId();
        ui64 handle = service
            .CreateHandle(headers, fs, nodeId, "", TCreateHandleArgs::RDWR)
            ->Record.GetHandle();

        NProtoPrivate::TAddDataRequest addData;
        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);

                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvAddDataRequest: {
                        addData = event->template Get<
                            TEvIndexTablet::TEvAddDataRequest>()->Record;
                        break;
                    }
                }
                return false;
            });

        auto data = GenerateValidateData(7_KB);
        auto offset = 3_KB;

        service.WriteData(headers, fs, nodeId, handle, offset, data);
        auto readData = service.ReadData(
            headers,
            fs,
            nodeId,
            handle,
            offset,
            data.size())->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL(data, readData);
        UNIT_ASSERT_VALUES_EQUAL(2, addData.UnalignedDataRangesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(data).Head(1_KB),
            addData.GetUnalignedDataRanges(0).GetContent());
        UNIT_ASSERT_VALUES_EQUAL(
            TStringBuf(data).Tail(5_KB),
            addData.GetUnalignedDataRanges(1).GetContent());

        auto stat = service.GetNodeAttr(
            headers,
            fs,
            RootNodeId,
            "file")->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(data.size() + offset, stat.GetSize());
    }

    Y_UNIT_TEST(ShouldFallbackThreeStageWriteToSimpleWrite)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const TString fs = "test";
        service.CreateFileStore(fs, 1000);

        NProto::TError error;
        error.SetCode(E_REJECTED);

        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);

                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvGenerateBlobIdsResponse: {
                        auto* msg = event->template Get<
                            TEvIndexTablet::TEvGenerateBlobIdsResponse>();
                        msg->Record.MutableError()->CopyFrom(error);
                        break;
                    }
                }
                return false;
            });

        {
            NProto::TStorageConfig newConfig;
            newConfig.SetThreeStageWriteEnabled(true);
            const auto response =
                ExecuteChangeStorageConfig(std::move(newConfig), service);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetThreeStageWriteEnabled(),
                true);
            TDispatchOptions options;
            env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        }

        auto headers = service.InitSession(fs, "client");
        ui64 nodeId = service
            .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
            ->Record.GetNode()
            .GetId();
        ui64 handle = service
            .CreateHandle(headers, fs, nodeId, "", TCreateHandleArgs::RDWR)
            ->Record.GetHandle();

        // GenerateBlobIdsResponse fails
        TString data = GenerateValidateData(256_KB);
        service.WriteData(headers, fs, nodeId, handle, 0, data);
        auto readDataResult =
            service.ReadData(headers, fs, nodeId, handle, 0, data.size());
        UNIT_ASSERT_VALUES_EQUAL(readDataResult->Record.GetBuffer(), data);
        auto& runtime = env.GetRuntime();
        // clang-format off
        UNIT_ASSERT_VALUES_EQUAL(2, runtime.GetCounter(TEvIndexTablet::EvGenerateBlobIdsResponse));
        UNIT_ASSERT_VALUES_EQUAL(3, runtime.GetCounter(TEvService::EvWriteDataResponse));
        // clang-format on
        runtime.ClearCounters();

        // AddDataResponse fails
        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);

                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvAddDataResponse: {
                        auto* msg = event->template Get<
                            TEvIndexTablet::TEvAddDataResponse>();
                        msg->Record.MutableError()->CopyFrom(error);
                        break;
                    }
                }
                return false;
            });
        data = GenerateValidateData(256_KB);
        service.WriteData(headers, fs, nodeId, handle, 0, data);
        readDataResult =
            service.ReadData(headers, fs, nodeId, handle, 0, data.size());
        UNIT_ASSERT_VALUES_EQUAL(readDataResult->Record.GetBuffer(), data);
        // clang-format off
        UNIT_ASSERT_VALUES_EQUAL(2, runtime.GetCounter(TEvIndexTablet::EvAddDataResponse));
        UNIT_ASSERT_VALUES_EQUAL(2, runtime.GetCounter(TEvIndexTablet::EvGenerateBlobIdsResponse));
        UNIT_ASSERT_VALUES_EQUAL(3, runtime.GetCounter(TEvService::EvWriteDataResponse));
        // clang-format on

        // TEvGet fails

        runtime.ClearCounters();

        TActorId worker;
        ui32 evPuts = 0;
        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);

                switch (event->GetTypeRewrite()) {
                    case TEvIndexTablet::EvGenerateBlobIdsRequest: {
                        if (!worker) {
                            worker = event->Sender;
                        }
                        break;
                    }
                    case TEvBlobStorage::EvPutResult: {
                        auto* msg =
                            event->template Get<TEvBlobStorage::TEvPutResult>();
                        if (event->Recipient == worker) {
                            if (evPuts == 0) {
                                msg->Status = NKikimrProto::ERROR;
                            }
                            ++evPuts;
                        }
                        break;
                    }
                }

                return false;
            });

        data = GenerateValidateData(256_KB);
        service.WriteData(headers, fs, nodeId, handle, 0, data);
        readDataResult =
            service.ReadData(headers, fs, nodeId, handle, 0, data.size());
        UNIT_ASSERT_VALUES_EQUAL(readDataResult->Record.GetBuffer(), data);

        // clang-format off
        UNIT_ASSERT_VALUES_EQUAL(0, runtime.GetCounter(TEvIndexTablet::EvAddDataResponse));
        UNIT_ASSERT_VALUES_EQUAL(2, runtime.GetCounter(TEvIndexTablet::EvGenerateBlobIdsResponse));
        UNIT_ASSERT_VALUES_EQUAL(3, runtime.GetCounter(TEvService::EvWriteDataResponse));
        UNIT_ASSERT_VALUES_EQUAL(1, evPuts);
        // clang-format on
    }

    Y_UNIT_TEST(ShouldSendBSGroupFlagsToTabletViaAddDataRequests)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const TString fs = "test";
        service.CreateFileStore(fs, 1000);

        {
            NProto::TStorageConfig newConfig;
            newConfig.SetThreeStageWriteEnabled(true);
            const auto response =
                ExecuteChangeStorageConfig(std::move(newConfig), service);
            env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        }

        auto headers = service.InitSession(fs, "client");
        ui64 nodeId = service
            .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
            ->Record.GetNode()
            .GetId();
        ui64 handle = service
            .CreateHandle(headers, fs, nodeId, "", TCreateHandleArgs::RDWR)
            ->Record.GetHandle();

        const auto yellowFlag =
            NKikimrBlobStorage::EStatusFlags::StatusDiskSpaceYellowStop;

        NProtoPrivate::TAddDataRequest addData;
        using TFlags = NKikimr::TStorageStatusFlags;
        const float freeSpaceShare = 0.22;
        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);

                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPutResult: {
                        auto* msg =
                            event->template Get<TEvBlobStorage::TEvPutResult>();
                        const_cast<TFlags&>(msg->StatusFlags).Raw |=
                            ui32(yellowFlag);
                        const_cast<float&>(msg->ApproximateFreeSpaceShare) =
                            freeSpaceShare;
                        break;
                    }

                    case TEvIndexTablet::EvAddDataRequest: {
                        addData = event->template Get<
                            TEvIndexTablet::TEvAddDataRequest>()->Record;
                        break;
                    }
                }
                return false;
            });

        TString data = GenerateValidateData(256_KB);
        service.WriteData(headers, fs, nodeId, handle, 0, data);
        UNIT_ASSERT_VALUES_EQUAL(1, addData.BlobIdsSize());
        UNIT_ASSERT_VALUES_EQUAL(1, addData.StorageStatusFlagsSize());
        UNIT_ASSERT(NKikimr::TStorageStatusFlags(
            addData.GetStorageStatusFlags(0)).Check(yellowFlag));
        UNIT_ASSERT_VALUES_EQUAL(1, addData.ApproximateFreeSpaceSharesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            freeSpaceShare,
            addData.GetApproximateFreeSpaceShares(0));
    }

    void ConfigureShards(
        TServiceClient& service,
        const TString& fsId,
        const TString& shard1Id,
        const TString& shard2Id)
    {
        {
            NProtoPrivate::TConfigureAsShardRequest request;
            request.SetFileSystemId(shard1Id);
            request.SetShardNo(1);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("configureasshard", buf);
            NProtoPrivate::TConfigureAsShardResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());
        }

        {
            NProtoPrivate::TConfigureAsShardRequest request;
            request.SetFileSystemId(shard2Id);
            request.SetShardNo(2);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("configureasshard", buf);
            NProtoPrivate::TConfigureAsShardResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());
        }

        {
            NProtoPrivate::TConfigureShardsRequest request;
            request.SetFileSystemId(fsId);
            *request.AddShardFileSystemIds() = shard1Id;
            *request.AddShardFileSystemIds() = shard2Id;

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("configureshards", buf);
            NProtoPrivate::TConfigureShardsResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());
        }
    }

    Y_UNIT_TEST(ShouldCreateSessionInShards)
    {
        NProto::TStorageConfig config;
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");
        auto headers1 = headers;
        headers1.FileSystemId = shard1Id;
        auto headers2 = headers;
        headers2.FileSystemId = shard2Id;

        ui64 nodeId1 =
            service
                .CreateNode(headers1, TCreateNodeArgs::File(RootNodeId, "file"))
                ->Record.GetNode()
                .GetId();

        UNIT_ASSERT_VALUES_EQUAL((1LU << 56U) + 2, nodeId1);

        ui64 handle1 =
            service
                .CreateHandle(
                    headers1,
                    headers1.FileSystemId,
                    nodeId1,
                    "",
                    TCreateHandleArgs::RDWR)
                ->Record.GetHandle();

        UNIT_ASSERT_C(
            handle1 >= (1LU << 56U) && handle1 < (2LU << 56U),
            handle1);

        service.WriteData(
            headers1,
            headers1.FileSystemId,
            nodeId1,
            handle1,
            0,
            TString(1_MB, 'a'));

        ui64 nodeId2 =
            service
                .CreateNode(headers2, TCreateNodeArgs::File(RootNodeId, "file"))
                ->Record.GetNode()
                .GetId();

        UNIT_ASSERT_VALUES_EQUAL((2LU << 56U) + 2, nodeId2);

        ui64 handle2 =
            service
                .CreateHandle(
                    headers2,
                    headers2.FileSystemId,
                    nodeId2,
                    "",
                    TCreateHandleArgs::RDWR)
                ->Record.GetHandle();

        UNIT_ASSERT_C(
            handle2 >= (2LU << 56U) && handle2 < (3LU << 56U),
            handle2);

        service.WriteData(
            headers2,
            headers2.FileSystemId,
            nodeId2,
            handle2,
            0,
            TString(1_MB, 'a'));

        for (const auto& shardId: {shard1Id, shard2Id}) {
            NProtoPrivate::TDescribeSessionsRequest request;
            request.SetFileSystemId(shardId);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("describesessions", buf);
            NProtoPrivate::TDescribeSessionsResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());

            const auto& sessions = response.GetSessions();
            UNIT_ASSERT_VALUES_EQUAL(1, sessions.size());

            UNIT_ASSERT_VALUES_EQUAL(
                headers.SessionId,
                sessions[0].GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL(
                headers.ClientId,
                sessions[0].GetClientId());
            UNIT_ASSERT_VALUES_EQUAL("", sessions[0].GetSessionState());
        }

        const TString sessionState = "some_state";
        service.ResetSession(headers, sessionState);

        for (const auto& shardId: {shard1Id, shard2Id}) {
            NProtoPrivate::TDescribeSessionsRequest request;
            request.SetFileSystemId(shardId);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("describesessions", buf);
            NProtoPrivate::TDescribeSessionsResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());

            const auto& sessions = response.GetSessions();
            UNIT_ASSERT_VALUES_EQUAL(1, sessions.size());

            UNIT_ASSERT_VALUES_EQUAL(
                headers.SessionId,
                sessions[0].GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL(
                headers.ClientId,
                sessions[0].GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(
                sessionState,
                sessions[0].GetSessionState());
        }

        service.DestroySession(headers);

        for (const auto& shardId: {shard1Id, shard2Id}) {
            NProtoPrivate::TDescribeSessionsRequest request;
            request.SetFileSystemId(shardId);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("describesessions", buf);
            NProtoPrivate::TDescribeSessionsResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());

            const auto& sessions = response.GetSessions();
            UNIT_ASSERT_VALUES_EQUAL(0, sessions.size());
        }
    }

    Y_UNIT_TEST(ShouldRestoreSessionInShardAfterShardRestart)
    {
        const auto idleSessionTimeout = TDuration::Minutes(2);
        NProto::TStorageConfig config;
        config.SetIdleSessionTimeout(idleSessionTimeout.MilliSeconds());
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);

        TActorId leaderActorId;
        ui64 shard1TabletId = -1;
        ui64 shard2TabletId = -1;
        const auto startupEventType =
            TEvIndexTabletPrivate::EvLoadCompactionMapChunkRequest;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeFileStoreResponse: {
                        using TDesc = TEvSSProxy::TEvDescribeFileStoreResponse;
                        const auto* msg = event->Get<TDesc>();
                        const auto& desc =
                            msg->PathDescription.GetFileStoreDescription();
                        if (desc.GetConfig().GetFileSystemId() == shard1Id) {
                            shard1TabletId = desc.GetIndexTabletId();
                        }

                        if (desc.GetConfig().GetFileSystemId() == shard2Id) {
                            shard2TabletId = desc.GetIndexTabletId();
                        }

                        break;
                    }

                    case startupEventType: {
                        if (!leaderActorId) {
                            leaderActorId = event->Sender;
                        }

                        break;
                    }
                }

                return false;
            });

        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");
        auto headers1 = headers;
        headers1.FileSystemId = shard1Id;
        auto headers2 = headers;
        headers2.FileSystemId = shard2Id;

        // creating nodes and handles in both shards

        ui64 nodeId1 =
            service
                .CreateNode(headers1, TCreateNodeArgs::File(RootNodeId, "file"))
                ->Record.GetNode()
                .GetId();

        UNIT_ASSERT_VALUES_EQUAL((1LU << 56U) + 2, nodeId1);

        ui64 handle1 =
            service
                .CreateHandle(
                    headers1,
                    headers1.FileSystemId,
                    nodeId1,
                    "",
                    TCreateHandleArgs::RDWR)
                ->Record.GetHandle();

        UNIT_ASSERT_C(
            handle1 >= (1LU << 56U) && handle1 < (2LU << 56U),
            handle1);

        ui64 nodeId2 =
            service
                .CreateNode(headers2, TCreateNodeArgs::File(RootNodeId, "file"))
                ->Record.GetNode()
                .GetId();

        UNIT_ASSERT_VALUES_EQUAL((2LU << 56U) + 2, nodeId2);

        ui64 handle2 =
            service
                .CreateHandle(
                    headers2,
                    headers2.FileSystemId,
                    nodeId2,
                    "",
                    TCreateHandleArgs::RDWR)
                ->Record.GetHandle();

        UNIT_ASSERT_C(
            handle2 >= (2LU << 56U) && handle2 < (3LU << 56U),
            handle2);

        // rebooting shards

        TVector<TActorId> fsActorIds;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvLoadCompactionMapChunkRequest: {
                        // catching one of the startup events to detect shard
                        // actor ids

                        if (Find(fsActorIds, event->Sender) == fsActorIds.end()) {
                            fsActorIds.push_back(event->Sender);
                        }

                        break;
                    }
                }

                return false;
            });

        TIndexTabletClient shard1(
            env.GetRuntime(),
            nodeIdx,
            shard1TabletId,
            {}, // config
            false // updateConfig
        );
        shard1.RebootTablet();

        TIndexTabletClient shard2(
            env.GetRuntime(),
            nodeIdx,
            shard2TabletId,
            {}, // config
            false // updateConfig
        );
        shard2.RebootTablet();

        // triggering shard sessions sync
        // sending the event manually since registration observers which enable
        // scheduling for actors are reset upon tablet reboot

        env.GetRuntime().AdvanceCurrentTime(idleSessionTimeout / 2);

        {
            using TRequest = TEvIndexTabletPrivate::TEvSyncSessionsRequest;

            env.GetRuntime().Send(
                new IEventHandle(
                    leaderActorId, // recipient
                    TActorId(), // sender
                    new TRequest(),
                    0, // flags
                    0),
                0);
        }

        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));
        // waiting for idle session expiration
        // sending the event manually since registration observers which enable
        // scheduling for actors are reset upon tablet reboot

        env.GetRuntime().AdvanceCurrentTime(idleSessionTimeout / 2);
        service.PingSession(headers);

        for (const auto& actorId: fsActorIds) {
            using TRequest = TEvIndexTabletPrivate::TEvCleanupSessionsRequest;

            env.GetRuntime().Send(
                new IEventHandle(
                    actorId, // recipient
                    TActorId(), // sender
                    new TRequest(),
                    0, // flags
                    0),
                0);
        }

        // need to pass deadline instead of timeout here since otherwise the
        // adjusted time gets added to the timeout
        env.GetRuntime().DispatchEvents(
            {},
            TInstant::Now() + TDuration::MilliSeconds(100));

        // shard sessions should exist

        for (const auto& id: {fsId, shard1Id, shard2Id}) {
            NProtoPrivate::TDescribeSessionsRequest request;
            request.SetFileSystemId(id);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("describesessions", buf);
            NProtoPrivate::TDescribeSessionsResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());

            const auto& sessions = response.GetSessions();
            UNIT_ASSERT_VALUES_EQUAL(1, sessions.size());

            UNIT_ASSERT_VALUES_EQUAL(
                headers.SessionId,
                sessions[0].GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL(
                headers.ClientId,
                sessions[0].GetClientId());
        }

        // handles should be alive

        service.WriteData(
            headers1,
            headers1.FileSystemId,
            nodeId1,
            handle1,
            0,
            TString(1_MB, 'a'));

        service.WriteData(
            headers2,
            headers2.FileSystemId,
            nodeId2,
            handle2,
            0,
            TString(1_MB, 'a'));
    }

    Y_UNIT_TEST(ShouldCreateNodeInShardViaLeader)
    {
        NProto::TStorageConfig config;
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");

        ui64 nodeId1 =
            service
                .CreateNode(headers, TCreateNodeArgs::File(
                    RootNodeId,
                    "file1",
                    0, // mode
                    shard1Id))
                ->Record.GetNode()
                .GetId();

        UNIT_ASSERT_VALUES_EQUAL((1LU << 56U) + 2, nodeId1);

        auto createHandleResponse = service.CreateHandle(
            headers,
            fsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::RDWR)->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            shard1Id,
            createHandleResponse.GetShardFileSystemId());

        UNIT_ASSERT_VALUES_UNEQUAL(
            "",
            createHandleResponse.GetShardNodeName());

        auto headers1 = headers;
        headers1.FileSystemId = shard1Id;

        createHandleResponse = service.CreateHandle(
            headers1,
            headers1.FileSystemId,
            RootNodeId,
            createHandleResponse.GetShardNodeName(),
            TCreateHandleArgs::RDWR)->Record;

        auto handle1 = createHandleResponse.GetHandle();

        UNIT_ASSERT_C(
            handle1 >= (1LU << 56U) && handle1 < (2LU << 56U),
            handle1);

        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            createHandleResponse.GetNodeAttr().GetId());

        service.WriteData(
            headers1,
            headers1.FileSystemId,
            nodeId1,
            handle1,
            0,
            TString(1_MB, 'a'));
    }

    Y_UNIT_TEST(ShouldCreateNodeInShardByCreateHandleViaLeader)
    {
        NProto::TStorageConfig config;
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");

        auto createHandleResponse = service.CreateHandle(
            headers,
            fsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::CREATE,
            shard1Id)->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            shard1Id,
            createHandleResponse.GetShardFileSystemId());

        const auto shardNodeName =
            createHandleResponse.GetShardNodeName();

        UNIT_ASSERT_VALUES_UNEQUAL("", shardNodeName);

        const auto nodeId1 = createHandleResponse.GetNodeAttr().GetId();
        UNIT_ASSERT_VALUES_EQUAL((1LU << 56U) + 2, nodeId1);

        auto headers1 = headers;
        headers1.FileSystemId = shard1Id;

        createHandleResponse = service.CreateHandle(
            headers1,
            headers1.FileSystemId,
            RootNodeId,
            shardNodeName,
            TCreateHandleArgs::RDWR)->Record;

        auto handle1 = createHandleResponse.GetHandle();

        UNIT_ASSERT_C(
            handle1 >= (1LU << 56U) && handle1 < (2LU << 56U),
            handle1);

        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            createHandleResponse.GetNodeAttr().GetId());

        service.WriteData(
            headers1,
            headers1.FileSystemId,
            nodeId1,
            handle1,
            0,
            TString(1_MB, 'a'));

        auto getAttrResponse = service.GetNodeAttr(
            headers,
            fsId,
            RootNodeId,
            "file1")->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            shard1Id,
            getAttrResponse.GetNode().GetShardFileSystemId());

        UNIT_ASSERT_VALUES_EQUAL(
            shardNodeName,
            getAttrResponse.GetNode().GetShardNodeName());

        getAttrResponse = service.GetNodeAttr(
            headers1,
            shard1Id,
            RootNodeId,
            shardNodeName)->Record;

        UNIT_ASSERT_VALUES_EQUAL(nodeId1, getAttrResponse.GetNode().GetId());
        UNIT_ASSERT_VALUES_EQUAL(1_MB, getAttrResponse.GetNode().GetSize());

        auto listNodesResponse = service.ListNodes(
            headers,
            fsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL("file1", listNodesResponse.GetNames(0));

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            shard1Id,
            listNodesResponse.GetNodes(0).GetShardFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL(
            shardNodeName,
            listNodesResponse.GetNodes(0).GetShardNodeName());
    }

    Y_UNIT_TEST(ShouldForwardRequestsToShard)
    {
        NProto::TStorageConfig config;
        config.SetMultiTabletForwardingEnabled(true);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");

        auto createHandleResponse = service.CreateHandle(
            headers,
            fsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::CREATE_EXL)->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            "",
            createHandleResponse.GetShardFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL(
            "",
            createHandleResponse.GetShardNodeName());

        const auto nodeId1 = createHandleResponse.GetNodeAttr().GetId();
        UNIT_ASSERT_VALUES_EQUAL((1LU << 56U) + 2, nodeId1);

        const auto handle1 = createHandleResponse.GetHandle();

        UNIT_ASSERT_C(
            handle1 >= (1LU << 56U) && handle1 < (2LU << 56U),
            handle1);

        auto accessNodeResponse = service.AccessNode(
            headers,
            fsId,
            nodeId1)->Record;

        Y_UNUSED(accessNodeResponse);

        auto setNodeAttrResponse = service.SetNodeAttr(
            headers,
            fsId,
            nodeId1,
            1_MB)->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            setNodeAttrResponse.GetNode().GetId());

        UNIT_ASSERT_VALUES_EQUAL(1_MB, setNodeAttrResponse.GetNode().GetSize());

        auto allocateDataResponse = service.AllocateData(
            headers,
            fsId,
            nodeId1,
            handle1,
            0,
            2_MB)->Record;

        Y_UNUSED(allocateDataResponse);

        auto data = GenerateValidateData(256_KB);
        service.WriteData(headers, fsId, nodeId1, handle1, 0, data);
        auto readDataResponse = service.ReadData(
            headers,
            fsId,
            nodeId1,
            handle1,
            0,
            data.size())->Record;
        UNIT_ASSERT_VALUES_EQUAL(data, readDataResponse.GetBuffer());

        auto destroyHandleResponse = service.DestroyHandle(
            headers,
            fsId,
            nodeId1,
            handle1)->Record;

        Y_UNUSED(destroyHandleResponse);

        const auto createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file2"))->Record;

        const auto nodeId2 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL((2LU << 56U) + 2, nodeId2);

        auto getNodeAttrResponse = service.GetNodeAttr(
            headers,
            fsId,
            RootNodeId,
            "file1")->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            getNodeAttrResponse.GetNode().GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            2_MB,
            getNodeAttrResponse.GetNode().GetSize());

        getNodeAttrResponse = service.GetNodeAttr(
            headers,
            fsId,
            nodeId1,
            "")->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            getNodeAttrResponse.GetNode().GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            2_MB,
            getNodeAttrResponse.GetNode().GetSize());

        auto listNodesResponse = service.ListNodes(
            headers,
            fsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(2, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL("file1", listNodesResponse.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL("file2", listNodesResponse.GetNames(1));

        UNIT_ASSERT_VALUES_EQUAL(2, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            listNodesResponse.GetNodes(0).GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            2_MB,
            listNodesResponse.GetNodes(0).GetSize());
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId2,
            listNodesResponse.GetNodes(1).GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            listNodesResponse.GetNodes(1).GetSize());

        service.UnlinkNode(headers, RootNodeId, "file1");

        listNodesResponse = service.ListNodes(
            headers,
            fsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL("file2", listNodesResponse.GetNames(0));

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId2,
            listNodesResponse.GetNodes(0).GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            listNodesResponse.GetNodes(0).GetSize());

        auto headers1 = headers;
        headers1.FileSystemId = shard1Id;

        listNodesResponse = service.ListNodes(
            headers1,
            shard1Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());

        auto headers2 = headers;
        headers2.FileSystemId = shard2Id;

        listNodesResponse = service.ListNodes(
            headers2,
            shard2Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());

        auto setXAttrResponse = service.SetNodeXAttr(
            headers,
            fsId,
            nodeId2,
            "user.some_attr",
            "some_value")->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, setXAttrResponse.GetVersion());

        auto getXAttrResponse = service.GetNodeXAttr(
            headers,
            fsId,
            nodeId2,
            "user.some_attr")->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, getXAttrResponse.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL("some_value", getXAttrResponse.GetValue());

        service.SendSetNodeXAttrRequest(
            headers,
            fsId,
            nodeId1,
            "user.some_attr",
            "some_value");

        auto setNodeXAttrResponseEvent = service.RecvSetNodeXAttrResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_FS_NOENT,
            setNodeXAttrResponseEvent->GetStatus(),
            setNodeXAttrResponseEvent->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldCreateDirectoryStructureInLeader)
    {
        NProto::TStorageConfig config;
        config.SetMultiTabletForwardingEnabled(true);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");

        auto createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::Directory(RootNodeId, "dir1"))->Record;
        const auto dir1Id = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(0, ExtractShardNo(dir1Id));

        createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::Directory(dir1Id, "dir1_1"))->Record;
        const auto dir1_1Id = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(0, ExtractShardNo(dir1_1Id));

        createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::Directory(dir1Id, "dir1_2"))->Record;
        const auto dir1_2Id = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(0, ExtractShardNo(dir1_2Id));

        auto createHandleResponse = service.CreateHandle(
            headers,
            fsId,
            dir1_1Id,
            "file1",
            TCreateHandleArgs::CREATE)->Record;

        const auto nodeId1 = createHandleResponse.GetNodeAttr().GetId();
        UNIT_ASSERT_VALUES_EQUAL(ShardedId(2, 1), nodeId1);

        const auto handle1 = createHandleResponse.GetHandle();
        UNIT_ASSERT_C(ExtractShardNo(handle1) == 1, handle1);

        createHandleResponse = service.CreateHandle(
            headers,
            fsId,
            dir1_2Id,
            "file1",
            TCreateHandleArgs::CREATE)->Record;

        const auto nodeId2 = createHandleResponse.GetNodeAttr().GetId();
        UNIT_ASSERT_VALUES_EQUAL(ShardedId(2, 2), nodeId2);

        const auto handle2 = createHandleResponse.GetHandle();
        UNIT_ASSERT_C(ExtractShardNo(handle2) == 2, handle2);

        auto data = GenerateValidateData(256_KB);
        service.WriteData(headers, fsId, nodeId1, handle1, 0, data);
        auto readDataResponse = service.ReadData(
            headers,
            fsId,
            nodeId1,
            handle1,
            0,
            data.size())->Record;
        UNIT_ASSERT_VALUES_EQUAL(data, readDataResponse.GetBuffer());

        data = GenerateValidateData(1_MB);
        service.WriteData(headers, fsId, nodeId2, handle2, 0, data);
        readDataResponse = service.ReadData(
            headers,
            fsId,
            nodeId2,
            handle2,
            0,
            data.size())->Record;
        UNIT_ASSERT_VALUES_EQUAL(data, readDataResponse.GetBuffer());

        service.DestroyHandle(headers, fsId, nodeId1, handle1);
        service.DestroyHandle(headers, fsId, nodeId2, handle2);
    }

    Y_UNIT_TEST(ShouldNotFailOnLegacyHandlesWithHighBits)
    {
        NProto::TStorageConfig config;
        config.SetMultiTabletForwardingEnabled(true);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1'000);

        // forcing fs to set high bits for handle ids
        {
            NProtoPrivate::TConfigureAsShardRequest request;
            request.SetFileSystemId(fsId);
            request.SetShardNo(111);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("configureasshard", buf);
            NProtoPrivate::TConfigureAsShardResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());
        }

        auto headers = service.InitSession(fsId, "client");

        auto createHandleResponse = service.CreateHandle(
            headers,
            fsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::CREATE_EXL)->Record;

        const auto nodeId1 = createHandleResponse.GetNodeAttr().GetId();
        UNIT_ASSERT_VALUES_EQUAL(111, ExtractShardNo(nodeId1));

        const auto handle1 = createHandleResponse.GetHandle();
        UNIT_ASSERT_VALUES_EQUAL(111, ExtractShardNo(handle1));

        auto data = GenerateValidateData(256_KB);
        service.WriteData(headers, fsId, nodeId1, handle1, 0, data);
        auto readDataResponse = service.ReadData(
            headers,
            fsId,
            nodeId1,
            handle1,
            0,
            data.size())->Record;
        UNIT_ASSERT_VALUES_EQUAL(data, readDataResponse.GetBuffer());
    }

    Y_UNIT_TEST(ShouldHandleCreateNodeErrorFromShardUponCreateHandleViaLeader)
    {
        NProto::TStorageConfig config;
        config.SetMultiTabletForwardingEnabled(true);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");

        auto error = MakeError(E_FAIL);

        env.GetRuntime().SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvCreateNodeResponse: {
                        auto* msg =
                            event->Get<TEvService::TEvCreateNodeResponse>();
                        if (error.GetCode()) {
                            msg->Record.MutableError()->CopyFrom(error);
                        }

                        break;
                    }
                }

                return false;
            });

        const ui64 requestId = 111;

        service.SendCreateHandleRequest(
            headers,
            fsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::CREATE_EXL,
            "",
            requestId);

        auto response = service.RecvCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            error.GetCode(),
            response->GetError().GetCode(),
            FormatError(response->GetError()));

        error = {};

        service.SendCreateHandleRequest(
            headers,
            fsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::CREATE_EXL,
            "",
            requestId);

        response = service.RecvCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            error.GetCode(),
            response->GetError().GetCode(),
            FormatError(response->GetError()));
    }

    void DoTestShouldNotFailListNodesUponGetAttrENOENT(
        NProto::TStorageConfig config)
    {
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");

        service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"));
        service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file2"));
        service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file3"));
        service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file4"));

        auto listNodesResponse = service.ListNodes(
            headers,
            fsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(4, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL("file1", listNodesResponse.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL("file2", listNodesResponse.GetNames(1));
        UNIT_ASSERT_VALUES_EQUAL("file3", listNodesResponse.GetNames(2));
        UNIT_ASSERT_VALUES_EQUAL("file4", listNodesResponse.GetNames(3));
        TVector<std::pair<ui64, TString>> nodes(4);
        for (ui32 i = 0; i < 4; ++i) {
            nodes[i] = {
                listNodesResponse.GetNodes(i).GetId(),
                listNodesResponse.GetNames(i)};
            UNIT_ASSERT_VALUES_UNEQUAL(0, nodes[i].first);
        }

        auto headers1 = headers;
        headers1.FileSystemId = shard1Id;

        listNodesResponse = service.ListNodes(
            headers1,
            shard1Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(2, listNodesResponse.NamesSize());
        const auto shard1NodeName1 = listNodesResponse.GetNames(0);
        const auto shard1NodeId1 = listNodesResponse.GetNodes(0).GetId();
        const auto shard1NodeName2 = listNodesResponse.GetNames(1);

        auto headers2 = headers;
        headers2.FileSystemId = shard2Id;

        listNodesResponse = service.ListNodes(
            headers2,
            shard2Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(2, listNodesResponse.NamesSize());
        const auto shard2NodeName1 = listNodesResponse.GetNames(0);
        const auto shard2NodeName2 = listNodesResponse.GetNames(1);

        // "breaking" one node - deleting it directly from the shard
        service.UnlinkNode(headers1, RootNodeId, shard1NodeName1);

        EraseIf(nodes, [=] (const auto& node) {
            return node.first == shard1NodeId1;
        });

        // ListNodes should still succeed
        listNodesResponse = service.ListNodes(
            headers,
            fsId,
            RootNodeId)->Record;

        // unresolved nodes should be removed from the response
        UNIT_ASSERT_VALUES_EQUAL(3, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            nodes[0].second,
            listNodesResponse.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL(
            nodes[1].second,
            listNodesResponse.GetNames(1));
        UNIT_ASSERT_VALUES_EQUAL(
            nodes[2].second,
            listNodesResponse.GetNames(2));

        UNIT_ASSERT_VALUES_EQUAL(3, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            nodes[0].first,
            listNodesResponse.GetNodes(0).GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            nodes[1].first,
            listNodesResponse.GetNodes(1).GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            nodes[2].first,
            listNodesResponse.GetNodes(2).GetId());

        const auto counters =
            env.GetCounters()->FindSubgroup("component", "service");
        UNIT_ASSERT(counters);
        const auto counter =
            counters->GetCounter("AppCriticalEvents/NodeNotFoundInShard");
        UNIT_ASSERT_EQUAL(1, counter->GetAtomic());

        // "breaking" all nodes - ListNodes should fail with E_IO after this
        service.UnlinkNode(headers1, RootNodeId, shard1NodeName2);
        service.UnlinkNode(headers2, RootNodeId, shard2NodeName1);
        service.UnlinkNode(headers2, RootNodeId, shard2NodeName2);

        service.SendListNodesRequest(headers, fsId, RootNodeId);
        auto response = service.RecvListNodesResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_IO,
            response->GetError().GetCode(),
            response->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldNotFailListNodesUponGetAttrENOENT)
    {
        NProto::TStorageConfig config;
        config.SetMultiTabletForwardingEnabled(true);
        DoTestShouldNotFailListNodesUponGetAttrENOENT(std::move(config));
    }

    Y_UNIT_TEST(ShouldNotFailListNodesUponGetAttrENOENTWithGetNodeAttrBatch)
    {
        NProto::TStorageConfig config;
        config.SetMultiTabletForwardingEnabled(true);
        config.SetGetNodeAttrBatchEnabled(true);
        DoTestShouldNotFailListNodesUponGetAttrENOENT(std::move(config));
    }

    Y_UNIT_TEST(ShouldListMultipleNodesWithGetNodeAttrBatch)
    {
        NProto::TStorageConfig config;
        config.SetMultiTabletForwardingEnabled(true);
        config.SetGetNodeAttrBatchEnabled(true);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");
        auto headers1 = headers;
        headers1.FileSystemId = shard1Id;

        TVector<TString> names;
        TVector<ui64> ids;
        for (ui32 i = 10; i < 50; ++i) {
            const auto name = Sprintf("file%u", i);
            const auto id = service.CreateNode(
                headers,
                TCreateNodeArgs::File(RootNodeId, name)
            )->Record.GetNode().GetId();
            names.push_back(name);
            ids.push_back(id);
        }

        auto listNodesResponse = service.ListNodes(
            headers1,
            shard1Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(
            names.size() / 2,
            listNodesResponse.NamesSize());
        const ui32 idxToUnlink1 = 13;
        const ui32 idxToUnlink2 = 17;
        const auto shard1NodeName1 = listNodesResponse.GetNames(idxToUnlink1);
        const ui64 unlinkedId1 = listNodesResponse.GetNodes(idxToUnlink1).GetId();
        const auto shard1NodeName2 = listNodesResponse.GetNames(idxToUnlink2);
        const ui64 unlinkedId2 = listNodesResponse.GetNodes(idxToUnlink2).GetId();

        listNodesResponse = service.ListNodes(
            headers,
            fsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(names.size(), listNodesResponse.NamesSize());
        for (ui32 i = 0; i < names.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(names[i], listNodesResponse.GetNames(i));
            UNIT_ASSERT_VALUES_EQUAL(
                ids[i],
                listNodesResponse.GetNodes(i).GetId());
        }

        service.UnlinkNode(headers1, RootNodeId, shard1NodeName1);
        service.UnlinkNode(headers1, RootNodeId, shard1NodeName2);

        listNodesResponse = service.ListNodes(
            headers,
            fsId,
            RootNodeId)->Record;

        for (auto id: {unlinkedId1, unlinkedId2}) {
            auto idx = Find(ids, id) - ids.begin();
            ids.erase(ids.begin() + idx);
            names.erase(names.begin() + idx);
        }

        UNIT_ASSERT_VALUES_EQUAL(names.size(), listNodesResponse.NamesSize());
        for (ui32 i = 0; i < names.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(names[i], listNodesResponse.GetNames(i));
            UNIT_ASSERT_VALUES_EQUAL(
                ids[i],
                listNodesResponse.GetNodes(i).GetId());
        }
    }

    Y_UNIT_TEST(DestroyFileStoreWithActiveSessionShouldFail)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto initialBlockCount = 1'000;
        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, initialBlockCount);

        auto headers = THeaders{fsId, "client", ""};
        auto createSessionResponse = service.CreateSession(headers);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            createSessionResponse->GetStatus(),
            createSessionResponse->GetErrorReason());
        service.AssertDestroyFileStoreFailed(fsId);

        headers.SessionId =
            createSessionResponse->Record.GetSession().GetSessionId();
        auto destroySessionResponse = service.DestroySession(headers);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            destroySessionResponse->GetStatus(),
            destroySessionResponse->GetErrorReason());

        auto destroyFileStoreResponse = service.DestroyFileStore(fsId);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            destroyFileStoreResponse->GetStatus(),
            destroyFileStoreResponse->GetErrorReason());
    }

    Y_UNIT_TEST(DestroyDestroyedFileStoreShouldNotFail)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const TString fsId2 = "test2";
        const auto initialBlockCount = 1'000;
        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, initialBlockCount);
        service.CreateFileStore(fsId2, initialBlockCount);

        {
            auto destroyFileStoreResponse = service.DestroyFileStore(fsId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                destroyFileStoreResponse->GetStatus(),
                destroyFileStoreResponse->GetErrorReason());

            auto alreadyDestroyedFileStoreResponse =
                service.DestroyFileStore(fsId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_FALSE,
                alreadyDestroyedFileStoreResponse->GetStatus(),
                alreadyDestroyedFileStoreResponse->GetErrorReason());
        }

        {
            auto destroyFileStoreResponse = service.DestroyFileStore(fsId2);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                destroyFileStoreResponse->GetStatus(),
                destroyFileStoreResponse->GetErrorReason());

            auto alreadyDestroyedFileStoreResponse =
                service.DestroyFileStore(fsId);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_FALSE,
                alreadyDestroyedFileStoreResponse->GetStatus(),
                alreadyDestroyedFileStoreResponse->GetErrorReason());
        }
    }

    Y_UNIT_TEST(DestroyFilestoreShouldRespectDenyList)
    {
        NProto::TStorageConfig config;
        config.MutableDestroyFilestoreDenyList()->Add("test");
        config.SetGetNodeAttrBatchEnabled(true);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto initialBlockCount = 1'000;
        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, initialBlockCount);

        auto destroyFileStoreResponse = service.AssertDestroyFileStoreFailed(fsId);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            destroyFileStoreResponse->GetStatus(),
            destroyFileStoreResponse->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldValidateRequestsWithShardId)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");

        const auto createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"))->Record;

        const auto nodeId1 = createNodeResponse.GetNode().GetId();

        auto headers1 = headers;
        headers1.FileSystemId = shard1Id;

        // a request with ShardId and without Name
        service.SendCreateHandleRequest(
            headers,
            fsId,
            nodeId1,
            "",
            TCreateHandleArgs::RDWR,
            shard1Id);

        auto createHandleResponseEvent = service.RecvCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            createHandleResponseEvent->GetError().GetCode(),
            createHandleResponseEvent->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldValidateShardConfiguration)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";
        const auto shard3Id = fsId + "-f3";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);
        service.CreateFileStore(shard3Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        // ShardNo change not allowed
        {
            NProtoPrivate::TConfigureAsShardRequest request;
            request.SetFileSystemId(shard1Id);
            request.SetShardNo(2);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("configureasshard", buf);
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetError().GetCode(),
                response->GetErrorReason());
        }

        // Shard deletion not allowed
        {
            NProtoPrivate::TConfigureShardsRequest request;
            request.SetFileSystemId(fsId);
            *request.AddShardFileSystemIds() = shard1Id;

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("configureshards", buf);
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetError().GetCode(),
                response->GetErrorReason());
        }

        // Shard reordering not allowed
        {
            NProtoPrivate::TConfigureShardsRequest request;
            request.SetFileSystemId(fsId);
            *request.AddShardFileSystemIds() = shard2Id;
            *request.AddShardFileSystemIds() = shard1Id;

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            service.SendExecuteActionRequest("configureshards", buf);
            auto response = service.RecvExecuteActionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetError().GetCode(),
                response->GetErrorReason());
        }

        // Shard addition IS allowed
        {
            NProtoPrivate::TConfigureAsShardRequest request;
            request.SetFileSystemId(shard3Id);
            request.SetShardNo(3);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("configureasshard", buf);
            NProtoPrivate::TConfigureAsShardResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());
        }

        {
            NProtoPrivate::TConfigureShardsRequest request;
            request.SetFileSystemId(fsId);
            *request.AddShardFileSystemIds() = shard1Id;
            *request.AddShardFileSystemIds() = shard2Id;
            *request.AddShardFileSystemIds() = shard3Id;

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("configureshards", buf);
            NProtoPrivate::TConfigureShardsResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());
        }

        // TODO(#1350): leader should check that shards' ShardNos correspond
        // to the shard order in leader's config
    }

    Y_UNIT_TEST(ShouldRenameExternalNodes)
    {
        NProto::TStorageConfig config;
        config.SetMultiTabletForwardingEnabled(true);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");

        // creating 2 files

        auto createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"))->Record;

        const auto nodeId1 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(1, ExtractShardNo(nodeId1));

        createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file2"))->Record;

        const auto nodeId2 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(2, ExtractShardNo(nodeId2));

        ui64 handle1 = service.CreateHandle(
            headers,
            fsId,
            nodeId1,
            "",
            TCreateHandleArgs::RDWR)->Record.GetHandle();

        // writing some data to file1 then moving file1 to file3

        auto data1 = GenerateValidateData(256_KB);
        service.WriteData(headers, fsId, nodeId1, handle1, 0, data1);

        auto renameNodeResponse = service.RenameNode(
            headers,
            RootNodeId,
            "file1",
            RootNodeId,
            "file3",
            0);

        // opening file3 for reading

        auto createHandleResponse = service.CreateHandle(
            headers,
            fsId,
            RootNodeId,
            "file3",
            TCreateHandleArgs::RDNLY)->Record;

        // checking that file3 refers to the same node as formerly file1

        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            createHandleResponse.GetNodeAttr().GetId());

        auto handle1r = createHandleResponse.GetHandle();

        // checking that we can read the data that we wrote to file1

        auto readDataResponse = service.ReadData(
            headers,
            fsId,
            nodeId1,
            handle1r,
            0,
            data1.size())->Record;
        UNIT_ASSERT_VALUES_EQUAL(data1, readDataResponse.GetBuffer());

        // checking that node listing shows file3 and file2

        auto listNodesResponse = service.ListNodes(
            headers,
            fsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(2, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL("file2", listNodesResponse.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL("file3", listNodesResponse.GetNames(1));

        UNIT_ASSERT_VALUES_EQUAL(2, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId2,
            listNodesResponse.GetNodes(0).GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            listNodesResponse.GetNodes(1).GetId());

        // checking that move into an existing file (file2) works

        renameNodeResponse = service.RenameNode(
            headers,
            RootNodeId,
            "file3",
            RootNodeId,
            "file2",
            0);

        // checking that we can still read the same data

        createHandleResponse = service.CreateHandle(
            headers,
            fsId,
            RootNodeId,
            "file2",
            TCreateHandleArgs::RDNLY)->Record;

        // nodeId should be kept intact

        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            createHandleResponse.GetNodeAttr().GetId());

        handle1r = createHandleResponse.GetHandle();

        readDataResponse = service.ReadData(
            headers,
            fsId,
            nodeId1,
            handle1r,
            0,
            data1.size())->Record;
        UNIT_ASSERT_VALUES_EQUAL(data1, readDataResponse.GetBuffer());

        // listing should show only file2 now

        listNodesResponse = service.ListNodes(
            headers,
            fsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL("file2", listNodesResponse.GetNames(0));

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            listNodesResponse.GetNodes(0).GetId());

        // listing in shard2 should show nothing

        auto headers2 = headers;
        headers2.FileSystemId = shard2Id;

        listNodesResponse = service.ListNodes(
            headers2,
            shard2Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());

        // now try to move to another subdirectory

        auto subdirId =
            service
                .CreateNode(
                    headers,
                    TCreateNodeArgs::Directory(RootNodeId, "subdir"))
                ->Record.GetNode()
                .GetId();

        renameNodeResponse = service.RenameNode(
            headers,
            RootNodeId,
            "file2",
            subdirId,
            "file2",
            0);

        // listing should show only subdir with file2 in it

        listNodesResponse = service.ListNodes(
            headers,
            fsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL("subdir", listNodesResponse.GetNames(0));

        listNodesResponse = service.ListNodes(headers, fsId, subdirId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL("file2", listNodesResponse.GetNames(0));

        // create 2 files in the same shard

        ui64 nodeId4 =
            service
                .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file4"))
                ->Record.GetNode()
                .GetId();

        // round robin is used for 2 clusters, so we just skip one shard by
        // creating a file in it
        service.CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file5"));

        ui64 nodeId6 =
            service
                .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file6"))
                ->Record.GetNode()
                .GetId();

        UNIT_ASSERT_VALUES_EQUAL(
            ExtractShardNo(nodeId4),
            ExtractShardNo(nodeId6));

        // now move to the same shard

        renameNodeResponse = service.RenameNode(
            headers,
            RootNodeId,
            "file4",
            RootNodeId,
            "file6",
            0);

        // file4 should not be present in the listing

        service.SendGetNodeAttrRequest(headers, fsId, RootNodeId, "file4");

        auto getNodeAttrResponse = service.RecvGetNodeAttrResponse();
        UNIT_ASSERT(getNodeAttrResponse);
        UNIT_ASSERT_C(
            FAILED(getNodeAttrResponse->GetStatus()),
            getNodeAttrResponse->GetErrorReason().c_str());
    }

    Y_UNIT_TEST(ShouldPerformLocksForExternalNodes)
    {
        NProto::TStorageConfig config;
        config.SetMultiTabletForwardingEnabled(true);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");

        ui64 nodeId = service
            .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
            ->Record.GetNode()
            .GetId();
        ui64 handle = service
            .CreateHandle(headers, fsId, nodeId, "", TCreateHandleArgs::RDWR)
            ->Record.GetHandle();

        service.AcquireLock(headers, fsId, handle, 1, 0, 4_KB);

        auto response =
            service.AssertAcquireLockFailed(headers, fsId, handle, 2, 0, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetCode(), E_FS_WOULDBLOCK);

        service.ReleaseLock(headers, fsId, handle, 2, 0, 4_KB);
        service.AcquireLock(headers, fsId, handle, 1, 0, 0);

        response =
            service.AssertAcquireLockFailed(headers, fsId, handle, 2, 0, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetCode(), E_FS_WOULDBLOCK);

        service.ReleaseLock(headers, fsId, handle, 1, 0, 0);

        // ShouldTrackSharedLocks

        service.AcquireLock(
            headers,
            fsId,
            handle,
            1,
            0,
            4_KB,
            DefaultPid,
            NProto::E_SHARED);
        service.AcquireLock(
            headers,
            fsId,
            handle,
            2,
            0,
            4_KB,
            DefaultPid,
            NProto::E_SHARED);

        response =
            service.AssertAcquireLockFailed(headers, fsId, handle, 1, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(
            response->GetError().GetCode(),
            E_FS_WOULDBLOCK);

        response = service.AssertAcquireLockFailed(headers, fsId, handle, 1, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetCode(), E_FS_WOULDBLOCK);

        service.AcquireLock(headers, fsId, handle, 3, 0, 4_KB, DefaultPid, NProto::E_SHARED);

        service.DestroyHandle(headers, fsId, nodeId, handle);

        handle = service
            .CreateHandle(headers, fsId, nodeId, "", TCreateHandleArgs::WRNLY)
            ->Record.GetHandle();

        service.AssertTestLockFailed(headers, fsId, handle, 1, 0, 4_KB, DefaultPid, NProto::E_SHARED);
        service.AssertAcquireLockFailed(headers, fsId, handle, 1, 0, 4_KB, DefaultPid, NProto::E_SHARED);
    }

    Y_UNIT_TEST(ShouldLinkExternalNodes)
    {
        NProto::TStorageConfig config;
        config.SetMultiTabletForwardingEnabled(true);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");

        auto headers1 = headers;
        headers1.FileSystemId = shard1Id;

        const auto nodeId1 =
            service
                .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file1"))
                ->Record.GetNode()
                .GetId();
        UNIT_ASSERT_VALUES_EQUAL(1, ExtractShardNo(nodeId1));

        auto node = service.GetNodeAttr(headers, fsId, RootNodeId, "file1")
                        ->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(1, node.GetLinks());

        auto linkNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::Link(RootNodeId, "file2", nodeId1));
        UNIT_ASSERT(linkNodeResponse);
        UNIT_ASSERT_C(
            SUCCEEDED(linkNodeResponse->GetStatus()),
            linkNodeResponse->GetErrorReason().c_str());

        // attributes of the linked node should be updated
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            linkNodeResponse->Record.GetNode().GetLinks());
        UNIT_ASSERT_VALUES_UNEQUAL(
            0,
            linkNodeResponse->Record.GetNode().GetCTime());

        // validate that the links field is incremented
        auto links = service.GetNodeAttr(headers, fsId, RootNodeId, "file1")
                         ->Record.GetNode().GetLinks();
        UNIT_ASSERT_VALUES_EQUAL(2, links);

        // get node attr should also work with the link
        links = service.GetNodeAttr(headers, fsId, RootNodeId, "file1")
                    ->Record.GetNode().GetLinks();
        UNIT_ASSERT_VALUES_EQUAL(2, links);

        // validate that reading from hardlinked file works
        auto data = GenerateValidateData(256_KB);
        ui64 handle = service
            .CreateHandle(headers, fsId, RootNodeId, "file1", TCreateHandleArgs::RDWR)
            ->Record.GetHandle();
        service.WriteData(headers, fsId, nodeId1, handle, 0, data);

        ui64 handle2 = service
            .CreateHandle(headers, fsId, RootNodeId, "file2", TCreateHandleArgs::RDWR)
            ->Record.GetHandle();

        auto data2 = service.ReadData(
            headers,
            fsId,
            linkNodeResponse->Record.GetNode().GetId(),
            handle2,
            0,
            data.size())->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL(data, data2);

        // Removal of both the file and a hardlink should remove file from both
        // the shard and a leader

        service.DestroyHandle(headers, fsId, nodeId1, handle);
        service.DestroyHandle(headers, fsId, linkNodeResponse->Record.GetNode().GetId(), handle2);

        service.UnlinkNode(headers, RootNodeId, "file1");
        service.UnlinkNode(headers, RootNodeId, "file2");

        // Now listing of the root should show no files

        auto listNodesResponse = service.ListNodes(
            headers,
            fsId,
            RootNodeId)->Record;
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());

        listNodesResponse = service.ListNodes(
            headers1,
            shard1Id,
            RootNodeId)->Record;
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());

        // GetNodeAttr should fail as well

        service.AssertGetNodeAttrFailed(headers, fsId, RootNodeId, "file1");
        service.AssertGetNodeAttrFailed(headers, fsId, RootNodeId, "file2");

        // Creating hardlinks to non-existing files should fail. It is
        // reasonable to assume that nodeId + 100 is not a valid node id.
        linkNodeResponse = service.AssertCreateNodeFailed(
            headers,
            TCreateNodeArgs::Link(RootNodeId, "file3", nodeId1 + 100));
    }

    Y_UNIT_TEST(ShouldForceDestroyWithAllowFileStoreForceDestroyFlag)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetAllowFileStoreForceDestroy(true);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");
        ui32 nodeIdx = env.CreateNode("nfs");
        const TString fsId = "test";
        const auto initialBlockCount = 1'000;
        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, initialBlockCount);

        auto headers = THeaders{fsId, "client", ""};
        auto createSessionResponse = service.CreateSession(headers);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            createSessionResponse->GetStatus(),
            createSessionResponse->GetErrorReason());
        auto destroyFileStoreResponse = service.DestroyFileStore(fsId, true);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            destroyFileStoreResponse->GetStatus(),
            destroyFileStoreResponse->GetErrorReason());
    }

    Y_UNIT_TEST(ForceDestroyWithoutAllowFileStoreForceDestroyFlagShouldFail)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");
        ui32 nodeIdx = env.CreateNode("nfs");
        const TString fsId = "test";
        const auto initialBlockCount = 1'000;
        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, initialBlockCount);

        auto headers = THeaders{fsId, "client", ""};
        auto createSessionResponse = service.CreateSession(headers);
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            createSessionResponse->GetStatus(),
            createSessionResponse->GetErrorReason());
        service.AssertDestroyFileStoreFailed(fsId, true);
    }

    Y_UNIT_TEST(ShouldAggregateFileSystemMetrics)
    {
        NProto::TStorageConfig config;
        config.SetMultiTabletForwardingEnabled(true);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");

        // creating 2 files

        auto createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"))->Record;

        const auto nodeId1 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(1, ExtractShardNo(nodeId1));

        createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file2"))->Record;

        const auto nodeId2 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL(2, ExtractShardNo(nodeId2));

        ui64 handle1 = service.CreateHandle(
            headers,
            fsId,
            nodeId1,
            "",
            TCreateHandleArgs::RDWR)->Record.GetHandle();

        auto data1 = GenerateValidateData(256_KB);
        service.WriteData(headers, fsId, nodeId1, handle1, 0, data1);

        ui64 handle2 = service.CreateHandle(
            headers,
            fsId,
            nodeId2,
            "",
            TCreateHandleArgs::RDWR)->Record.GetHandle();

        auto data2 = GenerateValidateData(512_KB);
        service.WriteData(headers, fsId, nodeId2, handle2, 0, data2);

        const auto fsStat = service.StatFileStore(headers, fsId)->Record;
        const auto& fileStore = fsStat.GetFileStore();
        UNIT_ASSERT_VALUES_EQUAL(fsId, fileStore.GetFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL(1'000, fileStore.GetBlocksCount());
        UNIT_ASSERT_VALUES_EQUAL(4_KB, fileStore.GetBlockSize());

        const auto& fileStoreStats = fsStat.GetStats();
        UNIT_ASSERT_VALUES_EQUAL(2, fileStoreStats.GetUsedNodesCount());
        UNIT_ASSERT_VALUES_EQUAL(
            768_KB / 4_KB,
            fileStoreStats.GetUsedBlocksCount());
    }

    Y_UNIT_TEST(ShouldRetryUnlinkingInShard)
    {
        NProto::TStorageConfig config;
        config.SetMultiTabletForwardingEnabled(true);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");

        const auto createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"))->Record;

        const auto nodeId1 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL((1LU << 56U) + 2, nodeId1);

        TAutoPtr<IEventHandle> shardUnlinkResponse;
        bool intercept = true;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvService::EvUnlinkNodeRequest) {
                    const auto* msg =
                        event->Get<TEvService::TEvUnlinkNodeRequest>();
                    if (intercept
                            && msg->Record.GetFileSystemId() == shard1Id)
                    {
                        auto response = std::make_unique<
                            TEvService::TEvUnlinkNodeResponse>(
                            MakeError(E_REJECTED, "error"));

                        shardUnlinkResponse = new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie);

                        return true;
                    }
                }
                return false;
            });

        const ui64 requestId = 111;
        service.SendUnlinkNodeRequest(
            headers,
            RootNodeId,
            "file1",
            false, // unlinkDirectory
            requestId);

        ui32 iterations = 0;
        while (!shardUnlinkResponse && iterations++ < 100) {
            env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT(shardUnlinkResponse);
        intercept = false;
        env.GetRuntime().Send(shardUnlinkResponse.Release());

        auto unlinkResponse = service.RecvUnlinkNodeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            unlinkResponse->GetError().GetCode(),
            unlinkResponse->GetError().GetMessage());

        auto listNodesResponse = service.ListNodes(
            headers,
            fsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());

        auto headers1 = headers;
        headers1.FileSystemId = shard1Id;

        listNodesResponse = service.ListNodes(
            headers1,
            shard1Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());

        // checking DupCache logic - just in case
        service.SendUnlinkNodeRequest(
            headers,
            RootNodeId,
            "file1",
            false, // unlinkDirectory
            requestId);

        unlinkResponse = service.RecvUnlinkNodeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            unlinkResponse->GetError().GetCode(),
            unlinkResponse->GetError().GetMessage());
    }

    Y_UNIT_TEST(ShouldRetryUnlinkingInShardUponLeaderRestart)
    {
        NProto::TStorageConfig config;
        config.SetMultiTabletForwardingEnabled(true);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);

        ui64 tabletId = -1;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeFileStoreResponse: {
                        using TDesc = TEvSSProxy::TEvDescribeFileStoreResponse;
                        const auto* msg = event->Get<TDesc>();
                        const auto& desc =
                            msg->PathDescription.GetFileStoreDescription();
                        if (desc.GetConfig().GetFileSystemId() == fsId) {
                            tabletId = desc.GetIndexTabletId();
                        }
                    }
                }

                return false;
            });

        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");

        const auto createNodeResponse = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"))->Record;

        const auto nodeId1 = createNodeResponse.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL((1LU << 56U) + 2, nodeId1);

        bool intercept = true;
        bool intercepted = false;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvService::EvUnlinkNodeRequest) {
                    const auto* msg =
                        event->Get<TEvService::TEvUnlinkNodeRequest>();
                    if (intercept
                            && msg->Record.GetFileSystemId() == shard1Id)
                    {
                        intercepted = true;
                        return true;
                    }
                }
                return false;
            });

        service.SendUnlinkNodeRequest(headers, RootNodeId, "file1");

        ui32 iterations = 0;
        while (!intercepted && iterations++ < 100) {
            env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT(intercepted);
        intercept = false;

        auto headers1 = headers;
        headers1.FileSystemId = shard1Id;

        auto listNodesResponse = service.ListNodes(
            headers1,
            shard1Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.RebootTablet();

        auto unlinkResponse = service.RecvUnlinkNodeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            unlinkResponse->GetError().GetCode(),
            unlinkResponse->GetError().GetMessage());

        // remaking session since CreateSessionActor doesn't do it by itself
        // because EvWakeup never arrives because Scheduling doesn't work by
        // default and RegistrationObservers get reset after RebootTablet
        headers = service.InitSession(fsId, "client");

        listNodesResponse = service.ListNodes(
            headers,
            fsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());

        listNodesResponse = service.ListNodes(
            headers1,
            shard1Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());
    }

    Y_UNIT_TEST(ShouldRetryUnlinkingInShardUponLeaderRestartForRenameNode)
    {
        NProto::TStorageConfig config;
        config.SetMultiTabletForwardingEnabled(true);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);

        ui64 tabletId = -1;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeFileStoreResponse: {
                        using TDesc = TEvSSProxy::TEvDescribeFileStoreResponse;
                        const auto* msg = event->Get<TDesc>();
                        const auto& desc =
                            msg->PathDescription.GetFileStoreDescription();
                        if (desc.GetConfig().GetFileSystemId() == fsId) {
                            tabletId = desc.GetIndexTabletId();
                        }
                    }
                }

                return false;
            });

        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");

        const auto createNodeResponse1 = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"))->Record;

        const auto nodeId1 = createNodeResponse1.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL((1LU << 56U) + 2, nodeId1);

        const auto createNodeResponse2 = service.CreateNode(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file2"))->Record;

        const auto nodeId2 = createNodeResponse2.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL((2LU << 56U) + 2, nodeId2);

        bool intercept = true;
        bool intercepted = false;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvService::EvUnlinkNodeRequest) {
                    const auto* msg =
                        event->Get<TEvService::TEvUnlinkNodeRequest>();
                    if (intercept
                            && msg->Record.GetFileSystemId() == shard2Id)
                    {
                        intercepted = true;
                        return true;
                    }
                }
                return false;
            });

        service.SendRenameNodeRequest(
            headers,
            RootNodeId,
            "file1",
            RootNodeId,
            "file2",
            0);

        ui32 iterations = 0;
        while (!intercepted && iterations++ < 100) {
            env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT(intercepted);
        intercept = false;

        auto headers2 = headers;
        headers2.FileSystemId = shard2Id;

        auto listNodesResponse = service.ListNodes(
            headers2,
            shard2Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.RebootTablet();

        auto renameResponse = service.RecvRenameNodeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            renameResponse->GetError().GetCode(),
            renameResponse->GetError().GetMessage());

        // remaking session since CreateSessionActor doesn't do it by itself
        // because EvWakeup never arrives because Scheduling doesn't work by
        // default and RegistrationObservers get reset after RebootTablet
        headers = service.InitSession(fsId, "client");

        listNodesResponse = service.ListNodes(
            headers,
            fsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL("file2", listNodesResponse.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            listNodesResponse.GetNodes(0).GetId());

        listNodesResponse = service.ListNodes(
            headers2,
            shard2Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());
    }

    Y_UNIT_TEST(ShouldRetryNodeCreationInShard)
    {
        NProto::TStorageConfig config;
        config.SetMultiTabletForwardingEnabled(true);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");

        TAutoPtr<IEventHandle> shardCreateResponse;
        bool intercept = true;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvService::EvCreateNodeRequest) {
                    const auto* msg =
                        event->Get<TEvService::TEvCreateNodeRequest>();
                    if (intercept
                            && msg->Record.GetFileSystemId() == shard1Id)
                    {
                        auto response = std::make_unique<
                            TEvService::TEvCreateNodeResponse>(
                            MakeError(E_REJECTED, "error"));

                        shardCreateResponse = new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie);

                        return true;
                    }
                }
                return false;
            });

        const ui64 requestId = 111;
        service.SendCreateNodeRequest(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"),
            requestId);

        ui32 iterations = 0;
        while (!shardCreateResponse && iterations++ < 100) {
            env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT(shardCreateResponse);
        intercept = false;
        env.GetRuntime().Send(shardCreateResponse.Release());

        auto createResponse = service.RecvCreateNodeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            createResponse->GetError().GetCode(),
            createResponse->GetError().GetMessage());

        const auto nodeId1 = createResponse->Record.GetNode().GetId();
        UNIT_ASSERT_VALUES_EQUAL((1LU << 56U) + 2, nodeId1);

        auto listNodesResponse = service.ListNodes(
            headers,
            fsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL("file1", listNodesResponse.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            listNodesResponse.GetNodes(0).GetId());

        // checking DupCache logic
        service.SendCreateNodeRequest(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"),
            requestId);

        createResponse = service.RecvCreateNodeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            createResponse->GetError().GetCode(),
            createResponse->GetError().GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            createResponse->Record.GetNode().GetId());
    }

    Y_UNIT_TEST(ShouldRetryNodeCreationInShardUponLeaderRestart)
    {
        NProto::TStorageConfig config;
        config.SetMultiTabletForwardingEnabled(true);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);

        ui64 tabletId = -1;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeFileStoreResponse: {
                        using TDesc = TEvSSProxy::TEvDescribeFileStoreResponse;
                        const auto* msg = event->Get<TDesc>();
                        const auto& desc =
                            msg->PathDescription.GetFileStoreDescription();
                        if (desc.GetConfig().GetFileSystemId() == fsId) {
                            tabletId = desc.GetIndexTabletId();
                        }
                    }
                }

                return false;
            });

        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");

        bool intercept = true;
        bool intercepted = false;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvService::EvCreateNodeRequest) {
                    const auto* msg =
                        event->Get<TEvService::TEvCreateNodeRequest>();
                    if (intercept
                            && msg->Record.GetFileSystemId() == shard1Id)
                    {
                        intercepted = true;
                        return true;
                    }
                }
                return false;
            });

        const ui64 requestId = 111;
        service.SendCreateNodeRequest(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"),
            requestId);

        ui32 iterations = 0;
        while (!intercepted && iterations++ < 100) {
            env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT(intercepted);
        intercept = false;

        // TODO listNodes in leader?

        auto headers1 = headers;
        headers1.FileSystemId = shard1Id;

        auto listNodesResponse = service.ListNodes(
            headers1,
            shard1Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(0, listNodesResponse.NodesSize());

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.RebootTablet();

        auto createResponse = service.RecvCreateNodeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            createResponse->GetError().GetCode(),
            createResponse->GetError().GetMessage());

        // remaking session since CreateSessionActor doesn't do it by itself
        // because EvWakeup never arrives because Scheduling doesn't work by
        // default and RegistrationObservers get reset after RebootTablet
        // restoreClientSession = true
        headers = service.InitSession(fsId, "client", {}, true);

        listNodesResponse = service.ListNodes(
            headers,
            fsId,
            RootNodeId)->Record;

        const ui64 nodeId1 = (1LU << 56U) + 2;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL("file1", listNodesResponse.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            listNodesResponse.GetNodes(0).GetId());

        listNodesResponse = service.ListNodes(
            headers1,
            shard1Id,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());

        // checking DupCache logic
        service.SendCreateNodeRequest(
            headers,
            TCreateNodeArgs::File(RootNodeId, "file1"),
            requestId);

        createResponse = service.RecvCreateNodeResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            createResponse->GetError().GetCode(),
            createResponse->GetError().GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            createResponse->Record.GetNode().GetId());
    }

    Y_UNIT_TEST(ShouldRetryNodeCreationInShardUponCreateHandle)
    {
        NProto::TStorageConfig config;
        config.SetMultiTabletForwardingEnabled(true);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");

        TAutoPtr<IEventHandle> shardCreateResponse;
        bool intercept = true;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvService::EvCreateNodeRequest) {
                    const auto* msg =
                        event->Get<TEvService::TEvCreateNodeRequest>();
                    if (intercept
                            && msg->Record.GetFileSystemId() == shard1Id)
                    {
                        auto response = std::make_unique<
                            TEvService::TEvCreateNodeResponse>(
                            MakeError(E_REJECTED, "error"));

                        shardCreateResponse = new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie);

                        return true;
                    }
                }
                return false;
            });

        const ui64 requestId = 111;
        service.SendCreateHandleRequest(
            headers,
            fsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::CREATE,
            "", // shardId
            requestId);

        ui32 iterations = 0;
        while (!shardCreateResponse && iterations++ < 100) {
            env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT(shardCreateResponse);
        intercept = false;
        env.GetRuntime().Send(shardCreateResponse.Release());

        auto createHandleResponse = service.RecvCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            createHandleResponse->GetError().GetCode(),
            createHandleResponse->GetError().GetMessage());

        const auto nodeId1 = createHandleResponse->Record.GetNodeAttr().GetId();
        UNIT_ASSERT_VALUES_EQUAL((1LU << 56U) + 2, nodeId1);

        auto listNodesResponse = service.ListNodes(
            headers,
            fsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL("file1", listNodesResponse.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            listNodesResponse.GetNodes(0).GetId());

        // checking DupCache logic
        service.SendCreateHandleRequest(
            headers,
            fsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::CREATE,
            "", // shardId
            requestId);

        createHandleResponse = service.RecvCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            createHandleResponse->GetError().GetCode(),
            createHandleResponse->GetError().GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            createHandleResponse->Record.GetNodeAttr().GetId());
    }

    Y_UNIT_TEST(ShouldRetryNodeCreationInShardUponCreateHandleUponLeaderRestart)
    {
        NProto::TStorageConfig config;
        config.SetMultiTabletForwardingEnabled(true);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto shard1Id = fsId + "-f1";
        const auto shard2Id = fsId + "-f2";

        TServiceClient service(env.GetRuntime(), nodeIdx);

        ui64 tabletId = -1;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeFileStoreResponse: {
                        using TDesc = TEvSSProxy::TEvDescribeFileStoreResponse;
                        const auto* msg = event->Get<TDesc>();
                        const auto& desc =
                            msg->PathDescription.GetFileStoreDescription();
                        if (desc.GetConfig().GetFileSystemId() == fsId) {
                            tabletId = desc.GetIndexTabletId();
                        }
                    }
                }

                return false;
            });

        service.CreateFileStore(fsId, 1'000);
        service.CreateFileStore(shard1Id, 1'000);
        service.CreateFileStore(shard2Id, 1'000);

        ConfigureShards(service, fsId, shard1Id, shard2Id);

        auto headers = service.InitSession(fsId, "client");

        bool intercept = true;
        bool intercepted = false;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvService::EvCreateNodeRequest) {
                    const auto* msg =
                        event->Get<TEvService::TEvCreateNodeRequest>();
                    if (intercept
                            && msg->Record.GetFileSystemId() == shard1Id)
                    {
                        intercepted = true;
                        return true;
                    }
                }
                return false;
            });

        const ui64 requestId = 111;
        service.SendCreateHandleRequest(
            headers,
            fsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::CREATE,
            "", // shardId
            requestId);

        ui32 iterations = 0;
        while (!intercepted && iterations++ < 100) {
            env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT(intercepted);
        intercept = false;

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.RebootTablet();

        auto createHandleResponse = service.RecvCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            createHandleResponse->GetError().GetCode(),
            createHandleResponse->GetError().GetMessage());

        const auto nodeId1 = (1LU << 56U) + 2;

        // remaking session since CreateSessionActor doesn't do it by itself
        // because EvWakeup never arrives because Scheduling doesn't work by
        // default and RegistrationObservers get reset after RebootTablet
        // restoreClientSession = true
        headers = service.InitSession(fsId, "client", {}, true);

        auto listNodesResponse = service.ListNodes(
            headers,
            fsId,
            RootNodeId)->Record;

        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL("file1", listNodesResponse.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            listNodesResponse.GetNodes(0).GetId());

        // checking DupCache logic
        service.SendCreateHandleRequest(
            headers,
            fsId,
            RootNodeId,
            "file1",
            TCreateHandleArgs::CREATE,
            "", // shardId
            requestId);

        createHandleResponse = service.RecvCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            createHandleResponse->GetError().GetCode(),
            createHandleResponse->GetError().GetMessage());
        UNIT_ASSERT_VALUES_EQUAL(
            nodeId1,
            createHandleResponse->Record.GetNodeAttr().GetId());
    }

    Y_UNIT_TEST(ShouldUseAliasesForRequestsForwarding)
    {
        const TString originalFs = "test";
        const TString mirroredFs = "test-mirrored";

        NProto::TStorageConfig::TFilestoreAliasEntry entry;
        entry.SetAlias(mirroredFs);
        entry.SetFsId(originalFs);
        NProto::TStorageConfig::TFilestoreAliases aliases;
        aliases.MutableEntries()->Add(std::move(entry));

        NProto::TStorageConfig config;
        config.MutableFilestoreAliases()->Swap(&aliases);

        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");


        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(originalFs, 1000);
        service.CreateFileStore(mirroredFs, 1000);

        auto originalHeaders = service.InitSession(originalFs, "client");
        auto mirroredHeaders = service.InitSession(mirroredFs, "client");

        // Create file in the original fs
        service.CreateNode(originalHeaders, TCreateNodeArgs::File(RootNodeId, "testfile"));

        // Check that the file is visible in the mirrored fs
        auto listNodesResponse =
            service.ListNodes(mirroredHeaders, mirroredFs, RootNodeId)->Record;
        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL(1, listNodesResponse.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL("testfile", listNodesResponse.GetNames(0));

        auto nodeId = listNodesResponse.GetNodes(0).GetId();

        // write to the file in the mirrored fs
        auto mirroredHandle = service.CreateHandle(
            mirroredHeaders,
            mirroredFs,
            nodeId,
            "",
            TCreateHandleArgs::WRNLY)->Record.GetHandle();
        auto data = GenerateValidateData(256_KB);
        service.WriteData(mirroredHeaders, mirroredFs, nodeId, mirroredHandle, 0, data);

        // validate that written data can be read from the original fs
        auto originalHandle = service.CreateHandle(
            originalHeaders,
            originalFs,
            nodeId,
            "",
            TCreateHandleArgs::RDNLY)->Record.GetHandle();
        auto readData = service.ReadData(
            originalHeaders,
            originalFs,
            nodeId,
            originalHandle,
            0,
            256_KB)->Record.GetBuffer();
        UNIT_ASSERT_VALUES_EQUAL(data, readData);
    }

    Y_UNIT_TEST(ShouldWriteCompactionMap)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        ui64 tabletId = -1;
        ui32 lastCompactionMapRangeId = 0;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvDescribeFileStoreResponse: {
                        using TDesc = TEvSSProxy::TEvDescribeFileStoreResponse;
                        const auto* msg = event->Get<TDesc>();
                        const auto& desc =
                            msg->PathDescription.GetFileStoreDescription();
                        tabletId = desc.GetIndexTabletId();
                        break;
                    }
                    case TEvIndexTabletPrivate::
                        EvLoadCompactionMapChunkResponse: {
                        lastCompactionMapRangeId = Max(
                            event
                                ->Get<TEvIndexTabletPrivate::
                                          TEvLoadCompactionMapChunkResponse>()
                                ->LastRangeId,
                            lastCompactionMapRangeId);
                        break;
                    }
                }

                return false;
        });

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore("test", 1'000);

        NProtoPrivate::TWriteCompactionMapRequest request;
        request.SetFileSystemId("test");
        for (ui32 i = 4; i < 30; ++i) {
            NProtoPrivate::TCompactionRangeStats range;
            range.SetRangeId(i);
            range.SetBlobCount(1);
            range.SetDeletionCount(2);
            *request.AddRanges() = range;
        }

        TString buf;
        google::protobuf::util::MessageToJsonString(request, &buf);
        auto jsonResponse = service.ExecuteAction("writecompactionmap", buf);
        NProtoPrivate::TWriteCompactionMapResponse response;
        UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
            jsonResponse->Record.GetOutput(),
            &response).ok());

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.RebootTablet();

        UNIT_ASSERT_VALUES_EQUAL(lastCompactionMapRangeId, 29);
    }

    void CheckDisableMultistageReadWritesForHdd(NProto::EStorageMediaKind kind)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        const TString fs = "test";
        service.CreateFileStore(fs, 1000, DefaultBlockSize, kind);

        {
            NProto::TStorageConfig newConfig;
            newConfig.SetTwoStageReadEnabled(true);
            newConfig.SetThreeStageWriteEnabled(true);
            newConfig.SetThreeStageWriteDisabledForHDD(true);
            newConfig.SetThreeStageWriteThreshold(1);
            newConfig.SetTwoStageReadDisabledForHDD(true);

            const auto response =
                ExecuteChangeStorageConfig(std::move(newConfig), service);

            UNIT_ASSERT_VALUES_EQUAL(
                true,
                response.GetStorageConfig().GetThreeStageWriteEnabled());
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                response.GetStorageConfig().GetThreeStageWriteDisabledForHDD());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                response.GetStorageConfig().GetThreeStageWriteThreshold());
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                response.GetStorageConfig().GetTwoStageReadEnabled());
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                response.GetStorageConfig().GetTwoStageReadDisabledForHDD());

            TDispatchOptions options;
            env.GetRuntime().DispatchEvents(options, TDuration::Seconds(1));
        }

        auto headers = service.InitSession(fs, "client");
        ui64 nodeId = service
            .CreateNode(headers, TCreateNodeArgs::File(RootNodeId, "file"))
            ->Record.GetNode()
            .GetId();
        ui64 handle = service
            .CreateHandle(headers, fs, nodeId, "", TCreateHandleArgs::RDWR)
            ->Record.GetHandle();

        auto data = GenerateValidateData(2 * DefaultBlockSize);
        service.WriteData(headers, fs, nodeId, handle, 0, data);

        auto readDataResult =
            service
                .ReadData(headers, fs, nodeId, handle, 0, data.size());

        UNIT_ASSERT_VALUES_EQUAL(data, readDataResult->Record.GetBuffer());

        auto counters = env.GetCounters()
            ->FindSubgroup("component", "service_fs")
            ->FindSubgroup("host", "cluster")
            ->FindSubgroup("filesystem", fs)
            ->FindSubgroup("client", "client");
        {
            auto subgroup = counters->FindSubgroup("request", "GenerateBlobIds");
            UNIT_ASSERT(subgroup);
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                subgroup->GetCounter("Count")->GetAtomic());
        }
        {
            auto subgroup = counters->FindSubgroup("request", "AddData");
            UNIT_ASSERT(subgroup);
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                subgroup->GetCounter("Count")->GetAtomic());
        }
        {
            auto subgroup = counters->FindSubgroup("request", "WriteData");
            UNIT_ASSERT(subgroup);
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                subgroup->GetCounter("Count")->GetAtomic());
        }
        {
            auto subgroup = counters->FindSubgroup("request", "WriteBlob");
            UNIT_ASSERT(subgroup);
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                subgroup->GetCounter("Count")->GetAtomic());
        }
        {
            auto subgroup = counters->FindSubgroup("request", "DescribeData");
            UNIT_ASSERT(subgroup);
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                subgroup->GetCounter("Count")->GetAtomic());
        }
        {
            auto subgroup = counters->FindSubgroup("request", "ReadBlob");
            UNIT_ASSERT(subgroup);
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                subgroup->GetCounter("Count")->GetAtomic());
        }
        {
            auto subgroup = counters->FindSubgroup("request", "ReadData");
            UNIT_ASSERT(subgroup);
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                subgroup->GetCounter("Count")->GetAtomic());
        }
    }

    Y_UNIT_TEST(ShouldNotPerformThreeStageWritesAndTwoStageReadsForHddIfDisabled)
    {
        CheckDisableMultistageReadWritesForHdd(NProto::STORAGE_MEDIA_HDD);
    }

    Y_UNIT_TEST(ShouldNotPerformThreeStageWritesAndTwoStageReadsForHybridIfDisabled)
    {
        CheckDisableMultistageReadWritesForHdd(NProto::STORAGE_MEDIA_HYBRID);
    }

    Y_UNIT_TEST(ShouldNotAffectSsdReadWritesIfMultistageReadWritesAreOffForHdd)
    {
        CheckThreeStageWrites(NProto::STORAGE_MEDIA_SSD, true);
        CheckTwoStageReads(NProto::STORAGE_MEDIA_SSD, true);
    }

    void DoTestShardedFileSystemConfigured(
        const TString& fsId,
        TServiceClient& service)
    {
        TVector<TString> expected = {fsId, fsId + "_s1", fsId + "_s2"};

        auto response = service.ListFileStores();
        const auto& fsIds = response->Record.GetFileStores();
        TVector<TString> ids(fsIds.begin(), fsIds.end());
        Sort(ids);

        UNIT_ASSERT_VALUES_EQUAL(expected, ids);

        auto headers = service.InitSession(fsId, "client");

        for (const auto& id: ids) {
            NProtoPrivate::TDescribeSessionsRequest request;
            request.SetFileSystemId(id);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("describesessions", buf);
            NProtoPrivate::TDescribeSessionsResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());

            const auto& sessions = response.GetSessions();
            UNIT_ASSERT_VALUES_EQUAL(1, sessions.size());

            UNIT_ASSERT_VALUES_EQUAL(
                headers.SessionId,
                sessions[0].GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL(
                headers.ClientId,
                sessions[0].GetClientId());
            UNIT_ASSERT_VALUES_EQUAL("", sessions[0].GetSessionState());
        }

        const TString sessionState = "some_state";
        service.ResetSession(headers, sessionState);

        for (const auto& id: ids) {
            NProtoPrivate::TDescribeSessionsRequest request;
            request.SetFileSystemId(id);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("describesessions", buf);
            NProtoPrivate::TDescribeSessionsResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());

            const auto& sessions = response.GetSessions();
            UNIT_ASSERT_VALUES_EQUAL(1, sessions.size());

            UNIT_ASSERT_VALUES_EQUAL(
                headers.SessionId,
                sessions[0].GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL(
                headers.ClientId,
                sessions[0].GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(
                sessionState,
                sessions[0].GetSessionState());
        }

        service.DestroySession(headers);

        for (const auto& id: ids) {
            NProtoPrivate::TDescribeSessionsRequest request;
            request.SetFileSystemId(id);

            TString buf;
            google::protobuf::util::MessageToJsonString(request, &buf);
            auto jsonResponse = service.ExecuteAction("describesessions", buf);
            NProtoPrivate::TDescribeSessionsResponse response;
            UNIT_ASSERT(google::protobuf::util::JsonStringToMessage(
                jsonResponse->Record.GetOutput(), &response).ok());

            const auto& sessions = response.GetSessions();
            UNIT_ASSERT_VALUES_EQUAL(0, sessions.size());
        }
    }

    Y_UNIT_TEST(ShouldConfigureShardsAutomatically)
    {
        NProto::TStorageConfig config;
        config.SetAutomaticShardCreationEnabled(true);
        config.SetMaxShardSize(1_GB);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";

        TServiceClient service(env.GetRuntime(), nodeIdx);
        service.CreateFileStore(fsId, 2_GB / 4_KB);

        DoTestShardedFileSystemConfigured(fsId, service);
    }

    Y_UNIT_TEST(ShouldHandleErrorsDuringShardedFileSystemCreation)
    {
        NProto::TStorageConfig config;
        config.SetAutomaticShardCreationEnabled(true);
        config.SetMaxShardSize(1_GB);
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        const TString fsId = "test";
        const auto blockCount = 2_GB / 4_KB;

        TServiceClient service(env.GetRuntime(), nodeIdx);

        TVector<TString> expected = {fsId, fsId + "_s1", fsId + "_s2"};

        NProto::TError createShardError;
        NProto::TError configureShardError;
        NProto::TError configureShardsError;

        TAutoPtr<IEventHandle> toSend;

        env.GetRuntime().SetEventFilter(
            [&] (TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvSSProxy::EvCreateFileStoreRequest: {
                        using TRequest = TEvSSProxy::TEvCreateFileStoreRequest;
                        using TResponse =
                            TEvSSProxy::TEvCreateFileStoreResponse;
                        const auto* msg = event->Get<TRequest>();
                        if (msg->Config.GetFileSystemId() != expected[1]) {
                            break;
                        }

                        if (!HasError(createShardError)) {
                            break;
                        }

                        auto response = std::make_unique<TResponse>(
                            createShardError);

                        toSend = new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie);

                        return true;
                    }

                    case TEvIndexTablet::EvConfigureAsShardRequest: {
                        using TRequest =
                            TEvIndexTablet::TEvConfigureAsShardRequest;
                        using TResponse =
                            TEvIndexTablet::TEvConfigureAsShardResponse;
                        const auto* msg = event->Get<TRequest>();
                        if (msg->Record.GetFileSystemId() != expected[1]) {
                            break;
                        }

                        if (!HasError(configureShardError)) {
                            break;
                        }

                        auto response = std::make_unique<TResponse>(
                            configureShardError);

                        toSend = new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie);

                        return true;
                    }

                    case TEvIndexTablet::EvConfigureShardsRequest: {
                        using TResponse =
                            TEvIndexTablet::TEvConfigureShardsResponse;

                        if (!HasError(configureShardsError)) {
                            break;
                        }

                        auto response = std::make_unique<TResponse>(
                            configureShardsError);

                        toSend = new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie);

                        return true;
                    }
                }

                return false;
            });

        createShardError = MakeError(E_REJECTED, "failed to create shard");
        service.SendCreateFileStoreRequest(fsId, blockCount);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT(toSend);
        env.GetRuntime().Send(toSend, nodeIdx);
        {
            auto response = service.RecvCreateFileStoreResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                FormatError(createShardError),
                FormatError(response->GetError()));
        }

        createShardError = {};
        configureShardError =
            MakeError(E_REJECTED, "failed to configure shard");
        service.SendCreateFileStoreRequest(fsId, blockCount);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT(toSend);
        env.GetRuntime().Send(toSend, nodeIdx);
        {
            auto response = service.RecvCreateFileStoreResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                FormatError(configureShardError),
                FormatError(response->GetError()));
        }

        configureShardError = {};
        configureShardsError =
            MakeError(E_REJECTED, "failed to configure shards");
        service.SendCreateFileStoreRequest(fsId, blockCount);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT(toSend);
        env.GetRuntime().Send(toSend, nodeIdx);
        {
            auto response = service.RecvCreateFileStoreResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                FormatError(configureShardsError),
                FormatError(response->GetError()));
        }

        configureShardsError = {};
        service.SendCreateFileStoreRequest(fsId, blockCount);
        {
            auto response = service.RecvCreateFileStoreResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                FormatError(MakeError(S_OK)),
                FormatError(response->GetError()));
        }

        DoTestShardedFileSystemConfigured(fsId, service);
    }
}

}   // namespace NCloud::NFileStore::NStorage
