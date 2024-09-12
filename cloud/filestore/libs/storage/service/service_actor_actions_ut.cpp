#include "service.h"

#include <cloud/filestore/libs/storage/testlib/service_client.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>
#include <cloud/filestore/private/api/protos/actions.pb.h>

#include <contrib/libs/protobuf/src/google/protobuf/stubs/stringpiece.h>

namespace NCloud::NFileStore::NStorage {

using namespace NKikimr;
using namespace std::string_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

[[maybe_unused]] TString ToString(const NProtoBuf::StringPiece& piece)
{
    return piece.ToString();
}

NProto::TStorageConfig ExecuteGetStorageConfig(
    const TString& fsId,
    TServiceClient& service)
{
    NProtoPrivate::TGetStorageConfigRequest request;
    request.SetFileSystemId(fsId);

    TString buf;
    google::protobuf::util::MessageToJsonString(request, &buf);

    auto jsonResponse = service.ExecuteAction("getstorageconfig", buf);
    UNIT_ASSERT_VALUES_EQUAL(S_OK, jsonResponse->GetStatus());

    NProto::TStorageConfig response;
    auto status = google::protobuf::util::JsonStringToMessage(
        jsonResponse->Record.GetOutput(),
        &response);
    UNIT_ASSERT_C(status.ok(), ToString(status.message()));
    return response;
}

NProtoPrivate::TChangeStorageConfigResponse ExecuteChangeStorageConfig(
    const TString& fsId,
    NProto::TStorageConfig config,
    TServiceClient& service,
    bool mergeWithConfig = false)
{
    NProtoPrivate::TChangeStorageConfigRequest request;
    request.SetFileSystemId(fsId);

    *request.MutableStorageConfig() = std::move(config);
    request.SetMergeWithStorageConfigFromTabletDB(mergeWithConfig);

    TString buf;
    google::protobuf::util::MessageToJsonString(request, &buf);

    auto jsonResponse = service.ExecuteAction("changestorageconfig", buf);
    UNIT_ASSERT_VALUES_EQUAL(S_OK, jsonResponse->GetStatus());

    NProtoPrivate::TChangeStorageConfigResponse response;
    auto status = google::protobuf::util::JsonStringToMessage(
        jsonResponse->Record.GetOutput(),
        &response);
    UNIT_ASSERT_C(status.ok(), ToString(status.message()));

    return response;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStorageServiceActionsTest)
{
    Y_UNIT_TEST(ShouldFail)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);
        auto response = service.AssertExecuteActionFailed("NonExistingAction", "{}");

        UNIT_ASSERT_VALUES_UNEQUAL(S_OK, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldDrainTablets)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);

        ui64 observedNodeId = 0;
        bool observedKeepDown = false;
        env.GetRuntime().SetObserverFunc([&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvHive::EvDrainNode: {
                        auto* msg = event->Get<TEvHive::TEvDrainNode>();
                        observedNodeId = msg->Record.GetNodeID();
                        observedKeepDown = msg->Record.GetKeepDown();
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        static constexpr auto DrainTabletsActionName = "draintablets";
        {
            NProtoPrivate::TDrainNodeRequest request;
            TString requestJson;
            google::protobuf::util::MessageToJsonString(request, &requestJson);

            auto response = service.ExecuteAction(DrainTabletsActionName, requestJson);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());

            UNIT_ASSERT_VALUES_EQUAL(service.GetSender().NodeId(), observedNodeId);
            UNIT_ASSERT(!observedKeepDown);
        }

        {
            NProtoPrivate::TDrainNodeRequest request;
            request.SetKeepDown(true);
            TString requestJson;
            google::protobuf::util::MessageToJsonString(request, &requestJson);

            auto response = service.ExecuteAction(DrainTabletsActionName, requestJson);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(service.GetSender().NodeId(), observedNodeId);
            UNIT_ASSERT(observedKeepDown);
        }
    }

    Y_UNIT_TEST(ShouldGetStorageConfigFromNodeOrFs)
    {
        NProto::TStorageConfig config;
        config.SetReadAheadCacheMaxNodes(42);

        TTestEnv env{{}, config};
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TServiceClient service(env.GetRuntime(), nodeIdx);

        service.CreateFileStore("fs0", 1'000);

        auto response = ExecuteGetStorageConfig("", service);
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(
            response, env.GetStorageConfig()->GetStorageConfigProto()));

        UNIT_ASSERT_VALUES_EQUAL(
            42,
            response.GetReadAheadCacheMaxNodes());

        {
            NProto::TStorageConfig newConfig;
            newConfig.SetMultiTabletForwardingEnabled(true);
            const auto response = ExecuteChangeStorageConfig(
                "fs0",
                std::move(newConfig),
                service);
            UNIT_ASSERT_VALUES_EQUAL(
                response.GetStorageConfig().GetMultiTabletForwardingEnabled(),
                true);
        }

        {
            auto response = ExecuteGetStorageConfig("fs0", service);

            UNIT_ASSERT_VALUES_EQUAL(
                response.GetMultiTabletForwardingEnabled(),
                true);
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
