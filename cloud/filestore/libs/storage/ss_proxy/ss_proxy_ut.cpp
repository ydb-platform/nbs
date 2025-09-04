#include "ss_proxy.h"

#include <cloud/filestore/libs/storage/testlib/helpers.h>
#include <cloud/filestore/libs/storage/testlib/ss_proxy_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <contrib/ydb/core/protos/flat_scheme_op.pb.h>
#include <contrib/ydb/core/protos/schemeshard/operations.pb.h>
#include <contrib/ydb/core/tx/tx_proxy/proxy.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

NKikimrSchemeOp::TModifyScheme CreateDir(const TString& path)
{
    TStringBuf workingDir;
    TStringBuf name;
    TStringBuf(path).RSplit('/', workingDir, name);

    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpMkDir);
    modifyScheme.SetWorkingDir(TString(workingDir));

    auto* op = modifyScheme.MutableMkDir();
    op->SetName(TString(name));

    return modifyScheme;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSSProxyTest)
{
    Y_UNIT_TEST(ShouldCreateDirectories)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TSSProxyClient ssProxy(env.GetStorageConfig(), env.GetRuntime(), nodeIdx);
        ssProxy.ModifyScheme(CreateDir("/local/foo"));
        ssProxy.ModifyScheme(CreateDir("/local/bar"));

        ssProxy.DescribeScheme("/local/foo");
        ssProxy.DescribeScheme("/local/bar");
    }

    Y_UNIT_TEST(ShouldCreateFileStore)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TSSProxyClient ssProxy(env.GetStorageConfig(), env.GetRuntime(), nodeIdx);
        ssProxy.CreateFileStore("test1", 1000);
        ssProxy.CreateFileStore("test2", 2000);

        ssProxy.DescribeFileStore("test1");
        ssProxy.DescribeFileStore("test2");

        ssProxy.DestroyFileStore("test1");
        ssProxy.DestroyFileStore("test2");
    }

    Y_UNIT_TEST(ShouldDestroyNonExistentFilestore)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TSSProxyClient ssProxy(env.GetStorageConfig(), env.GetRuntime(), nodeIdx);
        auto response = ssProxy.DestroyFileStore("nonexistent");
        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetCode(), S_FALSE);
    }

    Y_UNIT_TEST(ShouldReturnERejectedIfIfSchemeShardDetectsPathIdVersionMismatch)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TSSProxyClient ssProxy(env.GetStorageConfig(), env.GetRuntime(), nodeIdx);
        ssProxy.CreateFileStore("test", 2000);

        auto describe = ssProxy.DescribeFileStore("test");
        UNIT_ASSERT_C(Succeeded(describe), GetErrorReason(describe));

        const auto& pathDescription = describe->PathDescription;
        UNIT_ASSERT_EQUAL(
            NKikimrSchemeOp::EPathTypeFileStore,
            pathDescription.GetSelf().GetPathType());

        const auto& pathId = pathDescription.GetSelf().GetPathId();
        const auto& pathVersion = pathDescription.GetSelf().GetPathVersion();

        TString fsDir;
        TString fsName;

        {
            TStringBuf dir;
            TStringBuf name;
            TStringBuf(describe->Path).RSplit('/', dir, name);
            fsDir = TString{dir};
            fsName = TString{name};
        }

        NKikimrSchemeOp::TModifyScheme modifyScheme;
        modifyScheme.SetWorkingDir(fsDir);
        modifyScheme.SetOperationType(
            NKikimrSchemeOp::ESchemeOpAlterFileStore);

        auto* op = modifyScheme.MutableAlterFileStore();
        op->SetName(fsName);

        NKikimrFileStore::TConfig config;
        config.SetCloudId("cloud");
        config.SetVersion(10);

        op->MutableConfig()->CopyFrom(config);

        auto* applyIf = modifyScheme.MutableApplyIf()->Add();
        applyIf->SetPathId(pathId);
        applyIf->SetPathVersion(pathVersion + 1);

        ssProxy.SendRequest(
            MakeSSProxyServiceId(),
            std::make_unique<TEvStorageSSProxy::TEvModifySchemeRequest>(
                std::move(modifyScheme)));

        auto modifyResponse =
            ssProxy.RecvResponse<TEvStorageSSProxy::TEvModifySchemeResponse>();

        UNIT_ASSERT_C(FAILED(modifyResponse->GetStatus()), GetErrorReason(modifyResponse));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ABORTED,
            modifyResponse->GetError().GetCode(),
            modifyResponse->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldReturnConcurrentModificationErrorIfSchemeShardDetectsWrongVersion)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TSSProxyClient ssProxy(env.GetStorageConfig(), env.GetRuntime(), nodeIdx);
        ssProxy.CreateFileStore("test", 2000);

        auto describe = ssProxy.DescribeFileStore("test");
        UNIT_ASSERT_C(Succeeded(describe), GetErrorReason(describe));

        const auto& pathDescription = describe->PathDescription;
        UNIT_ASSERT_EQUAL(
            NKikimrSchemeOp::EPathTypeFileStore,
            pathDescription.GetSelf().GetPathType());

        const auto& pathId = pathDescription.GetSelf().GetPathId();
        const auto& pathVersion = pathDescription.GetSelf().GetPathVersion();

        TString fsDir;
        TString fsName;

        {
            TStringBuf dir;
            TStringBuf name;
            TStringBuf(describe->Path).RSplit('/', dir, name);
            fsDir = TString{dir};
            fsName = TString{name};
        }

        NKikimrSchemeOp::TModifyScheme modifyScheme;
        modifyScheme.SetWorkingDir(fsDir);
        modifyScheme.SetOperationType(
            NKikimrSchemeOp::ESchemeOpAlterFileStore);

        auto* op = modifyScheme.MutableAlterFileStore();
        op->SetName(fsName);

        NKikimrFileStore::TConfig config;
        config.SetCloudId("cloud");

        op->MutableConfig()->CopyFrom(config);

        auto* applyIf = modifyScheme.MutableApplyIf()->Add();
        applyIf->SetPathId(pathId);
        applyIf->SetPathVersion(pathVersion + 1);

        ssProxy.SendRequest(
            MakeSSProxyServiceId(),
            std::make_unique<TEvStorageSSProxy::TEvModifySchemeRequest>(
                std::move(modifyScheme)));

        auto modifyResponse =
            ssProxy.RecvResponse<TEvStorageSSProxy::TEvModifySchemeResponse>();

        UNIT_ASSERT_C(FAILED(modifyResponse->GetStatus()), GetErrorReason(modifyResponse));
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            modifyResponse->GetError().GetCode(),
            modifyResponse->GetErrorReason());
    }

    Y_UNIT_TEST(ShouldFailFSCreationIfDescribeSchemeFails)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TSSProxyClient ssProxy(env.GetStorageConfig(), env.GetRuntime(), nodeIdx);
        ssProxy.CreateFileStore("test", 2000);

        auto& runtime = env.GetRuntime();
        auto error = MakeError(E_FAIL, "Error");
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvStorageSSProxy::EvDescribeSchemeRequest: {
                        auto response = std::make_unique<TEvStorageSSProxy::TEvDescribeSchemeResponse>(
                                error);

                        runtime.Send(
                            new NActors::IEventHandle(
                                event->Sender,
                                event->Recipient,
                                response.release()),
                            nodeIdx);

                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        ssProxy.SendDescribeFileStoreRequest("test");

        auto response = ssProxy.RecvResponse<TEvSSProxy::TEvDescribeFileStoreResponse>();
        UNIT_ASSERT_VALUES_EQUAL(response->GetStatus(), error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(response->GetErrorReason(), error.GetMessage());
    }

    Y_UNIT_TEST(ShouldFailDecsribeVolumeIfSSTimesOut)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        auto& runtime = env.GetRuntime();
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvTxUserProxy::EvNavigate: {
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        TSSProxyClient ssProxy(env.GetStorageConfig(), env.GetRuntime(), nodeIdx);

        ssProxy.CreateFileStore("test", 1000);
        ssProxy.SendDescribeFileStoreRequest("test");

        auto response = ssProxy.RecvDescribeFileStoreResponse();
        UNIT_ASSERT_C(!Succeeded(response), GetErrorReason(response));

        UNIT_ASSERT_VALUES_EQUAL_C(
            E_TIMEOUT,
            response->GetStatus(),
            response->GetErrorReason()
        );
    }

    Y_UNIT_TEST(ShouldReturnERejectedIfIndexTabletIdIsZero)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TSSProxyClient ssProxy(env.GetStorageConfig(), env.GetRuntime(), nodeIdx);
        ssProxy.CreateFileStore("test", 2000);

        auto& runtime = env.GetRuntime();
        runtime.SetEventFilter([&] (auto& runtime, auto& ev) {
                Y_UNUSED(runtime);
                switch (ev->GetTypeRewrite()) {
                    case TEvStorageSSProxy::EvDescribeSchemeResponse: {
                        using TEvent = TEvStorageSSProxy::TEvDescribeSchemeResponse;
                        using TDescription = NKikimrSchemeOp::TPathDescription;
                        auto* msg = ev->template Get<TEvent>();
                        auto& desc =
                            const_cast<TDescription&>(msg->PathDescription);
                        desc.
                            MutableFileStoreDescription()->
                            SetIndexTabletId(0);
                    }
                }
                return false;
            }
        );

        ssProxy.SendDescribeFileStoreRequest("test");
        auto describe = ssProxy.RecvDescribeFileStoreResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, describe->GetStatus());
    }

    Y_UNIT_TEST(ShouldFailRequestIfWrongPathTypeIsReturnedFromSS)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        TSSProxyClient ssProxy(env.GetStorageConfig(), env.GetRuntime(), nodeIdx);
        ssProxy.CreateFileStore("test", 2000);

        auto& runtime = env.GetRuntime();
        runtime.SetEventFilter([&] (auto& runtime, auto& ev) {
                Y_UNUSED(runtime);
                switch (ev->GetTypeRewrite()) {
                    case TEvStorageSSProxy::EvDescribeSchemeResponse: {
                        using TEvent = TEvStorageSSProxy::TEvDescribeSchemeResponse;
                        using TDescription = NKikimrSchemeOp::TPathDescription;
                        auto* msg = ev->template Get<TEvent>();
                        auto& desc =
                            const_cast<TDescription&>(msg->PathDescription);
                        desc.
                            MutableSelf()->
                            SetPathType(NKikimrSchemeOp::EPathTypeBlockStoreVolume);
                    }
                }
                return false;
            }
        );

        ssProxy.SendDescribeFileStoreRequest("test");
        auto describe = ssProxy.RecvDescribeFileStoreResponse();
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, describe->GetStatus());
    }

    Y_UNIT_TEST(ShouldDescribeFileStoreInFallbackMode)
    {
        TString backupFilePath =
            "ShouldDescribeFileStoreInFallbackMode.path_description_backup";

        TTestEnv env;
        auto& runtime = env.GetRuntime();
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");

        NProto::TStorageConfig config;
        config.SetPathDescriptionBackupFilePath(backupFilePath);

        {
            TSSProxyClient ssProxy(
                CreateTestStorageConfig(config),
                runtime,
                nodeIdx);

            ssProxy.CreateFileStore("test", 2000);

            // Smoke check for background sync (15 seconds should be enough).
            runtime.AdvanceCurrentTime(TDuration::Seconds(15));
            runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(15));

            ssProxy.SendRequest(
                MakeSSProxyServiceId(),
                std::make_unique<TEvStorageSSProxy::TEvBackupPathDescriptionsRequest>());
        }

        {
            config.SetSSProxyFallbackMode(true);

            TSSProxyClient ssProxy(
                CreateTestStorageConfig(config),
                runtime,
                nodeIdx);

            ssProxy.DescribeFileStore("test");

            ssProxy.SendRequest(
                MakeSSProxyServiceId(),
                std::make_unique<TEvSSProxy::TEvDescribeFileStoreRequest>(
                    "unexisting"));

            auto response =
                ssProxy.RecvResponse<TEvSSProxy::TEvDescribeFileStoreResponse>();
            UNIT_ASSERT_C(FAILED(response->GetStatus()), response->GetErrorReason());
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
