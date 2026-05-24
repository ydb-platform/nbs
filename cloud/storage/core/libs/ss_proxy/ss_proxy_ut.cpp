#include "ss_proxy.h"

#include "public.h"

#include <cloud/storage/core/libs/api/ss_proxy.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/testlib/test_client.h>
#include <contrib/ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::Tests;
using namespace NSchemeShardUT_Private;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 NodeIdx = 0;

TSSProxyConfig CreateConfig(bool useSchemeCache)
{
    TSSProxyConfig config;
    config.SchemeShardDir = "/MyRoot";
    config.UseSchemeCache = useSchemeCache;
    return config;
}

TActorId SetupSSProxy(
    TTestBasicRuntime& runtime,
    bool useSchemeCache)
{
    auto ssProxyId = runtime.Register(
        CreateSSProxy(CreateConfig(useSchemeCache)).release(),
        NodeIdx);

    runtime.EnableScheduleForActor(ssProxyId);
    runtime.DispatchEvents({}, TDuration::Seconds(1));

    return ssProxyId;
}

TEvSSProxy::TEvDescribeSchemeResponse::TPtr DescribePath(
    TTestBasicRuntime& runtime,
    const TActorId& ssProxyId,
    const TString& path)
{
    const auto sender = runtime.AllocateEdgeActor(NodeIdx);

    runtime.Send(
        new IEventHandle(
            ssProxyId,
            sender,
            new TEvSSProxy::TEvDescribeSchemeRequest(path)),
        NodeIdx);

    TAutoPtr<IEventHandle> handle;

    auto* response =
        runtime.GrabEdgeEventRethrow<
            TEvSSProxy::TEvDescribeSchemeResponse>(handle);

    UNIT_ASSERT_C(
        !HasError(response->GetError()),
        response->GetError());

    return IEventHandle::Downcast<
        TEvSSProxy::TEvDescribeSchemeResponse>(
            std::move(handle));
}

void CreateFileStore(
    TTestBasicRuntime& runtime,
    const TActorId& ssProxyId,
    const TString& fileSystemId)
{
    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetWorkingDir("/MyRoot/nbs");
    modifyScheme.SetOperationType(
        NKikimrSchemeOp::ESchemeOpCreateFileStore);

    auto& fs = *modifyScheme.MutableCreateFileStore();
    fs.SetName(fileSystemId);

    auto& config = *fs.MutableConfig();
    config.SetFileSystemId(fileSystemId);
    config.SetBlockSize(4096);
    config.SetBlocksCount(1024);

    config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");

    const auto sender = runtime.AllocateEdgeActor(NodeIdx);

    runtime.Send(
        new IEventHandle(
            ssProxyId,
            sender,
            new TEvSSProxy::TEvModifySchemeRequest(
                std::move(modifyScheme))),
        NodeIdx);

    TAutoPtr<IEventHandle> handle;

    auto* response =
        runtime.GrabEdgeEventRethrow<
            TEvSSProxy::TEvModifySchemeResponse>(handle);

    UNIT_ASSERT_C(
        !HasError(response->GetError()),
        response->GetError());
}

void CreateBlockStoreVolume(
    TTestBasicRuntime& runtime,
    const TActorId& ssProxyId,
    const TString& diskId)
{
    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetWorkingDir("/MyRoot/nbs");
    modifyScheme.SetOperationType(
        NKikimrSchemeOp::ESchemeOpCreateBlockStoreVolume);

    auto& volume = *modifyScheme.MutableCreateBlockStoreVolume();
    volume.SetName(diskId);

    auto& config = *volume.MutableVolumeConfig();
    config.SetDiskId(diskId);
    config.SetBlockSize(4096);
    config.AddPartitions()->SetBlockCount(1024);

    config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
    config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
    config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-1");
    config.AddExplicitChannelProfiles()->SetPoolKind("pool-kind-2");

    const auto sender = runtime.AllocateEdgeActor(NodeIdx);

    runtime.Send(
        new IEventHandle(
            ssProxyId,
            sender,
            new TEvSSProxy::TEvModifySchemeRequest(
                std::move(modifyScheme))),
        NodeIdx);

    TAutoPtr<IEventHandle> handle;

    auto* response =
        runtime.GrabEdgeEventRethrow<
            TEvSSProxy::TEvModifySchemeResponse>(handle);

    UNIT_ASSERT_C(
        !HasError(response->GetError()),
        response->GetError());
}

void TestShouldDescribeFileStore(bool useSchemeCache)
{
    TTestBasicRuntime runtime;
    TTestEnv env(runtime);

    ui64 txId = 100;

    TestMkDir(runtime, ++txId, "/MyRoot", "nbs");
    env.TestWaitNotification(runtime, txId);

    auto ssProxyId = SetupSSProxy(runtime, useSchemeCache);

    CreateFileStore(runtime, ssProxyId, "fs");

    const auto response =
        DescribePath(runtime, ssProxyId, "/MyRoot/nbs/fs");

    const auto& pathDescription =
        response->Get()->PathDescription;

    UNIT_ASSERT_VALUES_EQUAL(
        NKikimrSchemeOp::EPathTypeFileStore,
        pathDescription.GetSelf().GetPathType());

    UNIT_ASSERT_VALUES_EQUAL(
        "fs",
        pathDescription
            .GetFileStoreDescription()
            .GetConfig()
            .GetFileSystemId());
}

void TestShouldDescribeBlockStoreVolume(bool useSchemeCache)
{
    TTestBasicRuntime runtime;
    TTestEnv env(runtime);

    ui64 txId = 100;

    TestMkDir(runtime, ++txId, "/MyRoot", "nbs");
    env.TestWaitNotification(runtime, txId);

    auto ssProxyId = SetupSSProxy(runtime, useSchemeCache);

    CreateBlockStoreVolume(runtime, ssProxyId, "volume");

    const auto response =
        DescribePath(runtime, ssProxyId, "/MyRoot/nbs/volume");

    const auto& pathDescription =
        response->Get()->PathDescription;

    UNIT_ASSERT_VALUES_EQUAL(
        NKikimrSchemeOp::EPathTypeBlockStoreVolume,
        pathDescription.GetSelf().GetPathType());

    UNIT_ASSERT_VALUES_EQUAL(
        "volume",
        pathDescription
            .GetBlockStoreVolumeDescription()
            .GetVolumeConfig()
            .GetDiskId());
}

void TestShouldListFileStoresAndBlockStoreVolumes(
    bool useSchemeCache)
{
    TTestBasicRuntime runtime;
    TTestEnv env(runtime);

    ui64 txId = 100;

    TestMkDir(runtime, ++txId, "/MyRoot", "nbs");
    env.TestWaitNotification(runtime, txId);

    auto ssProxyId = SetupSSProxy(runtime, useSchemeCache);

    CreateFileStore(runtime, ssProxyId, "fs");
    CreateBlockStoreVolume(runtime, ssProxyId, "volume");

    const auto response =
        DescribePath(runtime, ssProxyId, "/MyRoot/nbs");

    const auto& pathDescription =
        response->Get()->PathDescription;

    bool foundFileStore = false;
    bool foundBlockStoreVolume = false;

    for (const auto& child: pathDescription.GetChildren()) {
        if (child.GetName() == "fs") {
            UNIT_ASSERT_VALUES_EQUAL(
                NKikimrSchemeOp::EPathTypeFileStore,
                child.GetPathType());

            foundFileStore = true;
        } else if (child.GetName() == "volume") {
            UNIT_ASSERT_VALUES_EQUAL(
                NKikimrSchemeOp::EPathTypeBlockStoreVolume,
                child.GetPathType());

            foundBlockStoreVolume = true;
        }
    }

    UNIT_ASSERT(foundFileStore);
    UNIT_ASSERT(foundBlockStoreVolume);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TSSProxyTest)
{
    Y_UNIT_TEST(ShouldDescribeFileStoreWithSchemeCache)
    {
        TestShouldDescribeFileStore(true);
    }

    Y_UNIT_TEST(ShouldDescribeFileStoreWithoutSchemeCache)
    {
        TestShouldDescribeFileStore(false);
    }

    Y_UNIT_TEST(ShouldDescribeBlockStoreVolumeWithSchemeCache)
    {
        TestShouldDescribeBlockStoreVolume(true);
    }

    Y_UNIT_TEST(ShouldDescribeBlockStoreVolumeWithoutSchemeCache)
    {
        TestShouldDescribeBlockStoreVolume(false);
    }

    Y_UNIT_TEST(ShouldListFileStoresAndBlockStoreVolumesWithSchemeCache)
    {
        TestShouldListFileStoresAndBlockStoreVolumes(true);
    }

    Y_UNIT_TEST(ShouldListFileStoresAndBlockStoreVolumesWithoutSchemeCache)
    {
        TestShouldListFileStoresAndBlockStoreVolumes(false);
    }
}

}   // namespace NCloud::NStorage
