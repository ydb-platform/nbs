#include "service.h"

#include "service_actor_control_namespace.h"
#include "service_private.h"
#include "service_ut_helpers.h"

#include <cloud/filestore/libs/storage/testlib/service_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TControlNamespaceFixtureBase: public NUnitTest::TBaseTestCase
{
public:
    THolder<TTestEnv> Env;
    THolder<TServiceClient> Service;
    TString FsId = "test";

protected:
    void Init(bool enableControlNamespace)
    {
        NProto::TStorageConfig config;
        config.SetEnableControlNamespace(enableControlNamespace);

        Env = MakeHolder<TTestEnv>(TTestEnvConfig{}, config);
        ui32 nodeIdx = Env->AddDynamicNode();
        Service = MakeHolder<TServiceClient>(Env->GetRuntime(), nodeIdx);
        Service->CreateFileStore(FsId, 1'000);
    }
};

class TEnabledFixture: public TControlNamespaceFixtureBase
{
public:
    void SetUp(NUnitTest::TTestContext&) override
    {
        Init(true);
    }
};

class TDisabledFixture: public TControlNamespaceFixtureBase
{
public:
    void SetUp(NUnitTest::TTestContext&) override
    {
        Init(false);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStorageServiceControlNamespaceTest)
{
    Y_UNIT_TEST_F(ShouldExposeControlDirAndFsIdViaLookup, TEnabledFixture)
    {
        auto headers = Service->InitSession(FsId, "client");

        auto dirAttr =
            Service->GetNodeAttr(headers, FsId, RootNodeId, ".filestore-ctl");
        UNIT_ASSERT_VALUES_EQUAL(
            ControlDirIno,
            dirAttr->Record.GetNode().GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::E_DIRECTORY_NODE),
            static_cast<int>(dirAttr->Record.GetNode().GetType()));

        auto fsIdAttr =
            Service->GetNodeAttr(headers, FsId, ControlDirIno, "fsid");
        UNIT_ASSERT_VALUES_EQUAL(
            ControlFsIdFileIno,
            fsIdAttr->Record.GetNode().GetId());
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(NProto::E_REGULAR_NODE),
            static_cast<int>(fsIdAttr->Record.GetNode().GetType()));

        // an unknown name inside the control dir - ENOENT, not EIO
        Service->AssertGetNodeAttrFailed(headers, FsId, ControlDirIno, "nope");

        // self-lookup by ino (empty name) also works
        auto selfAttr =
            Service->GetNodeAttr(headers, FsId, ControlFsIdFileIno, "");
        UNIT_ASSERT_VALUES_EQUAL(
            ControlFsIdFileIno,
            selfAttr->Record.GetNode().GetId());
    }

    Y_UNIT_TEST_F(ShouldReadFsIdContent, TEnabledFixture)
    {
        auto headers = Service->InitSession(FsId, "client");

        auto handle = Service->CreateHandle(
            headers,
            FsId,
            ControlFsIdFileIno,
            "",
            TCreateHandleArgs::RDNLY);
        UNIT_ASSERT_VALUES_EQUAL(
            ControlFsIdFileIno,
            handle->Record.GetHandle());

        auto data = Service->ReadData(
            headers,
            FsId,
            ControlFsIdFileIno,
            handle->Record.GetHandle(),
            0,
            4_KB);
        UNIT_ASSERT_VALUES_EQUAL(FsId, data->Record.GetBuffer());

        // closing the synthesized handle must not leak to a real tablet
        Service->DestroyHandle(
            headers,
            FsId,
            ControlFsIdFileIno,
            handle->Record.GetHandle());
    }

    Y_UNIT_TEST_F(ShouldRejectWritesToFsId, TEnabledFixture)
    {
        auto headers = Service->InitSession(FsId, "client");

        // a write-mode open is rejected right at CreateHandle
        Service->AssertCreateHandleFailed(
            headers,
            FsId,
            ControlFsIdFileIno,
            "",
            TCreateHandleArgs::WRNLY);

        // create-and-open by (parent, name) is rejected the same way
        Service->AssertCreateHandleFailed(
            headers,
            FsId,
            ControlDirIno,
            "fsid",
            TCreateHandleArgs::CREATE);

        // a plain read-only handle obtained by name, then a direct write -
        // also rejected
        auto handle = Service->CreateHandle(
            headers,
            FsId,
            ControlDirIno,
            "fsid",
            TCreateHandleArgs::RDNLY);
        UNIT_ASSERT_VALUES_EQUAL(
            ControlFsIdFileIno,
            handle->Record.GetHandle());

        Service->AssertWriteDataFailed(
            headers,
            FsId,
            ControlFsIdFileIno,
            handle->Record.GetHandle(),
            0,
            TString("x"));
    }

    Y_UNIT_TEST_F(ShouldRejectTreeMutationsUnderControlDir, TEnabledFixture)
    {
        auto headers = Service->InitSession(FsId, "client");

        // mkdir/mknod directly under ".filestore-ctl"
        Service->AssertCreateNodeFailed(
            headers,
            TCreateNodeArgs::Directory(ControlDirIno, "x"));

        // and creating something literally named ".filestore-ctl" at root
        Service->AssertCreateNodeFailed(
            headers,
            TCreateNodeArgs::Directory(RootNodeId, ".filestore-ctl"));
    }

    Y_UNIT_TEST_F(ShouldRejectRenamesTouchingControlDir, TEnabledFixture)
    {
        auto headers = Service->InitSession(FsId, "client");

        const auto dirId =
            Service
                ->CreateNode(
                    headers,
                    TCreateNodeArgs::Directory(RootNodeId, "dir"))
                ->Record.GetNode()
                .GetId();
        Service->CreateNode(headers, TCreateNodeArgs::File(dirId, "file"));

        // moving a real file into ".filestore-ctl"
        Service->AssertRenameNodeFailed(
            headers,
            dirId,
            "file",
            ControlDirIno,
            "x",
            0);

        // renaming a real dir onto the literal name ".filestore-ctl" at root
        Service->AssertRenameNodeFailed(
            headers,
            RootNodeId,
            "dir",
            RootNodeId,
            ".filestore-ctl",
            0);
    }

    Y_UNIT_TEST_F(ShouldListControlDirContents, TEnabledFixture)
    {
        auto headers = Service->InitSession(FsId, "client");

        auto response = Service->ListNodes(headers, FsId, ControlDirIno);
        UNIT_ASSERT_VALUES_EQUAL(1u, response->Record.NamesSize());
        UNIT_ASSERT_VALUES_EQUAL("fsid", response->Record.GetNames(0));
        UNIT_ASSERT_VALUES_EQUAL(
            ControlFsIdFileIno,
            response->Record.GetNodes(0).GetId());
    }

    Y_UNIT_TEST_F(ShouldStayInertWhenDisabled, TDisabledFixture)
    {
        auto headers = Service->InitSession(FsId, "client");

        // feature off - ".filestore-ctl" is just a nonexistent name
        auto response = Service->AssertGetNodeAttrFailed(
            headers,
            FsId,
            RootNodeId,
            ".filestore-ctl");
        UNIT_ASSERT_VALUES_UNEQUAL(S_OK, response->GetStatus());
    }
}

}   // namespace NCloud::NFileStore::NStorage
