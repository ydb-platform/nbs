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

constexpr TStringBuf ControlDirName = ".filestore-ctl";

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
        if (enableControlNamespace) {
            config.SetControlNamespaceDirName(TString(ControlDirName));
        }

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

        auto dirAttr = Service->GetNodeAttr(
            headers,
            FsId,
            RootNodeId,
            TString(ControlDirName));
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
            TCreateNodeArgs::Directory(RootNodeId, TString(ControlDirName)));
    }

    Y_UNIT_TEST_F(ShouldRejectHardLinksTargetingControlNamespace, TEnabledFixture)
    {
        auto headers = Service->InitSession(FsId, "client");

        // hard-linking a real name to a reserved ino as the link target -
        // the target, not just (parent, name), must be classified too
        auto linkToFsId =
            TCreateNodeArgs(ENodeType::Link, RootNodeId, "x");
        linkToFsId.TargetNode = ControlFsIdFileIno;
        Service->AssertCreateNodeFailed(headers, linkToFsId);

        auto linkToControlDir =
            TCreateNodeArgs(ENodeType::Link, RootNodeId, "y");
        linkToControlDir.TargetNode = ControlDirIno;
        Service->AssertCreateNodeFailed(headers, linkToControlDir);
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
            TString(ControlDirName),
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

    Y_UNIT_TEST_F(ShouldReadViaAForgedHandleWithoutCreateHandle, TEnabledFixture)
    {
        // handles in this namespace are just the target ino - CreateHandle
        // never allocates session-bound state, so a caller may present
        // ControlFsIdFileIno as a handle directly, by design
        auto headers = Service->InitSession(FsId, "client");

        auto data = Service->ReadData(
            headers,
            FsId,
            ControlFsIdFileIno,
            ControlFsIdFileIno,
            0,
            4_KB);
        UNIT_ASSERT_VALUES_EQUAL(FsId, data->Record.GetBuffer());
    }

    Y_UNIT_TEST_F(ShouldRejectReadingTheControlDirAsAFile, TEnabledFixture)
    {
        auto headers = Service->InitSession(FsId, "client");

        // the control dir itself is a legitimately obtainable read-only
        // handle - reading through it must not fall back to serving fsid's
        // content
        auto handle = Service->CreateHandle(
            headers,
            FsId,
            ControlDirIno,
            "",
            TCreateHandleArgs::RDNLY);
        UNIT_ASSERT_VALUES_EQUAL(ControlDirIno, handle->Record.GetHandle());

        Service->AssertReadDataFailed(
            headers,
            FsId,
            ControlDirIno,
            handle->Record.GetHandle(),
            0,
            4_KB);
    }

    Y_UNIT_TEST_F(ShouldToleratesAGarbageListNodesCookie, TEnabledFixture)
    {
        auto headers = Service->InitSession(FsId, "client");

        // the control dir's single entry always fits on the first page, so
        // a non-empty cookie can only come from a caller ignoring the
        // "no cookie in the response means done" contract - must not crash,
        // and must not repeat the entry
        auto response = Service->CreateListNodesRequest(
            headers,
            FsId,
            ControlDirIno);
        response->Record.SetCookie("garbage");
        Service->SendRequest(MakeStorageServiceId(), std::move(response));
        auto reply = Service->RecvListNodesResponse();
        UNIT_ASSERT_C(SUCCEEDED(reply->GetStatus()), reply->GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL(0u, reply->Record.NamesSize());
    }

    Y_UNIT_TEST_F(ShouldRejectRequestsWithInvalidSession, TEnabledFixture)
    {
        // a session that was never created via InitSession
        THeaders headers{FsId, "bogus-client", "bogus-session", 0};

        // by-name form (TryHandleControlNamespaceGetNodeAttr)
        auto attrResponse = Service->AssertGetNodeAttrFailed(
            headers,
            FsId,
            RootNodeId,
            TString(ControlDirName));
        UNIT_ASSERT_VALUES_EQUAL(
            attrResponse->GetError().GetCode(),
            (ui32)E_FS_INVALID_SESSION);

        // self-lookup-by-ino form (ForwardRequestToShard's control-namespace
        // fast path)
        auto selfAttrResponse = Service->AssertGetNodeAttrFailed(
            headers,
            FsId,
            ControlFsIdFileIno,
            "");
        UNIT_ASSERT_VALUES_EQUAL(
            selfAttrResponse->GetError().GetCode(),
            (ui32)E_FS_INVALID_SESSION);

        auto handleResponse = Service->AssertCreateHandleFailed(
            headers,
            FsId,
            ControlFsIdFileIno,
            "",
            TCreateHandleArgs::RDNLY);
        UNIT_ASSERT_VALUES_EQUAL(
            handleResponse->GetError().GetCode(),
            (ui32)E_FS_INVALID_SESSION);

        auto listResponse =
            Service->AssertListNodesFailed(headers, FsId, ControlDirIno);
        UNIT_ASSERT_VALUES_EQUAL(
            listResponse->GetError().GetCode(),
            (ui32)E_FS_INVALID_SESSION);
    }

    Y_UNIT_TEST_F(ShouldIgnoreRequestSuppliedFileSystemIdForFsIdContent, TEnabledFixture)
    {
        auto headers = Service->InitSession(FsId, "client");
        const TString spoofedFsId = "some-other-filesystem";

        // by-name form: session is for FsId, but the request itself claims
        // to be for a different filesystem
        auto attr = Service->GetNodeAttr(
            headers,
            spoofedFsId,
            ControlDirIno,
            "fsid");
        UNIT_ASSERT_VALUES_EQUAL(
            ControlFsIdFileIno,
            attr->Record.GetNode().GetId());

        auto handle = Service->CreateHandle(
            headers,
            spoofedFsId,
            ControlFsIdFileIno,
            "",
            TCreateHandleArgs::RDNLY);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui64>(FsId.size()),
            handle->Record.GetNodeAttr().GetSize());

        auto data = Service->ReadData(
            headers,
            spoofedFsId,
            ControlFsIdFileIno,
            handle->Record.GetHandle(),
            0,
            4_KB);
        UNIT_ASSERT_VALUES_EQUAL(FsId, data->Record.GetBuffer());

        // self-lookup-by-ino form, via ForwardRequestToShard
        auto selfAttr = Service->GetNodeAttr(
            headers,
            spoofedFsId,
            ControlFsIdFileIno,
            "");
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui64>(FsId.size()),
            selfAttr->Record.GetNode().GetSize());
    }

    Y_UNIT_TEST_F(ShouldStayInertWhenDisabled, TDisabledFixture)
    {
        auto headers = Service->InitSession(FsId, "client");

        // feature off - ".filestore-ctl" is just a nonexistent name
        auto response = Service->AssertGetNodeAttrFailed(
            headers,
            FsId,
            RootNodeId,
            TString(ControlDirName));
        UNIT_ASSERT_VALUES_UNEQUAL(S_OK, response->GetStatus());
    }
}

}   // namespace NCloud::NFileStore::NStorage
