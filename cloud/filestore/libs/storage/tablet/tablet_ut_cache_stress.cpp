#include <cloud/filestore/libs/storage/tablet/protos/tablet.pb.h>
#include <cloud/filestore/libs/storage/tablet/tablet_state_iface.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/hash_set.h>

#include <random>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TRequestGenerator
{
    enum EOperation : ui32
    {
        GET_NODE_ATTR = 0,
        SET_NODE_ATTR,
        GET_NODE_XATTR,
        SET_NODE_XATTR,
        CREATE_NODE,
        UNLINK_NODE,
        CREATE_HANDLE,
        DESTROY_HANDLE,
        REBOOT_TABLET,
        RESET_SESSION,

        OPERATION_COUNT
    };

    EOperation GetRandomOperation()
    {
        return static_cast<EOperation>(
            Random->GenRand() % static_cast<ui64>(EOperation::OPERATION_COUNT));
    }

public:
    TRequestGenerator(NProto::TStorageConfig storageConfig, const ui64 seed)
        : Logging(CreateLoggingService("console", {TLOG_DEBUG}))
        , Log(Logging->CreateLog("NFS_TEST"))
        , Env({}, std::move(storageConfig))
        , Random(CreateDeterministicRandomProvider(seed))
    {
        STORAGE_INFO("Seed: %lu", seed);

        Env.CreateSubDomain("nfs");

        ui32 nodeIdx = Env.CreateNode("nfs");
        ui64 tabletId = Env.BootIndexTablet(nodeIdx);

        Tablet = std::make_unique<TIndexTabletClient>(
            Env.GetRuntime(),
            nodeIdx,
            tabletId);

        Tablet->InitSession("client", "session");

        ObservedNodes.push_back({RootNodeId, InvalidNodeId, ""});
    }

    TVector<TString> RunRandomIndexLoad()
    {
        TVector<TString> responses;

        const auto sanitizerType = GetEnv("SANITIZER_TYPE");
        STORAGE_INFO("Sanitizer: %s", sanitizerType.c_str());
        const THashSet<TString> slowSanitizers({"thread", "undefined", "address"});
        const ui32 d = slowSanitizers.contains(sanitizerType) ? 20 : 1;

        const size_t numRequests = 5000 / d;

        while (responses.size() < numRequests) {
            auto operation = GetRandomOperation();
            TString response;
            switch (operation) {
                case GET_NODE_ATTR:
                    response = DoGetNodeAttr();
                    break;
                case SET_NODE_ATTR:
                    response = DoSetNodeAttr();
                    break;
                case GET_NODE_XATTR:
                    response = DoGetNodeXAttr();
                    break;
                case SET_NODE_XATTR:
                    response = DoSetNodeXAttr();
                    break;
                case CREATE_NODE:
                    response = DoCreateNode();
                    break;
                case UNLINK_NODE:
                    response = DoUnlinkNode();
                    break;
                case CREATE_HANDLE:
                    response = DoCreateHandle();
                    break;
                case DESTROY_HANDLE:
                    response = DoDestroyHandle();
                    break;
                case REBOOT_TABLET:
                    response = DoRebootTablet();
                    break;
                case RESET_SESSION:
                    response = DoResetSession();
                    break;
                default:
                    ythrow yexception() << "must be unreachable";
            }

            if (!response.empty()) {
                responses.push_back(response);
            }
        }

        STORAGE_INFO(
            "Storage stats: %s",
            Tablet->GetStorageStats()->Record.ShortDebugString().c_str());

        return responses;
    }

private:
    TString DoGetNodeAttr()
    {
        bool useParent = Random->GenRand() % 2 == 0;
        const TNode node = PickRandomNode();
        if (useParent && node.ParentId == InvalidNodeId) {
            useParent = false;
        }

        NProto::TGetNodeAttrResponse response;
        if (useParent) {
            response = Tablet->SendAndRecvGetNodeAttr(node.ParentId, node.Name)
                           ->Record;
        } else {
            response = Tablet->SendAndRecvGetNodeAttr(node.NodeId, "")->Record;
        }
        RectifyTimes(response.MutableNode());

        return response.DebugString();
    }

    TString DoSetNodeAttr()
    {
        const TNode node = PickRandomNode();

        NProto::TSetNodeAttrRequest request;
        auto args = TSetNodeAttrArgs(node.NodeId);
        if (Random->GenRand() % 2 == 0) {
            args.SetMode(Random->GenRand());
        }
        if (Random->GenRand() % 2 == 0) {
            args.SetUid(Random->GenRand());
        }
        if (Random->GenRand() % 2 == 0) {
            args.SetGid(Random->GenRand());
        }
        if (Random->GenRand() % 2 == 0) {
            args.SetSize(Random->GenRand());
        }
        if (Random->GenRand() % 2 == 0) {
            args.SetATime(GenerateRandomTime().MicroSeconds());
        }
        if (Random->GenRand() % 2 == 0) {
            args.SetMTime(GenerateRandomTime().MicroSeconds());
        }
        if (Random->GenRand() % 2 == 0) {
            args.SetCTime(GenerateRandomTime().MicroSeconds());
        }

        auto result = Tablet->SendAndRecvSetNodeAttr(args)->Record;
        RectifyTimes(result.MutableNode());
        return result.DebugString();
    }

    TString DoGetNodeXAttr()
    {
        const TNode node = PickRandomNode();
        return Tablet->SendAndRecvGetNodeXAttr(node.NodeId, PickXAttr(false))
            ->Record.DebugString();
    }

    TString DoSetNodeXAttr()
    {
        const TNode node = PickRandomNode();
        return Tablet
            ->SendAndRecvSetNodeXAttr(
                node.NodeId,
                PickXAttr(true),
                PickXAttrValue())
            ->Record.DebugString();
    }

    enum ECreateNodeMode : ui32
    {
        // TODO(debnatkh): support hardlinks and symlinks
        CREATE_FILE = 0,
        CREATE_DIR,

        CREATE_NODE_MODE_COUNT
    };

    TString DoCreateNode()
    {
        const TString name = Sprintf("node%lu", Random->GenRand());
        const TNode parent = PickRandomNode();

        std::unique_ptr<TEvService::TEvCreateNodeResponse> response;
        switch (Random->GenRand() % CREATE_NODE_MODE_COUNT) {
            case CREATE_FILE:
                response = Tablet->SendAndRecvCreateNode(
                    TCreateNodeArgs::File(parent.NodeId, name));
                break;
            case CREATE_DIR:
                response = Tablet->SendAndRecvCreateNode(
                    TCreateNodeArgs::Directory(parent.NodeId, name));
                break;
            default:
                Y_ASSERT(false);
        }

        if (SUCCEEDED(response->GetError().GetCode())) {
            ObservedNodes.push_back({
                .NodeId = response->Record.GetNode().GetId(),
                .ParentId = parent.NodeId,
                .Name = name,
            });
        }
        RectifyTimes(response->Record.MutableNode());

        return response->Record.DebugString();
    }

    TString DoUnlinkNode()
    {
        const TNode node = PickRandomNode();
        if (node.Name.empty()) {
            // The only node without a name is the root node, which should not
            // be unlinked
            return "";
        }
        auto response = Tablet->SendAndRecvUnlinkNode(
            node.NodeId,
            node.Name,
            /* unlinkDirectory = */ Random->GenRand() % 2 == 0);

        if (SUCCEEDED(response->GetError().GetCode())) {
            auto* it = std::find_if(
                ObservedNodes.begin(),
                ObservedNodes.end(),
                [node](const TNode& n) { return n.NodeId == node.NodeId; });
            Y_ASSERT(it != ObservedNodes.end());
            ObservedNodes.erase(it);
        }

        return response->Record.DebugString();
    }

    TString DoCreateHandle()
    {
        const TNode node = PickRandomNode();
        ui32 mode;
        switch (Random->GenRand() % 4) {
            case 0:
                mode = TCreateHandleArgs::RDNLY;
                break;
            case 1:
                mode = TCreateHandleArgs::WRNLY;
                break;
            case 2:
                mode = TCreateHandleArgs::RDWR;
                break;
            case 3:
                mode = TCreateHandleArgs::CREATE;
                break;
            default:
                Y_ASSERT(false);
        }

        auto response = Tablet->SendAndRecvCreateHandle(node.NodeId, mode);

        if (SUCCEEDED(response->GetError().GetCode())) {
            Handles.push_back(response->Record.GetHandle());
            AllHandles.push_back(response->Record.GetHandle());
        }
        RectifyTimes(response->Record.MutableNodeAttr());
        RectifyHandle(response->Record);

        return response->Record.DebugString();
    }

    TString DoDestroyHandle()
    {
        if (Handles.empty()) {
            return "";
        }

        const ui64 handle = Handles[Random->GenRand() % Handles.size()];
        auto response = Tablet->SendAndRecvDestroyHandle(handle);

        if (SUCCEEDED(response->GetError().GetCode())) {
            auto* it = std::find(Handles.begin(), Handles.end(), handle);
            Y_ASSERT(it != Handles.end());
            Handles.erase(it);
        }

        return response->Record.DebugString();
    }

    TString DoRebootTablet()
    {
        Tablet->RebootTablet();
        Tablet->RecoverSession();
        return "Tablet rebooted";
    }

    TString DoResetSession()
    {
        return Tablet->ResetSession("")->Record.DebugString();
    }

    ////////////////////////////////////////////////////////////////////////////////

    // All timestamps after this timestamp are pruned, so we can safely
    // differentiate between timestamps dependent on the test execution time and
    // timestamps set by the test itself
    //
    // 9 September 2001
    constexpr static const TInstant TimestampPivot =
        TInstant::Seconds(1000000000);

    struct TNode;

    /**
     * Picks a random node from the observed nodes. It may be a
     * non-existent node, but this is expected, as it is reasonable to perform
     * operations on non-existent nodes
     */
    TNode PickRandomNode()
    {
        Y_ABORT_UNLESS(!ObservedNodes.empty());
        return ObservedNodes[Random->GenRand() % ObservedNodes.size()];
    }

    /**
     * Generates a random xattr name. If enforceValid is true, only valid
     * xattr is generated. Only valid xattrs are supposed to be set by the test,
     * all other xattrs still can be read (and these reads are expected to fail)
     */
    TString PickXAttr(bool enforceValid)
    {
        static const size_t MaxXAttrs = 8;
        static const size_t MaxValidXAttrs = 4;

        if (enforceValid) {
            return Sprintf("xattr%lu", Random->GenRand() % MaxValidXAttrs);
        }
        return Sprintf("xattr%lu", Random->GenRand() % MaxXAttrs);
    }

    TString PickXAttrValue()
    {
        return Sprintf("value%lu", Random->GenRand());
    }

    /**
     * Some timestamps in the node attributes are not deterministic and
     * are dependent on the timestamp of the test execution. Thus, all
     * reasonably large timestamps are replaced with incrementing counter
     * starting from 1
     */
    void RectifyTimes(NProto::TNodeAttr* node)
    {
        if (TInstant::Seconds(node->GetATime()) > TimestampPivot) {
            node->SetATime(++TimeCounter);
        }
        if (TInstant::Seconds(node->GetMTime()) > TimestampPivot) {
            node->SetMTime(++TimeCounter);
        }
        if (TInstant::Seconds(node->GetCTime()) > TimestampPivot) {
            node->SetCTime(++TimeCounter);
        }
    }

    /**
     * @brief Handles are assigned randomly. To be able to compare the results
     * of two runs they are replaced mapping that always selects the lowest
     * unused value for the handle that has not been observed yet.
     */
    template <typename T>
    void RectifyHandle(T& response)
    {
        auto handle = response.GetHandle();
        if (handle == InvalidHandle) {
            return;
        }
        if (HandleMapping.find(handle) == HandleMapping.end()) {
            ui64 newHandle = HandleMapping.size() + 1;
            HandleMapping[handle] = newHandle;
        }
        response.SetHandle(HandleMapping[handle]);
    }

    TInstant GenerateRandomTime()
    {
        return TInstant::Seconds(Random->GenRand() % TimestampPivot.Seconds());
    }

    ////////////////////////////////////////////////////////////////////////////////

    ILoggingServicePtr Logging;
    TLog Log;
    TTestEnv Env;
    std::unique_ptr<TIndexTabletClient> Tablet;

    TVector<ui64> Handles;
    TVector<ui64> AllHandles;

    // See description of RectifyHandle for details
    THashMap<ui64, ui64> HandleMapping;
    // See description of RectifyTimes for details
    ui64 TimeCounter = 1;

    struct TNode
    {
        ui64 NodeId;
        ui64 ParentId = InvalidNodeId;
        TString Name = "";
    };
    TVector<TNode> ObservedNodes;

    TIntrusivePtr<IRandomProvider> Random;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

/**
 * The main idea of this test is to perform various index operations
 * against the tablet with in-memory cache enabled and disabled. It is expected
 * that the results will be the same
 */
Y_UNIT_TEST_SUITE(TIndexTabletTest_Cache_Stress)
{
    Y_UNIT_TEST(ShouldTruncateLargeFiles)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetInMemoryIndexCacheNodesCapacity(100);
        storageConfig.SetInMemoryIndexCacheNodeAttrsCapacity(100);
        storageConfig.SetInMemoryIndexCacheNodeRefsCapacity(100);
        storageConfig.SetInMemoryIndexCacheNodeRefsExhaustivenessCapacity(2);

        ui64 seed = Now().GetValue();
        auto responses1 =
            TRequestGenerator(storageConfig, seed).RunRandomIndexLoad();
        auto responses2 = TRequestGenerator({}, seed).RunRandomIndexLoad();

        UNIT_ASSERT_VALUES_EQUAL(responses1.size(), responses2.size());
        for (size_t i = 0; i < responses1.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                responses1[i],
                responses2[i],
                Sprintf("request #%lu", i));
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
