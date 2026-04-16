#include "request.h"

#include <cloud/filestore/libs/client/session.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/public/api/protos/data.pb.h>
#include <cloud/filestore/public/api/protos/node.pb.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/guid.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/system/mutex.h>

#include <atomic>

namespace NCloud::NFileStore::NLoadTest {

using namespace NThreading;
using namespace NCloud::NFileStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TNodeInfo
{
    ui64 NodeId = 0;
    ui64 Size = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Performs handleless IO on parentless files (no open handle required).
// Requires AllowHandlelessIO feature enabled on the target filesystem.
//
// Files are created via CreateNode and are accessed by NodeId with invalid
// handle. This requires ParentlessFilesOnly feature on the target filesystem
class TDatashardLikeRequestGenerator final
    : public IRequestGenerator
    , public std::enable_shared_from_this<TDatashardLikeRequestGenerator>
{
private:
    static constexpr ui32 DefaultBlockSize = 4_KB;

    const NProto::TDatashardLikeLoadSpec Spec;
    const TString FileSystemId;
    const NProto::THeaders Headers;

    TLog Log;

    ISessionPtr Session;
    // When set, data IO (ReadData/WriteData) is routed through this client
    // instead of Session (used for SHM transport).
    IShmDataClientPtr DataClient;

    TVector<std::pair<ui64, NProto::EAction>> Actions;
    ui64 TotalRate = 0;

    TMutex StateLock;
    TVector<TNodeInfo> NodeInfos;

    ui64 ReadBytes = DefaultBlockSize;
    ui64 WriteBytes = DefaultBlockSize;
    ui64 InitialFileSize = 0;

    std::atomic<ui64> LastRequestId = 0;

public:
    TDatashardLikeRequestGenerator(
            NProto::TDatashardLikeLoadSpec spec,
            ILoggingServicePtr logging,
            ISessionPtr session,
            IShmDataClientPtr dataClient,
            TString filesystemId,
            NProto::THeaders headers)
        : Spec(std::move(spec))
        , FileSystemId(std::move(filesystemId))
        , Headers(std::move(headers))
        , Session(std::move(session))
        , DataClient(std::move(dataClient))
    {
        Log = logging->CreateLog(Headers.GetClientId());

        if (auto bytes = Spec.GetReadBytes()) {
            ReadBytes = bytes;
        }
        if (auto bytes = Spec.GetWriteBytes()) {
            WriteBytes = bytes;
        }

        InitialFileSize = Spec.GetInitialFileSize();
        Y_ENSURE(InitialFileSize > 0, "InitialFileSize must be set");
        Y_ENSURE(
            InitialFileSize >= ReadBytes,
            Sprintf(
                "InitialFileSize (%lu) must be >= ReadBytes (%lu)",
                InitialFileSize,
                ReadBytes));
        Y_ENSURE(
            InitialFileSize >= WriteBytes,
            Sprintf(
                "InitialFileSize (%lu) must be >= WriteBytes (%lu)",
                InitialFileSize,
                WriteBytes));

        for (const auto& action: Spec.GetActions()) {
            Y_ENSURE(
                action.GetRate() > 0,
                "please specify positive action rate");
            TotalRate += action.GetRate();
            Actions.emplace_back(std::make_pair(TotalRate, action.GetAction()));
        }

        Y_ENSURE(!Actions.empty(), "please specify at least one action");
    }

    bool HasNextRequest() override
    {
        return true;
    }

    TFuture<TCompletedRequest> ExecuteNextRequest() override
    {
        const auto& action = PeekNextAction();
        switch (action) {
            case NProto::ACTION_READ:
                return DoRead();
            case NProto::ACTION_WRITE:
                return DoWrite();
            default:
                Y_ABORT("unexpected action: %u", (ui32)action);
        }
    }

private:
    NProto::EAction PeekNextAction()
    {
        auto number = RandomNumber(TotalRate);
        auto it = UpperBound(
            Actions.begin(),
            Actions.end(),
            number,
            [](ui64 b, const auto& pair) { return b < pair.first; });

        Y_ABORT_UNLESS(it != Actions.end());
        return it->second;
    }

    TFuture<TCompletedRequest> DoCreateNode()
    {
        auto started = TInstant::Now();

        auto request = CreateRequest<NProto::TCreateNodeRequest>();
        request->SetNodeId(RootNodeId);
        request->SetName(CreateGuidAsString());
        request->MutableFile()->SetMode(0664);

        auto self = weak_from_this();
        return Session->CreateNode(CreateCallContext(), std::move(request))
            .Apply(
                [=](const TFuture<NProto::TCreateNodeResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleCreateNode(future, started);
                    }

                    return MakeFuture(
                        TCompletedRequest{
                            NProto::ACTION_CREATE_NODE,
                            started,
                            MakeError(E_FAIL, "cancelled")});
                });
    }

    TFuture<TCompletedRequest> HandleCreateNode(
        const TFuture<NProto::TCreateNodeResponse>& future,
        TInstant started)
    {
        try {
            const auto& response = future.GetValue();
            CheckResponse(response);

            TNodeInfo info;
            info.NodeId = response.GetNode().GetId();
            info.Size = 0;

            NThreading::TFuture<NProto::TSetNodeAttrResponse> resizeFuture;
            if (InitialFileSize > 0) {
                static const int resizeFlags =
                    ProtoFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);

                auto resizeReq = CreateRequest<NProto::TSetNodeAttrRequest>();
                resizeReq->SetNodeId(info.NodeId);
                resizeReq->SetFlags(resizeFlags);
                resizeReq->MutableUpdate()->SetSize(InitialFileSize);

                resizeFuture = Session->SetNodeAttr(
                    CreateCallContext(),
                    std::move(resizeReq));
            } else {
                resizeFuture =
                    NThreading::MakeFuture(NProto::TSetNodeAttrResponse());
            }

            return resizeFuture.Apply(
                [=, this](const TFuture<NProto::TSetNodeAttrResponse>& f)
                { return HandleResizeAfterCreate(f, info, started); });
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "create node has failed: %s",
                FormatError(error).c_str());

            return NThreading::MakeFuture(
                TCompletedRequest{NProto::ACTION_CREATE_NODE, started, error});
        }
    }

    TCompletedRequest HandleResizeAfterCreate(
        const TFuture<NProto::TSetNodeAttrResponse>& future,
        TNodeInfo info,
        TInstant started)
    {
        try {
            const auto& response = future.GetValue();
            CheckResponse(response);
            info.Size = InitialFileSize;

            with_lock (StateLock) {
                NodeInfos.push_back(std::move(info));
            }

            return {NProto::ACTION_CREATE_NODE, started, response.GetError()};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "set node attr for %lu has failed: %s",
                info.NodeId,
                FormatError(error).c_str());

            return {NProto::ACTION_CREATE_NODE, started, error};
        }
    }

    TFuture<TCompletedRequest> DoRead()
    {
        TGuard<TMutex> guard(StateLock);
        auto it = std::find_if(
            NodeInfos.begin(),
            NodeInfos.end(),
            [this](const TNodeInfo& n) { return n.Size >= ReadBytes; });
        if (it == NodeInfos.end()) {
            guard.Release();
            return DoCreateNode();
        }

        std::swap(*it, NodeInfos.back());
        auto nodeInfo = NodeInfos.back();
        NodeInfos.pop_back();
        guard.Release();

        const auto started = TInstant::Now();
        const ui64 slotCount = nodeInfo.Size / ReadBytes;
        const ui64 byteOffset = RandomNumber(slotCount) * ReadBytes;

        auto request = CreateRequest<NProto::TReadDataRequest>();
        request->SetHandle(InvalidHandle);
        request->SetNodeId(nodeInfo.NodeId);
        request->SetOffset(byteOffset);
        request->SetLength(ReadBytes);

        char* shmLocalPtr = DataClient
            ? DataClient->PrepareRead(*request)
            : nullptr;

        auto self = weak_from_this();
        return Session->ReadData(CreateCallContext(), std::move(request))
            .Apply(
                [=](const TFuture<NProto::TReadDataResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleRead(future, nodeInfo, started, shmLocalPtr);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_READ,
                        started,
                        MakeError(E_FAIL, "cancelled")};
                });
    }

    TCompletedRequest HandleRead(
        const TFuture<NProto::TReadDataResponse>& future,
        TNodeInfo nodeInfo,
        TInstant started,
        char* shmLocalPtr)
    {
        try {
            auto response = future.GetValue();
            CheckResponse(response);

            if (shmLocalPtr && response.GetBuffer().empty() && response.GetLength() > 0) {
                response.SetBuffer(TString(shmLocalPtr, response.GetLength()));
            }

            with_lock (StateLock) {
                NodeInfos.push_back(std::move(nodeInfo));
            }

            return {NProto::ACTION_READ, started, response.GetError()};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "read on node %lu has failed: %s",
                nodeInfo.NodeId,
                FormatError(error).c_str());

            return {NProto::ACTION_READ, started, error};
        }
    }

    TFuture<TCompletedRequest> DoWrite(TNodeInfo nodeInfo = {})
    {
        TGuard<TMutex> guard(StateLock);
        if (nodeInfo.NodeId == 0) {
            if (NodeInfos.empty()) {
                return DoCreateNode();
            }
            nodeInfo = GetNodeInfo();
        }

        const auto started = TInstant::Now();
        ui64 byteOffset = nodeInfo.Size;
        nodeInfo.Size += WriteBytes;

        TString buffer(WriteBytes, '\0');

        auto request = CreateRequest<NProto::TWriteDataRequest>();

        request->SetHandle(InvalidHandle);
        request->SetNodeId(nodeInfo.NodeId);
        request->SetOffset(byteOffset);
        *request->MutableBuffer() = std::move(buffer);

        if (DataClient) {
            DataClient->PrepareWrite(*request);
        }

        auto self = weak_from_this();
        return Session->WriteData(CreateCallContext(), std::move(request))
            .Apply(
                [=](const TFuture<NProto::TWriteDataResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleWrite(future, nodeInfo, started);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_WRITE,
                        started,
                        MakeError(E_FAIL, "cancelled")};
                });
    }

    TCompletedRequest HandleWrite(
        const TFuture<NProto::TWriteDataResponse>& future,
        TNodeInfo nodeInfo,
        TInstant started)
    {
        try {
            const auto& response = future.GetValue();
            CheckResponse(response);

            with_lock (StateLock) {
                NodeInfos.push_back(std::move(nodeInfo));
            }

            return {NProto::ACTION_WRITE, started, response.GetError()};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "write on node %lu has failed: %s",
                nodeInfo.NodeId,
                FormatError(error).c_str());

            return {NProto::ACTION_WRITE, started, error};
        }
    }

    template <typename T>
    std::shared_ptr<T> CreateRequest()
    {
        auto request = std::make_shared<T>();
        request->SetFileSystemId(FileSystemId);
        request->MutableHeaders()->CopyFrom(Headers);

        return request;
    }

    TNodeInfo GetNodeInfo()
    {
        Y_ABORT_UNLESS(!NodeInfos.empty());
        ui64 index = RandomNumber(NodeInfos.size());
        std::swap(NodeInfos[index], NodeInfos.back());
        auto info = NodeInfos.back();
        NodeInfos.pop_back();
        return info;
    }

    template <typename T>
    void CheckResponse(const T& response)
    {
        if (HasError(response)) {
            STORAGE_THROW_SERVICE_ERROR(response.GetError());
        }
    }

    TIntrusivePtr<TCallContext> CreateCallContext()
    {
        return MakeIntrusive<TCallContext>(
            FileSystemId,
            LastRequestId.fetch_add(1, std::memory_order_relaxed));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRequestGeneratorPtr CreateDatashardLikeRequestGenerator(
    NProto::TDatashardLikeLoadSpec spec,
    ILoggingServicePtr logging,
    NClient::ISessionPtr session,
    IShmDataClientPtr dataClient,
    TString filesystemId,
    NProto::THeaders headers)
{
    return std::make_shared<TDatashardLikeRequestGenerator>(
        std::move(spec),
        std::move(logging),
        std::move(session),
        std::move(dataClient),
        std::move(filesystemId),
        std::move(headers));
}

}   // namespace NCloud::NFileStore::NLoadTest
