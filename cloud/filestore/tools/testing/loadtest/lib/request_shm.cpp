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
#include <util/string/builder.h>
#include <util/system/mutex.h>

#include <atomic>

namespace NCloud::NFileStore::NLoadTest {

using namespace NThreading;
using namespace NCloud::NFileStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TNodeInfo
{
    TString Name;
    ui64 NodeId = 0;
    ui64 Size = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TSharedMemoryDataRequestGenerator final
    : public IRequestGenerator
    , public std::enable_shared_from_this<TSharedMemoryDataRequestGenerator>
{
private:
    static constexpr ui32 DefaultBlockSize = 4_KB;

    const NProto::TSharedMemoryLoadSpec Spec;
    const TString FileSystemId;
    const NProto::THeaders Headers;
    const ISessionPtr Session_;
    const IFileStoreServicePtr Client_;

    TLog Log;

    TVector<std::pair<ui64, NProto::EAction>> Actions_;
    ui64 TotalRate_ = 0;

    TMutex StateLock_;
    TVector<TNodeInfo> NodeInfos_;

    ui64 ReadBytes_ = DefaultBlockSize;
    ui64 WriteBytes_ = DefaultBlockSize;
    ui64 InitialFileSize_ = 0;

    std::atomic<ui64> LastRequestId_{0};

public:
    TSharedMemoryDataRequestGenerator(
            NProto::TSharedMemoryLoadSpec spec,
            ILoggingServicePtr logging,
            ISessionPtr session,
            IFileStoreServicePtr client,
            TString filesystemId,
            NProto::THeaders headers)
        : Spec(std::move(spec))
        , FileSystemId(std::move(filesystemId))
        , Headers(std::move(headers))
        , Session_(std::move(session))
        , Client_(std::move(client))
    {
        Log = logging->CreateLog(Headers.GetClientId());

        if (auto bytes = Spec.GetReadBytes()) {
            ReadBytes_ = bytes;
        }
        if (auto bytes = Spec.GetWriteBytes()) {
            WriteBytes_ = bytes;
        }

        InitialFileSize_ = Spec.GetInitialFileSize();

        for (const auto& action : Spec.GetActions()) {
            Y_ENSURE(action.GetRate() > 0, "please specify positive action rate");
            TotalRate_ += action.GetRate();
            Actions_.emplace_back(TotalRate_, action.GetAction());
        }

        Y_ENSURE(!Actions_.empty(), "please specify at least one action");
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
        auto number = RandomNumber(TotalRate_);
        auto it = UpperBound(
            Actions_.begin(),
            Actions_.end(),
            number,
            [](ui64 b, const auto& pair) { return b < pair.first; });

        Y_ABORT_UNLESS(it != Actions_.end());
        return it->second;
    }

    // Creates a parentless file via CreateNode, then optionally resizes it.
    // No handle is opened — IO uses NodeId directly (handleless IO).
    TFuture<TCompletedRequest> DoCreateNode()
    {
        const auto started = TInstant::Now();
        auto name = GenerateNodeName();

        auto request = CreateRequest<NProto::TCreateNodeRequest>();
        request->SetNodeId(RootNodeId);
        request->SetName(name);
        request->MutableFile()->SetMode(0664);

        auto self = weak_from_this();
        return Session_->CreateNode(CreateCallContext(), std::move(request))
            .Apply([=, name = std::move(name)](
                       const TFuture<NProto::TCreateNodeResponse>& future) {
                if (auto ptr = self.lock()) {
                    return ptr->HandleCreateNode(future, name, started);
                }
                return MakeFuture(TCompletedRequest{
                    NProto::ACTION_CREATE_NODE,
                    started,
                    MakeError(E_FAIL, "cancelled")});
            });
    }

    TFuture<TCompletedRequest> HandleCreateNode(
        const TFuture<NProto::TCreateNodeResponse>& future,
        const TString& name,
        TInstant started)
    {
        try {
            const auto& response = future.GetValue();
            CheckResponse(response);

            TNodeInfo info;
            info.Name = name;
            info.NodeId = response.GetNode().GetId();
            info.Size = 0;

            NThreading::TFuture<NProto::TSetNodeAttrResponse> resizeFuture;
            if (InitialFileSize_ > 0) {
                static const int resizeFlags =
                    ProtoFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);

                // Resize using NodeId only — no handle required
                auto resizeReq = CreateRequest<NProto::TSetNodeAttrRequest>();
                resizeReq->SetNodeId(info.NodeId);
                resizeReq->SetFlags(resizeFlags);
                resizeReq->MutableUpdate()->SetSize(InitialFileSize_);

                resizeFuture =
                    Session_->SetNodeAttr(CreateCallContext(), std::move(resizeReq));
            } else {
                resizeFuture = NThreading::MakeFuture(NProto::TSetNodeAttrResponse());
            }

            return resizeFuture.Apply(
                [=, this](const TFuture<NProto::TSetNodeAttrResponse>& f) {
                    return HandleResizeAfterCreate(f, info, started);
                });
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR("create node %s failed: %s",
                name.Quote().c_str(),
                FormatError(error).c_str());
            return NThreading::MakeFuture(TCompletedRequest{
                NProto::ACTION_CREATE_NODE, started, error});
        }
    }

    TCompletedRequest HandleResizeAfterCreate(
        const TFuture<NProto::TSetNodeAttrResponse>& future,
        TNodeInfo info,
        TInstant started)
    {
        try {
            const auto& response = future.GetValue();
            if (!HasError(response)) {
                info.Size = InitialFileSize_;
            }
        } catch (...) {
            // Resize failed — node is still usable but not pre-sized
        }

        with_lock (StateLock_) {
            NodeInfos_.push_back(info);
        }

        return {NProto::ACTION_CREATE_NODE, started, {}};
    }

    TFuture<TCompletedRequest> DoRead()
    {
        TGuard<TMutex> guard(StateLock_);
        auto it = std::find_if(
            NodeInfos_.begin(),
            NodeInfos_.end(),
            [this](const TNodeInfo& n) { return n.Size >= ReadBytes_; });
        if (it == NodeInfos_.end()) {
            guard.Release();
            return DoCreateNode();
        }

        std::swap(*it, NodeInfos_.back());
        auto nodeInfo = NodeInfos_.back();
        NodeInfos_.pop_back();
        guard.Release();

        const auto started = TInstant::Now();
        const ui64 slotCount = nodeInfo.Size / ReadBytes_;
        const ui64 byteOffset = RandomNumber(slotCount) * ReadBytes_;

        auto request = CreateRequest<NProto::TReadDataRequest>();
        // Handleless IO: Handle=InvalidHandle, NodeId identifies the file.
        // Requires AllowHandlelessIO feature on the filesystem.
        request->SetHandle(InvalidHandle);
        request->SetNodeId(nodeInfo.NodeId);
        request->SetOffset(byteOffset);
        request->SetLength(ReadBytes_);

        auto self = weak_from_this();
        return Client_->ReadData(CreateCallContext(), std::move(request))
            .Apply([=](const TFuture<NProto::TReadDataResponse>& future) {
                if (auto ptr = self.lock()) {
                    return ptr->HandleRead(future, nodeInfo, started);
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
        TInstant started)
    {
        try {
            const auto& response = future.GetValue();
            CheckResponse(response);

            with_lock (StateLock_) {
                NodeInfos_.push_back(std::move(nodeInfo));
            }

            return {NProto::ACTION_READ, started, response.GetError()};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR("read on node %lu failed: %s",
                nodeInfo.NodeId,
                FormatError(error).c_str());
            return {NProto::ACTION_READ, started, error};
        }
    }

    TFuture<TCompletedRequest> DoWrite(TNodeInfo nodeInfo = {})
    {
        {
            TGuard<TMutex> guard(StateLock_);
            if (nodeInfo.NodeId == 0) {
                if (NodeInfos_.empty()) {
                    guard.Release();
                    return DoCreateNode();
                }
                nodeInfo = GetNodeInfo();
            }
        }

        const auto started = TInstant::Now();
        ui64 byteOffset = nodeInfo.Size;
        nodeInfo.Size += WriteBytes_;

        TString buffer(WriteBytes_, '\0');

        auto request = CreateRequest<NProto::TWriteDataRequest>();
        // Handleless IO: Handle=InvalidHandle, NodeId identifies the file.
        // Requires AllowHandlelessIO feature on the filesystem.
        request->SetHandle(InvalidHandle);
        request->SetNodeId(nodeInfo.NodeId);
        request->SetOffset(byteOffset);
        *request->MutableBuffer() = std::move(buffer);

        auto self = weak_from_this();
        return Client_->WriteData(CreateCallContext(), std::move(request))
            .Apply([=](const TFuture<NProto::TWriteDataResponse>& future) {
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

            with_lock (StateLock_) {
                NodeInfos_.push_back(std::move(nodeInfo));
            }

            return {NProto::ACTION_WRITE, started, response.GetError()};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR("write on node %lu failed: %s",
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

    TString GenerateNodeName()
    {
        return TStringBuilder()
            << Headers.GetClientId() << ":shm:" << CreateGuidAsString();
    }

    TNodeInfo GetNodeInfo()
    {
        Y_ABORT_UNLESS(!NodeInfos_.empty());
        ui64 index = RandomNumber(NodeInfos_.size());
        std::swap(NodeInfos_[index], NodeInfos_.back());
        auto info = NodeInfos_.back();
        NodeInfos_.pop_back();
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
            LastRequestId_.fetch_add(1, std::memory_order_relaxed));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRequestGeneratorPtr CreateSharedMemoryRequestGenerator(
    NProto::TSharedMemoryLoadSpec spec,
    ILoggingServicePtr logging,
    NClient::ISessionPtr session,
    IFileStoreServicePtr client,
    TString filesystemId,
    NProto::THeaders headers)
{
    return std::make_shared<TSharedMemoryDataRequestGenerator>(
        std::move(spec),
        std::move(logging),
        std::move(session),
        std::move(client),
        std::move(filesystemId),
        std::move(headers));
}

}   // namespace NCloud::NFileStore::NLoadTest
