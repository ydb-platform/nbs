#include "request.h"

#include <cloud/filestore/libs/client/session.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/public/api/protos/data.pb.h>
#include <cloud/filestore/public/api/protos/node.pb.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/guid.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>

namespace NCloud::NFileStore::NLoadTest {

using namespace NThreading;
using namespace NCloud::NFileStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TIndexRequestGenerator final
    : public IRequestGenerator
    , public std::enable_shared_from_this<TIndexRequestGenerator>
{
private:
    static constexpr ui32 LockLength = 4096;

    struct TNode
    {
        TString Name;
        NProto::TNodeAttr Attrs;
    };

    struct THandle
    {
        TString Path;
        ui64 Handle = 0;
    };

private:
    const NProto::TIndexLoadSpec Spec;
    const TString FileSystemId;
    const NProto::THeaders Headers;
    const ui64 OwnerId = RandomNumber(100500u);

    TLog Log;

    ISessionPtr Session;

    TVector<std::pair<ui64, NProto::EAction>> Actions;
    ui64 TotalRate = 0;

    TMutex StateLock;
    THashMap<TString, TNode> StagedNodes;
    THashMap<TString, TNode> Nodes;

    THashMap<ui64, THandle> Handles;

    THashSet<ui64> Locks;
    THashSet<ui64> StagedLocks;

    ui64 LastRequestId = 0;

public:
    TIndexRequestGenerator(
            NProto::TIndexLoadSpec spec,
            ILoggingServicePtr logging,
            ISessionPtr session,
            TString filesystemId,
            NProto::THeaders headers)
        : Spec(std::move(spec))
        , FileSystemId(std::move(filesystemId))
        , Headers(std::move(headers))
        , Session(std::move(session))
    {
        Log = logging->CreateLog(Headers.GetClientId());

        for (const auto& action: Spec.GetActions()) {
            Y_ENSURE(action.GetRate() > 0, "please specify positive action rate");

            TotalRate += action.GetRate();
            Actions.emplace_back(std::make_pair(TotalRate, action.GetAction()));
        }

        Y_ENSURE(!Actions.empty(), "please specify at least one action for the test spec");
    }

    bool HasNextRequest() override
    {
        return true;
    }

    NThreading::TFuture<TCompletedRequest> ExecuteNextRequest() override
    {
        const auto& action = PeekNextAction();
        switch (action) {
        case NProto::ACTION_CREATE_NODE:
            return DoCreateNode();
        case NProto::ACTION_RENAME_NODE:
            return DoRenameNode();
        case NProto::ACTION_REMOVE_NODE:
            return DoUnlinkNode();
        case NProto::ACTION_CREATE_HANDLE:
            return DoCreateHandle();
        case NProto::ACTION_DESTROY_HANDLE:
            return DoDestroyHandle();
        case NProto::ACTION_GET_NODE_ATTR:
            return DoGetNodeAttr();
        case NProto::ACTION_ACQUIRE_LOCK:
            return DoAcquireLock();
        case NProto::ACTION_RELEASE_LOCK:
            return DoReleaseLock();
        default:
            Y_ABORT("unexpected action: %u", (ui32)action);
        }
    }

private:
    NProto::EAction PeekNextAction()
    {
        auto number = RandomNumber(TotalRate);
        auto it = LowerBound(
            Actions.begin(),
            Actions.end(),
            number,
            [] (const auto& pair, ui64 b) { return pair.first < b; });

        Y_ABORT_UNLESS(it != Actions.end());
        return it->second;
    }

    TFuture<TCompletedRequest> DoCreateNode()
    {
        TGuard<TMutex> guard(StateLock);

        auto name = GenerateNodeName();
        StagedNodes[name] = {};

        auto request = CreateRequest<NProto::TCreateNodeRequest>();
        request->SetNodeId(RootNodeId);
        request->SetName(name);
        request->MutableFile()->SetMode(0777);

        auto started = TInstant::Now();
        auto self = weak_from_this();
        return Session->CreateNode(CreateCallContext(), std::move(request)).Apply(
            [=] (const TFuture<NProto::TCreateNodeResponse>& future) {
                if (auto ptr = self.lock()) {
                    return ptr->HandleCreateNode(future, name, started);
                }

                return TCompletedRequest{
                    NProto::ACTION_CREATE_NODE,
                    started,
                    MakeError(E_CANCELLED, "cancelled")};
            });
    }

    TCompletedRequest HandleCreateNode(
        const TFuture<NProto::TCreateNodeResponse>& future,
        const TString& name,
        TInstant started)
    {
        TGuard<TMutex> guard(StateLock);
        Y_ABORT_UNLESS(StagedNodes.erase(name));

        try {
            auto response = future.GetValue();
            CheckResponse(response);

            Nodes[name] = TNode{name, response.GetNode()};
            return {NProto::ACTION_CREATE_NODE, started, response.GetError()};
        } catch (const TServiceError& e)  {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR("create node %s has failed: %s",
                name.c_str(),
                FormatError(error).c_str());

            return {NProto::ACTION_CREATE_NODE, started, error};
        }
    }

    TFuture<TCompletedRequest> DoRenameNode()
    {
        TGuard<TMutex> guard(StateLock);

        if (Nodes.empty()) {
            return DoCreateNode();
        }

        auto started = TInstant::Now();

        auto it = Nodes.begin();
        auto old = it->first;
        auto newbie = GenerateNodeName();

        auto request = CreateRequest<NProto::TRenameNodeRequest>();
        request->SetNodeId(RootNodeId);
        request->SetName(old);
        request->SetNewParentId(RootNodeId);
        request->SetNewName(newbie);

        StagedNodes[newbie] = {};
        StagedNodes[old] = std::move(it->second);
        Nodes.erase(it);

        auto self = weak_from_this();
        return Session->RenameNode(CreateCallContext(), std::move(request)).Apply(
            [=, old = std::move(old), newbie = std::move(newbie)] (const TFuture<NProto::TRenameNodeResponse>& future) {
                if (auto ptr = self.lock()) {
                    return ptr->HandleRenameNode(future, old, newbie, started);
                }

                return TCompletedRequest{
                    NProto::ACTION_RENAME_NODE,
                    started,
                    MakeError(E_CANCELLED, "cancelled")};
            });
    }

    TCompletedRequest HandleRenameNode(
        const TFuture<NProto::TRenameNodeResponse>& future,
        const TString& old,
        const TString& newbie,
        TInstant started)
    {
        TGuard<TMutex> guard(StateLock);

        auto node = std::move(StagedNodes[old]);
        StagedNodes.erase(old);
        StagedNodes.erase(newbie);

        try {
            auto response = future.GetValue();
            CheckResponse(response);

            Nodes[newbie] = std::move(node);
            return {NProto::ACTION_RENAME_NODE, started, response.GetError()};
        } catch (const TServiceError& e)  {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR("rename node %s has failed: %s",
                old.c_str(),
                FormatError(error).c_str());

            return {NProto::ACTION_RENAME_NODE, started, error};
        }
    }

    TFuture<TCompletedRequest> DoUnlinkNode()
    {
        TGuard<TMutex> guard(StateLock);
        if (Nodes.empty()) {
            return DoCreateNode();
        }

        auto started = TInstant::Now();

        auto it = Nodes.begin();
        auto name = it->first;

        auto request = CreateRequest<NProto::TUnlinkNodeRequest>();
        request->SetNodeId(RootNodeId);
        request->SetName(name);

        StagedNodes[name] = std::move(it->second);
        Nodes.erase(it);

        auto self = weak_from_this();
        return Session->UnlinkNode(CreateCallContext(), std::move(request)).Apply(
            [=, name = std::move(name)] (const TFuture<NProto::TUnlinkNodeResponse>& future) {
                if (auto ptr = self.lock()) {
                    return ptr->HandleUnlinkNode(future, name, started);
                }

                return TCompletedRequest{
                    NProto::ACTION_REMOVE_NODE,
                    started,
                    MakeError(E_CANCELLED, "cancelled")};
            });
    }

    TCompletedRequest HandleUnlinkNode(
        const TFuture<NProto::TUnlinkNodeResponse>& future,
        const TString& name,
        TInstant started)
    {
        with_lock (StateLock) {
            StagedNodes.erase(name);
        }

        try {
            auto response = future.GetValue();
            CheckResponse(response);

            return {NProto::ACTION_REMOVE_NODE, started, response.GetError()};
        } catch (const TServiceError& e)  {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR("unlink for %s has failed: %s",
                name.c_str(),
                FormatError(error).c_str());

            return {NProto::ACTION_REMOVE_NODE, started, error};
        }
    }

    TFuture<TCompletedRequest> DoCreateHandle()
    {
        static const int flags = ProtoFlag(NProto::TCreateHandleRequest::E_READ)
            | ProtoFlag(NProto::TCreateHandleRequest::E_WRITE);

        TGuard<TMutex> guard(StateLock);
        if (Nodes.empty()) {
            return DoCreateNode();
        }

        auto started = TInstant::Now();

        auto it = Nodes.begin();
        if (Nodes.size() > 1) {
            std::advance(it, Min(RandomNumber(Nodes.size() - 1), 64lu));
        }

        auto name = it->first;

        auto request = CreateRequest<NProto::TCreateHandleRequest>();
        request->SetNodeId(RootNodeId);
        request->SetName(name);
        request->SetFlags(flags);

        auto self = weak_from_this();
        return Session->CreateHandle(CreateCallContext(), std::move(request)).Apply(
            [=, name = std::move(name)] (const TFuture<NProto::TCreateHandleResponse>& future) {
                if (auto ptr = self.lock()) {
                    return ptr->HandleCreateHandle(future, name, started);
                }

                return TCompletedRequest{
                    NProto::ACTION_CREATE_HANDLE,
                    started,
                    MakeError(E_CANCELLED, "cancelled")};
            });
    }

    TCompletedRequest HandleCreateHandle(
        const TFuture<NProto::TCreateHandleResponse>& future,
        const TString name,
        TInstant started)
    {
        TGuard<TMutex> guard(StateLock);

        try {
            auto response = future.GetValue();
            CheckResponse(response);

            auto handle = response.GetHandle();
            Handles[handle] = THandle{name, handle};

            return {NProto::ACTION_CREATE_HANDLE, started, response.GetError()};
        } catch (const TServiceError& e)  {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR("create handle for %s has failed: %s",
                name.c_str(),
                FormatError(error).c_str());

            return {NProto::ACTION_CREATE_HANDLE, started, error};
        }
    }

    TFuture<TCompletedRequest> DoDestroyHandle()
    {
        TGuard<TMutex> guard(StateLock);
        if (Handles.empty()) {
            return DoCreateHandle();
        }

        auto started = TInstant::Now();
        auto it = Handles.begin();

        ui64 handle = it->first;
        auto name = it->second.Path;
        Handles.erase(it);
        if (auto it = Locks.find(handle); it != Locks.end()) {
            Locks.erase(it);
        }
        if (auto it = StagedLocks.find(handle); it != StagedLocks.end()) {
            StagedLocks.erase(it);
        }

        auto request = CreateRequest<NProto::TDestroyHandleRequest>();
        request->SetHandle(handle);

        auto self = weak_from_this();
        return Session->DestroyHandle(CreateCallContext(), std::move(request)).Apply(
            [=, name = std::move(name)] (const TFuture<NProto::TDestroyHandleResponse>& future) {
                if (auto ptr = self.lock()) {
                    return ptr->HandleDestroyHandle(name, future, started);
                }

                return TCompletedRequest{
                    NProto::ACTION_DESTROY_HANDLE,
                    started,
                    MakeError(E_CANCELLED, "cancelled")};
            });
    }

    TFuture<TCompletedRequest> DoGetNodeAttr()
    {
        TGuard<TMutex> guard(StateLock);
        if (Nodes.empty()) {
            return DoCreateNode();
        }

        auto started = TInstant::Now();
        auto it = Nodes.begin();
        if (Nodes.size() > 1) {
            std::advance(it, Min(RandomNumber(Nodes.size() - 1), 64lu));
        }

        auto name = it->first;

        auto request = CreateRequest<NProto::TGetNodeAttrRequest>();
        request->SetNodeId(RootNodeId);
        request->SetName(name);

        auto self = weak_from_this();
        STORAGE_DEBUG("GetNodeAttr client started");
        return Session->GetNodeAttr(CreateCallContext(), std::move(request))
            .Apply(
                [=, name = std::move(name)](
                    const TFuture<NProto::TGetNodeAttrResponse>& future)
                {
                    if (auto ptr = self.lock()) {
                        return ptr->HandleGetNodeAttr(name, future, started);
                    }

                    return TCompletedRequest{
                        NProto::ACTION_GET_NODE_ATTR,
                        started,
                        MakeError(E_CANCELLED, "cancelled")};
                });
    }

    TCompletedRequest HandleGetNodeAttr(
        const TString& name,
        const TFuture<NProto::TGetNodeAttrResponse>& future,
        TInstant started)
    {
        try {
            auto response = future.GetValue();
            STORAGE_DEBUG("GetNodeAttr client completed");
            CheckResponse(response);
            TGuard<TMutex> guard(StateLock);
            if (response.GetNode().SerializeAsString() !=
                Nodes[name].Attrs.SerializeAsString())
            {
                auto error = MakeError(
                    E_FAIL,
                    TStringBuilder()
                        << "attributes are not equal for node " << name << ": "
                        << response.GetNode().DebugString().Quote()
                        << " != " << Nodes[name].Attrs.DebugString().Quote());
                STORAGE_ERROR(error.GetMessage().c_str());
            }
            return {NProto::ACTION_GET_NODE_ATTR, started, {}};
        } catch (const TServiceError& e) {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR(
                "get node attr %s has failed: %s",
                name.c_str(),
                FormatError(error).c_str());

            return {NProto::ACTION_GET_NODE_ATTR, started, error};
        }
    }

    TCompletedRequest HandleDestroyHandle(
        const TString& name,
        const TFuture<NProto::TDestroyHandleResponse>& future,
        TInstant started)
    {
        try {
            auto response = future.GetValue();
            CheckResponse(response);

            return {NProto::ACTION_DESTROY_HANDLE, started, {}};
        } catch (const TServiceError& e)  {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR("destroy handle %s has failed: %s",
                name.c_str(),
                FormatError(error).c_str());

            return {NProto::ACTION_DESTROY_HANDLE, started, error};
        }
    }

    TFuture<TCompletedRequest> DoAcquireLock()
    {
        TGuard<TMutex> guard(StateLock);
        if (Handles.empty()) {
            return DoCreateHandle();
        }

        auto started = TInstant::Now();
        auto it = Handles.begin();
        while (it != Handles.end() && (Locks.contains(it->first) || StagedLocks.contains(it->first))) {
            ++it;
        }

        if (it == Handles.end()) {
            return DoCreateHandle();
        }

        auto handle = it->first;
        Y_ABORT_UNLESS(StagedLocks.insert(handle).second);

        auto request = CreateRequest<NProto::TAcquireLockRequest>();
        request->SetHandle(handle);
        request->SetOwner(OwnerId);
        request->SetLength(LockLength);

        auto self = weak_from_this();
        return Session->AcquireLock(CreateCallContext(), std::move(request)).Apply(
            [=] (const TFuture<NProto::TAcquireLockResponse>& future) {
                if (auto ptr = self.lock()) {
                    return ptr->HandleAcquireLock(handle, future, started);
                }

                return TCompletedRequest{
                    NProto::ACTION_ACQUIRE_LOCK,
                    started,
                    MakeError(E_CANCELLED, "cancelled")};
            });
    }

    TCompletedRequest HandleAcquireLock(
        ui64 handle,
        const TFuture<NProto::TAcquireLockResponse>& future,
        TInstant started)
    {
        TGuard<TMutex> guard(StateLock);

        auto it = StagedLocks.find(handle);
        if (it == StagedLocks.end()) {
            // nothing todo, file was removed
            Y_ABORT_UNLESS(!Locks.contains(handle));
            return {NProto::ACTION_ACQUIRE_LOCK, started, {}};
        }

        StagedLocks.erase(it);

        try {
            const auto& response = future.GetValue();
            CheckResponse(response);

            Locks.insert(handle);
            return {NProto::ACTION_ACQUIRE_LOCK, started, {}};
        } catch (const TServiceError& e)  {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR("acquire lock on %lu has failed: %s",
                handle,
                FormatError(error).c_str());

            return {NProto::ACTION_ACQUIRE_LOCK, started, error};
        }
    }

    TFuture<TCompletedRequest> DoReleaseLock()
    {
        TGuard<TMutex> guard(StateLock);
        if (Locks.empty()) {
            return DoAcquireLock();
        }

        auto it = Locks.begin();
        auto handle = *it;

        Y_ABORT_UNLESS(StagedLocks.insert(handle).second);
        Locks.erase(it);

        auto request = CreateRequest<NProto::TReleaseLockRequest>();
        request->SetHandle(handle);
        request->SetOwner(OwnerId);
        request->SetLength(LockLength);

        auto started = TInstant::Now();
        auto self = weak_from_this();
        return Session->ReleaseLock(CreateCallContext(), std::move(request)).Apply(
            [=] (const TFuture<NProto::TReleaseLockResponse>& future) {
                if (auto ptr = self.lock()) {
                    return ptr->HandleReleaseLock(handle, future, started);
                }

                return TCompletedRequest{
                    NProto::ACTION_RELEASE_LOCK,
                    started,
                    MakeError(E_CANCELLED, "cancelled")};
            });
    }

    TCompletedRequest HandleReleaseLock(
        ui64 handle,
        const TFuture<NProto::TReleaseLockResponse>& future,
        TInstant started)
    {
        TGuard<TMutex> guard(StateLock);

        auto it = StagedLocks.find(handle);
        if (it != StagedLocks.end()) {
            StagedLocks.erase(it);
        }

        try {
            CheckResponse(future.GetValue());
            return {NProto::ACTION_RELEASE_LOCK, started, {}};
        } catch (const TServiceError& e)  {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR("release lock on %lu has failed: %s",
                handle,
                FormatError(error).c_str());

            return {NProto::ACTION_RELEASE_LOCK, started, error};
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
        return TStringBuilder() << Headers.GetClientId() << ":" << CreateGuidAsString();
    }

    template <typename T>
    void CheckResponse(const T& response)
    {
        if (HasError(response)) {
            throw TServiceError(response.GetError());
        }
    }

    TIntrusivePtr<TCallContext> CreateCallContext()
    {
        return MakeIntrusive<TCallContext>(FileSystemId, AtomicIncrement(LastRequestId));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRequestGeneratorPtr CreateIndexRequestGenerator(
    NProto::TIndexLoadSpec spec,
    ILoggingServicePtr logging,
    ISessionPtr session,
    TString filesystemId,
    NProto::THeaders headers)
{
    return std::make_shared<TIndexRequestGenerator>(
        std::move(spec),
        std::move(logging),
        std::move(session),
        std::move(filesystemId),
        std::move(headers));
}

}   // namespace NCloud::NFileStore::NLoadTest
