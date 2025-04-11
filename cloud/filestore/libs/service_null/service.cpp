#include "service.h"

#include <cloud/filestore/libs/service/filestore.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/hash.h>
#include <util/generic/size_literals.h>

namespace NCloud::NFileStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

// predefined session state to restore connection upon restart
static constexpr TStringBuf SessionState = "\010\007\020\037\030\333\370\377\010 \213\370R(\200\240@";

void Fill(NProto::TFileStore& filestore)
{
    filestore.SetBlockSize(4_KB);
    filestore.SetBlocksCount(1000000);
    filestore.SetNodesCount(1000000);
}

void Fill(NProto::TNodeAttr& attr, ui64 nodeId)
{
    attr.SetId(nodeId);
    attr.SetMode(0775);
    if (nodeId == RootNodeId) {
        attr.SetType(NProto::E_DIRECTORY_NODE);
    } else {
        attr.SetType(NProto::E_REGULAR_NODE);
        attr.SetSize(500_GB);
        attr.SetLinks(1);
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TDummyFileStore
    : public IFileStoreService
{
#define FILESTORE_IMPLEMENT_METHOD(name, ...)                                  \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(callContext);                                                 \
        Y_UNUSED(request);                                                     \
        return MakeFuture(NProto::T##name##Response());                        \
    }                                                                          \
// FILESTORE_IMPLEMENT_METHOD

    FILESTORE_SERVICE(FILESTORE_IMPLEMENT_METHOD)
    FILESTORE_IMPLEMENT_METHOD(ReadDataLocal)
    FILESTORE_IMPLEMENT_METHOD(WriteDataLocal)

#undef FILESTORE_IMPLEMENT_METHOD
};

////////////////////////////////////////////////////////////////////////////////

struct TNullFileStore final
    : public TDummyFileStore
{
    THashMap<TString, ui64> NodesIndex = {
        {"a.txt", 1},
        {"b.txt", 2},
    };

    void Start() override
    {}

    void Stop() override
    {}

    TFuture<NProto::TCreateSessionResponse> CreateSession(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TCreateSessionRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        NProto::TCreateSessionResponse response;
        *response.MutableSession()->MutableSessionState() = SessionState;

        return MakeFuture(response);
    }

    TFuture<NProto::TStatFileStoreResponse> StatFileStore(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TStatFileStoreRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        NProto::TStatFileStoreResponse response;
        Fill(*response.MutableFileStore());

        return MakeFuture(response);
    }

    TFuture<NProto::TGetFileStoreInfoResponse> GetFileStoreInfo(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TGetFileStoreInfoRequest> request) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        NProto::TGetFileStoreInfoResponse response;
        Fill(*response.MutableFileStore());

        return MakeFuture(response);
    }

    TFuture<NProto::TCreateNodeResponse> CreateNode(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TCreateNodeRequest> request) override
    {
        Y_UNUSED(callContext);

        NProto::TCreateNodeResponse response;
        Fill(*response.MutableNode(), request->GetNodeId());

        return MakeFuture(response);
    }

    TFuture<NProto::TListNodesResponse> ListNodes(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TListNodesRequest> request) override
    {
        Y_UNUSED(callContext);

        TString name, cookie;
        NProto::TListNodesResponse response;
        if (!request->GetCookie()) {
            *response.AddNames() = "a.txt";
            *response.MutableCookie() = "cookie";
            Fill(*response.AddNodes(), RootNodeId + NodesIndex["a.txt"]);
        } else if (request->GetCookie() == "cookie"){
            *response.AddNames() = "b.txt";
            *response.MutableCookie() = "end";
            Fill(*response.AddNodes(), RootNodeId + NodesIndex["a.txt"]);
        } else {
            // end
        }

        return MakeFuture(response);
    }

    TFuture<NProto::TGetNodeAttrResponse> GetNodeAttr(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TGetNodeAttrRequest> request) override
    {
        Y_UNUSED(callContext);

        NProto::TGetNodeAttrResponse response;
        // i.e. lookup by node id or by parent & name
        // beware: for some pairs (nodeId, name) may produce invalid node id
        // because of int overflow
        ui64 nodeId = request->GetNodeId() + THash<TString>{}(request->GetName());
        Fill(*response.MutableNode(), nodeId);

        return MakeFuture(response);
    }

    TFuture<NProto::TSetNodeAttrResponse> SetNodeAttr(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TSetNodeAttrRequest> request) override
    {
        Y_UNUSED(callContext);

        NProto::TSetNodeAttrResponse response;
        Fill(*response.MutableNode(), request->GetNodeId());

        return MakeFuture(response);
    }

    TFuture<NProto::TCreateHandleResponse> CreateHandle(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TCreateHandleRequest> request) override
    {
        Y_UNUSED(callContext);

        NProto::TCreateHandleResponse response;
        response.SetHandle(1);
        Fill(*response.MutableNodeAttr(), request->GetNodeId());

        return MakeFuture(response);
    }

    TFuture<NProto::TReadDataResponse> ReadData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request) override
    {
        Y_UNUSED(callContext);

        NProto::TReadDataResponse response;
        response.MutableBuffer()->assign(request->GetLength(), '\0');

        return MakeFuture(response);
    }

    void GetSessionEventsStream(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TGetSessionEventsRequest> request,
        IResponseHandlerPtr<NProto::TGetSessionEventsResponse> responseHandler) override
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);
        Y_UNUSED(responseHandler);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileStoreServicePtr CreateNullFileStore()
{
    return std::make_shared<TNullFileStore>();
}

}   // namespace NCloud::NFileStore
