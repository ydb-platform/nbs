#include "test_verbs.h"

#include "utils.h"

#include <cloud/blockstore/libs/rdma/iface/protocol.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/network/address.h>
#include <util/stream/format.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/system/error.h>

namespace NCloud::NBlockStore::NRdma::NVerbs {

////////////////////////////////////////////////////////////////////////////////

struct TConnectionEvent
    : rdma_cm_event
{
    bool HaveConnParam = false;

    TConnectionEvent(
            rdma_cm_event_type eventType,
            rdma_cm_id* cmId,
            rdma_cm_id* listenId,
            rdma_conn_param* connParam)
        : rdma_cm_event{}
    {
        id = cmId;
        listen_id = listenId;
        event = eventType;

        if (connParam) {
            param.conn = *connParam;
            void* data = malloc(param.conn.private_data_len);
            memcpy(
                data,
                connParam->private_data,
                connParam->private_data_len);
            param.conn.private_data = data;
            HaveConnParam = true;
        }
    }

    static int Destroy(rdma_cm_event* event)
    {
        auto* ce = static_cast<TConnectionEvent*>(event);
        if (ce->HaveConnParam) {
            free(const_cast<void*>(ce->param.conn.private_data));
        }
        delete ce;
        return 0;
    }
};

void EnqueueConnectionEvent(
    TTestContextPtr context,
    rdma_cm_event_type type,
    rdma_cm_id* id,
    rdma_cm_id* listenId = nullptr,
    rdma_conn_param* param = nullptr)
{
    auto* event = new TConnectionEvent(type, id, listenId, param);

    auto g = Guard(context->ConnectionLock);
    context->ConnectionEvents.push_back({
        static_cast<rdma_cm_event*>(event),
        TConnectionEvent::Destroy,
    });

    context->ConnectionHandle.Set();
}

////////////////////////////////////////////////////////////////////////////////

// TODO: implement queue pairs (qp) - right now the code in this file completely
// ignores the queue pair entity.

struct TTestVerbs
    : IVerbs
{
    TTestContextPtr TestContext;

    TTestVerbs(TTestContextPtr testContext)
        : TestContext(std::move(testContext))
    {
    }

    TDeviceListPtr GetDeviceList() override
    {
        return NullPtr;
    }

    TContextPtr OpenDevice(ibv_device* device) override
    {
        Y_UNUSED(device);

        return NullPtr;
    }

    TProtectionDomainPtr CreateProtectionDomain(ibv_context* context) override
    {
        Y_UNUSED(context);

        return NullPtr;
    }

    struct TMemoryRegion
        : ibv_mr
    {
        TMemoryRegion(ibv_pd* ibvPd, void* bAddr, size_t bLen)
        {
            pd = ibvPd;
            addr = bAddr;
            length = bLen;

            // TODO
            context = nullptr;
            handle = 0;
            lkey = 0;
            rkey = 0;
        }

        static int Destroy(ibv_mr* mr)
        {
            delete static_cast<TMemoryRegion*>(mr);
            return 0;
        }
    };

    TMemoryRegionPtr RegisterMemoryRegion(
        ibv_pd* pd,
        void* addr,
        size_t length,
        int flags) override
    {
        Y_UNUSED(flags);

        return {
            static_cast<ibv_mr*>(new TMemoryRegion(pd, addr, length)),
            TMemoryRegion::Destroy,
        };
    }

    struct TCompletionChannel
        : ibv_comp_channel
    {
        TCompletionChannel(TTestContextPtr testContext, ibv_context* ibvContext)
        {
            context = ibvContext;
            fd = testContext->CompletionHandle.Handle();
            refcnt = 0;
        }

        static int Destroy(ibv_comp_channel* channel)
        {
            delete static_cast<TCompletionChannel*>(channel);
            return 0;
        }
    };

    TCompletionChannelPtr CreateCompletionChannel(ibv_context* context) override
    {
        return {
            static_cast<ibv_comp_channel*>(new TCompletionChannel(
                TestContext,
                context)),
            TCompletionChannel::Destroy,
        };
    }

    TCompletionQueuePtr CreateCompletionQueue(
        ibv_context* context,
        int cqe,
        void *cq_context,
        ibv_comp_channel *channel,
        int comp_vector) override
    {
        Y_UNUSED(context);
        Y_UNUSED(cqe);
        Y_UNUSED(cq_context);
        Y_UNUSED(channel);
        Y_UNUSED(comp_vector);

        return NullPtr;
    }

    void RequestCompletionEvent(ibv_cq* cq, int solicited_only) override
    {
        Y_UNUSED(cq);
        Y_UNUSED(solicited_only);
    }

    void* GetCompletionEvent(ibv_cq* cq) override
    {
        Y_UNUSED(cq);
        return nullptr;
    }

    void AckCompletionEvents(ibv_cq* cq, unsigned int nevents) override
    {
        Y_UNUSED(cq);
        Y_UNUSED(nevents);

        TestContext->CompletionHandle.Clear();
    }

    bool PollCompletionQueue(
        ibv_cq* cq,
        ICompletionHandler* handler) override
    {
        Y_UNUSED(cq);

        TVector<ibv_send_wr*> sends;
        TVector<ibv_recv_wr*> recvs;

        with_lock (TestContext->CompletionLock) {
            sends = {
                TestContext->SendEvents.begin(),
                TestContext->SendEvents.end()
            };
            TestContext->SendEvents.clear();

            recvs = {
                TestContext->ProcessedRecvEvents.begin(),
                TestContext->ProcessedRecvEvents.end()
            };
            TestContext->ProcessedRecvEvents.clear();
        }

        auto handleEvent = [&] (ui64 wrId, ibv_wc_opcode opcode) {
            ibv_wc wc = {
                .wr_id = wrId,
                .status = IBV_WC_SUCCESS,
                .opcode = opcode,
            };
            with_lock (TestContext->CompletionLock) {
                if (TestContext->HandleCompletionEvent) {
                    TestContext->HandleCompletionEvent(&wc);
                }
            }
            handler->HandleCompletionEvent(&wc);
        };

        for (const auto& x: sends) {
            handleEvent(x->wr_id, IBV_WC_SEND);
        }

        for (const auto& x: recvs) {
            handleEvent(x->wr_id, IBV_WC_RECV);
        }

        return true;
    }

    void PostSend(ibv_qp* qp, ibv_send_wr* wr) override
    {
        Y_UNUSED(qp);

        auto g = Guard(TestContext->CompletionLock);

        const auto* msg =
            reinterpret_cast<TRequestMessage*>(wr->sg_list[0].addr);
        TestContext->ReqIds.push_back(msg->ReqId);

        TestContext->SendEvents.push_back(wr);
        TestContext->CompletionHandle.Set();
    }

    void PostRecv(ibv_qp* qp, ibv_recv_wr* wr) override
    {
        if (TestContext->PostRecv) {
            TestContext->PostRecv(qp, wr);
        }

        auto g = Guard(TestContext->CompletionLock);
        TestContext->RecvEvents.push_back(wr);

        AtomicIncrement(TestContext->PostRecvCounter);
    }

    // connection manager

    struct TAddressInfo
        : rdma_addrinfo
    {
        TSockAddrInet6 SrcAddr;
        TSockAddrInet6 DstAddr;

        TAddressInfo(const TString& host, ui32 port)
            : SrcAddr("::1", 1111)
            , DstAddr(host.data(), port)
        {
            memset(
                static_cast<rdma_addrinfo*>(this),
                0,
                sizeof(rdma_addrinfo));
            ai_src_addr = SrcAddr.SockAddr();
            ai_dst_addr = DstAddr.SockAddr();
        }

        static void Destroy(rdma_addrinfo* info)
        {
            delete static_cast<TAddressInfo*>(info);
        }
    };

    TAddressInfoPtr GetAddressInfo(
        const TString& host,
        ui32 port,
        rdma_addrinfo* hints) override
    {
        Y_UNUSED(hints);

        return {
            static_cast<rdma_addrinfo*>(new TAddressInfo(host, port)),
            TAddressInfo::Destroy,
        };
    }

    struct TEventChannel
        : rdma_event_channel
    {
        TTestContextPtr Context;

        TEventChannel(TTestContextPtr context)
            : Context(context)
        {
            fd = Context->ConnectionHandle.Handle();
        }

        static void Destroy(rdma_event_channel* channel)
        {
            delete static_cast<TEventChannel*>(channel);
        }
    };

    TEventChannelPtr CreateEventChannel() override
    {
        return {
            static_cast<rdma_event_channel*>(new TEventChannel(TestContext)),
            TEventChannel::Destroy,
        };
    }

    TConnectionEventPtr GetConnectionEvent(rdma_event_channel* channel) override
    {
        Y_UNUSED(channel);

        auto g = Guard(TestContext->ConnectionLock);
        if (TestContext->ConnectionEvents) {
            auto event = std::move(TestContext->ConnectionEvents.front());
            TestContext->ConnectionEvents.pop_front();
            return event;
        }

        return NullPtr;
    }

    struct TConnection
        : rdma_cm_id
    {
        TString Peer;

        TConnection(
            rdma_event_channel* channel,
            void* context,
            rdma_port_space ps)
        {
            memset(this, 0, sizeof(rdma_cm_id));

            Y_UNUSED(channel);
            Y_UNUSED(context);
            Y_UNUSED(ps);
        }

        static int Destroy(rdma_cm_id *id)
        {
            delete static_cast<TConnection*>(id);
            return 0;
        }
    };

    TConnectionPtr CreateConnection(
        rdma_event_channel* channel,
        void* context,
        rdma_port_space ps,
        ui8 tos) override
    {
        auto* connection = new TConnection(channel, context, ps);
        TestContext->Connection = static_cast<rdma_cm_id*>(connection);
        TestContext->ToS = tos;

        return {
            static_cast<rdma_cm_id*>(connection),
            TConnection::Destroy,
        };
    }

    void BindAddress(rdma_cm_id* id, sockaddr* addr) override
    {
        Y_UNUSED(id);
        Y_UNUSED(addr);
    }

    void ResolveAddress(
        rdma_cm_id* id,
        sockaddr* srcAddr,
        sockaddr* dstAddr,
        TDuration timeout) override
    {
        Y_UNUSED(timeout);

        static_assert(sizeof(sockaddr) <= sizeof(sockaddr_storage));
        memcpy(&id->route.addr.src_storage, srcAddr, sizeof(sockaddr));
        memcpy(&id->route.addr.dst_storage, dstAddr, sizeof(sockaddr));

        EnqueueConnectionEvent(TestContext, RDMA_CM_EVENT_ADDR_RESOLVED, id);
    }

    void ResolveRoute(rdma_cm_id* id, TDuration timeout) override
    {
        Y_UNUSED(timeout);

        EnqueueConnectionEvent(TestContext, RDMA_CM_EVENT_ROUTE_RESOLVED, id);
    }

    TString GetPeer(rdma_cm_id *id) override
    {
        auto* addr = &id->route.addr.dst_addr;

        if (addr->sa_family) {
            return PrintAddress(addr);
        }
        return "unknown";
    }

    void Listen(rdma_cm_id* id, int backlog) override
    {
        if (TestContext->Listen) {
            TestContext->Listen(id, backlog);
        }
    }

    void Reject(
        rdma_cm_id* id,
        rdma_conn_param* param,
        int status)
    {
        const auto* connect = static_cast<const TConnectMessage*>(
            param->private_data);

        // emulate internal server error
        TRejectMessage reject = {
            .Status = SafeCast<ui16>(status),
            .QueueSize = connect->QueueSize,
            .MaxBufferSize = connect->MaxBufferSize,
        };
        InitMessageHeader(&reject, RDMA_PROTO_VERSION);

        auto param2 = *param;
        param2.private_data = &reject;
        param2.private_data_len = sizeof(reject);

        EnqueueConnectionEvent(
            TestContext,
            RDMA_CM_EVENT_REJECTED,
            id,
            nullptr,    // listenId
            param);
    }

    void Connect(rdma_cm_id* id, rdma_conn_param* param) override
    {
        if (!TestContext->AllowConnect) {
            return Reject(id, param, RDMA_PROTO_FAIL);
        }

        EnqueueConnectionEvent(
            TestContext,
            RDMA_CM_EVENT_ESTABLISHED,
            id,
            nullptr,    // listenId
            param);
    }

    void Disconnect(rdma_cm_id* id) override
    {
        EnqueueConnectionEvent(TestContext, RDMA_CM_EVENT_DISCONNECTED, id);
    }

    void Accept(rdma_cm_id* id, rdma_conn_param* param) override
    {
        Y_UNUSED(id);
        Y_UNUSED(param);
    }

    void Reject(
        rdma_cm_id* id,
        const void* private_data,
        ui8 private_data_len) override
    {
        if (TestContext->Reject) {
            TestContext->Reject(id, private_data, private_data_len);
        }
    }

    void CreateQP(rdma_cm_id* id, ibv_qp_init_attr* qp_attrs) override
    {
        if (TestContext->CreateQP) {
            TestContext->CreateQP(id, qp_attrs);
        }

        auto g = Guard(TestContext->CompletionLock);
        TestContext->HandleCompletionEvent = nullptr;
    }

    void DestroyQP(rdma_cm_id* id) override
    {
        Y_UNUSED(id);
    }

    void ModifyQP(ibv_qp* qp, ibv_qp_attr* attr, int mask) override
    {
        Y_UNUSED(qp);
        Y_UNUSED(attr);
        Y_UNUSED(mask);
    }
};

////////////////////////////////////////////////////////////////////////////////

IVerbsPtr CreateTestVerbs(TTestContextPtr context)
{
    return std::make_shared<TTestVerbs>(std::move(context));
}

void CreateConnection(TTestContextPtr context)
{
    TConnectMessage message;
    InitMessageHeader(&message, RDMA_PROTO_VERSION);

    rdma_conn_param param = {
        .private_data = &message,
        .private_data_len = sizeof(TConnectMessage),
    };

    auto id = std::make_unique<rdma_cm_id>();
    memset(id.get(), 0, sizeof(rdma_cm_id));

    EnqueueConnectionEvent(
        context,
        RDMA_CM_EVENT_CONNECT_REQUEST,
        id.get(),
        context->Connection,    // listenId
        &param);

    context->ClientConnections.push_back(std::move(id));
}

void Disconnect(TTestContextPtr context)
{
    EnqueueConnectionEvent(
        context,
        RDMA_CM_EVENT_DISCONNECTED,
        static_cast<rdma_cm_id*>(context->Connection));

    with_lock (context->CompletionLock) {
        context->HandleCompletionEvent = [] (ibv_wc* wc) {
            wc->status = IBV_WC_WR_FLUSH_ERR;
            wc->opcode = static_cast<ibv_wc_opcode>(0);
        };

        std::move(
            context->RecvEvents.begin(),
            context->RecvEvents.end(),
            std::back_inserter(context->ProcessedRecvEvents));

        context->RecvEvents.clear();
        context->ReqIds.clear();
    }

    context->CompletionHandle.Set();
}

}   // namespace NCloud::NBlockStore::NRdma::NVerbs
