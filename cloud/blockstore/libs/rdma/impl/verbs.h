#pragma once

#include "public.h"

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

namespace NCloud::NBlockStore::NRdma::NVerbs {

////////////////////////////////////////////////////////////////////////////////

#define RDMA_DECLARE_PTR(name, type, dereg)                                    \
    using T##name##Ptr = std::unique_ptr<type, decltype(&dereg)>;              \
    inline T##name##Ptr WrapPtr(type* ptr) { return { ptr, dereg }; }          \
// RDMA_DECLARE_PTR

RDMA_DECLARE_PTR(Context, ibv_context, ibv_close_device);
RDMA_DECLARE_PTR(DeviceList, ibv_device*, ibv_free_device_list);
RDMA_DECLARE_PTR(ProtectionDomain, ibv_pd, ibv_dealloc_pd);
RDMA_DECLARE_PTR(MemoryRegion, ibv_mr, ibv_dereg_mr);
RDMA_DECLARE_PTR(CompletionChannel, ibv_comp_channel, ibv_destroy_comp_channel);
RDMA_DECLARE_PTR(CompletionQueue, ibv_cq, ibv_destroy_cq);
RDMA_DECLARE_PTR(AddressInfo, rdma_addrinfo, rdma_freeaddrinfo);
RDMA_DECLARE_PTR(EventChannel, rdma_event_channel, rdma_destroy_event_channel);
RDMA_DECLARE_PTR(ConnectionEvent, rdma_cm_event, rdma_ack_cm_event);
RDMA_DECLARE_PTR(Connection, rdma_cm_id, rdma_destroy_id);

#undef RDMA_DECLARE_PTR

////////////////////////////////////////////////////////////////////////////////

struct TNullPtr
{
    template <typename T, typename F>
    operator std::unique_ptr<T, F> () const
    {
        return { nullptr, nullptr };
    }
};

constexpr TNullPtr NullPtr;

////////////////////////////////////////////////////////////////////////////////

struct ICompletionHandler
{
    virtual ~ICompletionHandler() = default;
    virtual void HandleCompletionEvent(ibv_wc* wc) = 0;
};

struct IVerbs
{
    virtual ~IVerbs() = default;

    virtual TContextPtr OpenDevice(ibv_device* device) = 0;
    virtual TDeviceListPtr GetDeviceList() = 0;

    virtual TProtectionDomainPtr CreateProtectionDomain(
        ibv_context* context) = 0;
    virtual TMemoryRegionPtr RegisterMemoryRegion(
        ibv_pd* pd,
        void* addr,
        size_t length,
        int flags) = 0;

    virtual TCompletionChannelPtr CreateCompletionChannel(
        ibv_context* context) = 0;
    virtual TCompletionQueuePtr CreateCompletionQueue(
            ibv_context* context,
            int cqe,
            void *cq_context,
            ibv_comp_channel *channel,
            int comp_vector) = 0;

    virtual void RequestCompletionEvent(ibv_cq* cq, int solicitedOnly) = 0;
    virtual void* GetCompletionEvent(ibv_cq* cq) = 0;
    virtual void AckCompletionEvents(ibv_cq* cq, unsigned int count) = 0;

    virtual bool PollCompletionQueue(
        ibv_cq* cq,
        ICompletionHandler* handler) = 0;

    virtual void PostSend(ibv_qp* qp, ibv_send_wr* wr) = 0;
    virtual void PostRecv(ibv_qp* qp, ibv_recv_wr* wr) = 0;

    // connection manager

    virtual TAddressInfoPtr GetAddressInfo(
        const TString& host,
        ui32 port,
        rdma_addrinfo* hints) = 0;

    virtual TEventChannelPtr CreateEventChannel() = 0;
    virtual TConnectionEventPtr GetConnectionEvent(
        rdma_event_channel* channel) = 0;

    virtual TConnectionPtr CreateConnection(
        rdma_event_channel* channel,
        void* context,
        rdma_port_space ps,
        ui8 tos) = 0;

    virtual void BindAddress(rdma_cm_id* id, sockaddr* addr) = 0;
    virtual void ResolveAddress(
        rdma_cm_id* id,
        sockaddr* src,
        sockaddr* dst,
        TDuration timeout) = 0;
    virtual void ResolveRoute(rdma_cm_id* id, TDuration timeout) = 0;
    virtual TString GetPeer(rdma_cm_id* id) = 0;

    virtual void Listen(rdma_cm_id* id, int backlog) = 0;
    virtual void Connect(rdma_cm_id* id, rdma_conn_param* param) = 0;
    virtual void Disconnect(rdma_cm_id* id) = 0;
    virtual void Accept(rdma_cm_id* id, rdma_conn_param* param) = 0;
    virtual void Reject(rdma_cm_id* id,
        const void* private_data,
        ui8 private_data_len) = 0;

    virtual void CreateQP(rdma_cm_id* id, ibv_qp_init_attr* attr) = 0;
    virtual void DestroyQP(rdma_cm_id* id) = 0;

    virtual void ModifyQP(ibv_qp *qp, ibv_qp_attr* attr, int mask) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IVerbsPtr CreateVerbs();

////////////////////////////////////////////////////////////////////////////////

const char* GetStatusString(ibv_wc_status status);
const char* GetOpcodeName(ibv_wc_opcode opcode);
const char* GetEventName(rdma_cm_event_type event);

TString PrintAddress(const sockaddr* addr);
TString PrintConnectionParams(const rdma_conn_param* param);
TString PrintCompletion(ibv_wc* wc);

}   // namespace NCloud::NBlockStore::NRdma::NVerbs
