#pragma once

#include "verbs.h"
#include "poll.h"

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/generic/deque.h>
#include <util/generic/vector.h>
#include <util/system/spinlock.h>

namespace NCloud::NStorage::NRdma::NVerbs {

////////////////////////////////////////////////////////////////////////////////

struct TTestContext: TAtomicRefCount<TTestContext>
{
    rdma_cm_id* Connection = nullptr;
    TEventHandle ConnectionHandle;
    TVector<std::unique_ptr<rdma_cm_id>> ClientConnections;
    TDeque<NVerbs::TConnectionEventPtr> ConnectionEvents;
    bool AllowConnect = false;
    TSpinLock ConnectionLock;
    ui8 ToS = 0;

    TEventHandle CompletionHandle;
    TDeque<ibv_send_wr*> SendEvents;
    TDeque<ui32> ReqIds;
    TDeque<ibv_recv_wr*> RecvEvents;
    TDeque<ibv_recv_wr*> ProcessedRecvEvents;
    TSpinLock CompletionLock;
    TAtomic PostRecvCounter = 0;

    std::function<void(ibv_qp* qp, ibv_send_wr* wr)> PostSend;
    std::function<void(ibv_qp* qp, ibv_recv_wr* wr)> PostRecv;
    std::function<void(rdma_cm_id* id, int backlog)> Listen;
    std::function<void(ibv_wc* wc)> HandleCompletionEvent;
    std::function<void(rdma_cm_id* id, ibv_qp_init_attr* attr)> CreateQP;
    std::function<void(rdma_cm_id* id, const void* data, ui8 size)> Reject;
    std::function<
        TAddressInfoPtr(const TString& host, ui32 port, rdma_addrinfo* hints)>
        GetAddressInfo;
    std::function<void(rdma_cm_id* id)> DestroyQP;
    std::function<void(ibv_qp* qp, ibv_qp_attr* attr, int mask)> ModifyQP;
    std::function<void(ibv_pd* pd, void* addr, size_t length, int flags)>
        RegisterMemoryRegion;

    // If set, overrides the default behavior of TTestVerbs::Connect. The test
    // can use it together with EnqueueAcceptEvent / EnqueueRejectEvent helpers
    // to simulate custom server-side responses (e.g. specific reject messages
    // or accept messages with a particular protocol version).
    std::function<void(rdma_cm_id* id, rdma_conn_param* param)> HandleConnect;
};

using TTestContextPtr = TIntrusivePtr<TTestContext>;

////////////////////////////////////////////////////////////////////////////////

IVerbsPtr CreateTestVerbs(TTestContextPtr context);

void CreateConnection(TTestContextPtr context);
void CreateConnection(
    TTestContextPtr context,
    ui16 sendQueueSize,
    ui16 recvQueueSize,
    ui32 maxBufferSize);
void Disconnect(TTestContextPtr context);

// Enqueues an RDMA_CM_EVENT_ESTABLISHED for the given connection with the
// supplied private data (typically a TAcceptMessage). Intended to be used from
// TTestContext::HandleConnect.
void EnqueueAcceptEvent(
    TTestContextPtr context,
    rdma_cm_id* id,
    const void* privateData,
    size_t privateDataLen);

// Enqueues an RDMA_CM_EVENT_REJECTED for the given connection with the
// supplied private data (typically a TRejectMessage / TRejectMessage2).
// Intended to be used from TTestContext::HandleConnect.
void EnqueueRejectEvent(
    TTestContextPtr context,
    rdma_cm_id* id,
    const void* privateData,
    size_t privateDataLen);

}   // namespace NCloud::NStorage::NRdma::NVerbs
