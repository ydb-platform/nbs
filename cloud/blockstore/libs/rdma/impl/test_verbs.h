#pragma once

#include "verbs.h"
#include "poll.h"

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/generic/deque.h>
#include <util/system/spinlock.h>

namespace NCloud::NBlockStore::NRdma::NVerbs {

////////////////////////////////////////////////////////////////////////////////

struct TTestContext: TAtomicRefCount<TTestContext>
{
    rdma_cm_id* Connection = nullptr;
    TEventHandle ConnectionHandle;
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

    std::function<void(ibv_qp* qp, ibv_recv_wr* wr)> PostRecv;
    std::function<void(rdma_cm_id* id, int backlog)> Listen;
    std::function<void(ibv_wc* wc)> HandleCompletionEvent;
};

using TTestContextPtr = TIntrusivePtr<TTestContext>;

////////////////////////////////////////////////////////////////////////////////

IVerbsPtr CreateTestVerbs(TTestContextPtr context);

void Disconnect(TTestContextPtr context);

}   // namespace NCloud::NBlockStore::NRdma::NVerbs
