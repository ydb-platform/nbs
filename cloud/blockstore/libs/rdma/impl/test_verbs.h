#pragma once

#include "verbs.h"

#include <cloud/blockstore/libs/rdma/iface/poll.h>

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

    TEventHandle CompletionHandle;
    TDeque<ibv_send_wr*> SendEvents;
    TDeque<ui32> ReqIds;
    TDeque<ibv_recv_wr*> RecvEvents;
    TDeque<ibv_recv_wr*> ProcessedRecvEvents;
    TSpinLock CompletionLock;

    TAtomic PostRecv = 0;
};

using TTestContextPtr = TIntrusivePtr<TTestContext>;

////////////////////////////////////////////////////////////////////////////////

IVerbsPtr CreateTestVerbs(TTestContextPtr context);

void Disconnect(TTestContextPtr context);

}   // namespace NCloud::NBlockStore::NRdma::NVerbs
