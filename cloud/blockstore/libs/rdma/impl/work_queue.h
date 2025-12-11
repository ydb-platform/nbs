#pragma once

#include "public.h"

#include <util/generic/utility.h>
#include <util/stream/output.h>
#include <util/stream/printf.h>

#include <rdma/rdma_verbs.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t RDMA_MAX_SEND_SGE = 1;
constexpr size_t RDMA_MAX_RECV_SGE = 1;

////////////////////////////////////////////////////////////////////////////////

union TWorkRequestId {
    ui64 Id;

    struct
    {
        ui32 Magic;
        ui16 Generation;
        ui16 Index;
    };

    TWorkRequestId(ui64 id)
        : Id(id)
    {}

    TWorkRequestId(ui32 magic, ui16 generation, ui16 index)
        : Magic(magic)
        , Generation(generation)
        , Index(index)
    {}
};

////////////////////////////////////////////////////////////////////////////////

inline IOutputStream& operator<<(IOutputStream& out, const TWorkRequestId& id)
{
    Printf(out, "%08X.%X.%X", id.Magic, id.Generation, id.Index);
    return out;
}

////////////////////////////////////////////////////////////////////////////////

struct TSendWr
{
    ibv_send_wr wr;
    ibv_sge sg_list[RDMA_MAX_SEND_SGE];

    void* context;

    TSendWr()
    {
        Zero(*this);
    }

    template <typename T>
    T* Message()
    {
        return reinterpret_cast<T*>(wr.sg_list[0].addr);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TRecvWr
{
    ibv_recv_wr wr;
    ibv_sge sg_list[RDMA_MAX_RECV_SGE];

    void* context;

    TRecvWr()
    {
        Zero(*this);
    }

    template <typename T>
    T* Message()
    {
        return reinterpret_cast<T*>(wr.sg_list[0].addr);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TWorkQueue
{
private:
    T* Head = nullptr;
    size_t Size_ = 0;

public:
    void Push(T* wr)
    {
        Y_DEBUG_ABORT_UNLESS(wr);
        wr->wr.next = Head ? &Head->wr : nullptr;
        wr->context = nullptr;
        Head = wr;
        Size_++;
    }

    T* Pop()
    {
        auto* wr = Head;
        if (wr) {
            Head = reinterpret_cast<T*>(wr->wr.next);
            wr->wr.next = nullptr;
            Size_--;
        }
        return wr;
    }

    void Clear()
    {
        Head = nullptr;
        Size_ = 0;
    }

    size_t Size() const
    {
        return Size_;
    }
};

}   // namespace NCloud::NBlockStore::NRdma
