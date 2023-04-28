#pragma once

#include "public.h"

#include <library/cpp/threading/future/future.h>

#include <util/generic/vector.h>
#include <util/system/event.h>
#include <util/system/spinlock.h>
#include <util/thread/pool.h>

#include <functional>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

struct TGraph
{
    using TAction = std::function<void()>;
    TVector<TAction> Vertices;
    TVector<TVector<size_t>> Edges;
};

using TVertexId = ui32;

////////////////////////////////////////////////////////////////////////////////

class TGraphExecutor
{
private:
    TGraph Graph;
    TVector<ui32> Vertex2DepCount;
    TVector<TVector<TVertexId>> OutgoingEdges;

    TVector<TVertexId> Ready;
    TManualEvent ReadyEvent;
    ui32 DoneCount;
    TAdaptiveLock Lock;

    TAdaptiveThreadPool ThreadPool;

public:
    TGraphExecutor(TGraph graph);

public:
    void Run();

private:
    NThreading::TFuture<void> RunVertex(TVertexId vertexId);
};

}   // namespace NCloud::NBlockStore::NLoadTest
