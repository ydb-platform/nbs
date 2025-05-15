#include "action_graph.h"

namespace NCloud::NBlockStore::NLoadTest {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TGraphExecutor::TGraphExecutor(TGraph graph)
    : Graph(std::move(graph))
    , Vertex2DepCount(Graph.Vertices.size())
    , OutgoingEdges(Graph.Vertices.size())
    , DoneCount(0)
{
    Y_ENSURE(Graph.Vertices.size() == Graph.Edges.size());

    for (TVertexId i = 0; i < Graph.Edges.size(); ++i) {
        for (TVertexId j : Graph.Edges[i]) {
            OutgoingEdges[j].push_back(i);
        }
        Vertex2DepCount[i] = Graph.Edges[i].size();
    }

    for (TVertexId i = 0; i < Graph.Vertices.size(); ++i) {
        if (!Vertex2DepCount[i]) {
            Ready.push_back(i);
        }
    }

    ThreadPool.Start();
}

void TGraphExecutor::Run()
{
    while (true) {
        TVector<TVertexId> ready;

        with_lock (Lock) {
            if (DoneCount == Graph.Vertices.size()) {
                return;
            }

            Ready.swap(ready);
        }

        for (TVertexId vertexId : ready) {
            RunVertex(vertexId).Subscribe([=, this] (auto) {
                with_lock (Lock) {
                    for (const auto other : OutgoingEdges[vertexId]) {
                        auto& depCount = Vertex2DepCount[other];
                        Y_DEBUG_ABORT_UNLESS(depCount > 0);
                        if (!--depCount) {
                            Ready.push_back(other);
                        }
                    }
                    ++DoneCount;
                }

                ReadyEvent.Signal();
            });
        }

        ReadyEvent.WaitI();
        ReadyEvent.Reset();
    }
}

TFuture<void> TGraphExecutor::RunVertex(TVertexId vertexId)
{
    TPromise<void> promise = NewPromise();

    ThreadPool.SafeAddFunc(
        [=, this] () mutable {
            Graph.Vertices[vertexId]();
            promise.SetValue();
        }
    );

    return promise;
}

}   // namespace NCloud::NBlockStore::NLoadTest
