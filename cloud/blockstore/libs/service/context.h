#pragma once

#include "public.h"

#include <cloud/blockstore/public/api/protos/latency.pb.h>

#include <cloud/storage/core/libs/common/context.h>

#include <memory>
#include <mutex>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TLatencyVolumeRequest;

struct TCallContext final: public TCallContextBase
{
private:
    TAtomic SilenceRetriableErrors = false;
    TAtomic HasUncountableRejects = false;
    TAtomic LatencyEnabled = false;
    mutable std::mutex LatencyLock;
    std::shared_ptr<const NProto::TLatencyDiagnostics> LatencyDiagnostics;

public:
    TCallContext(ui64 requestId = 0);

    bool GetSilenceRetriableErrors() const;
    void SetSilenceRetriableErrors(bool silence);

    bool GetHasUncountableRejects() const;
    void SetHasUncountableRejects();

    void EnableLatency();
    bool IsLatencyEnabled() const;

    // Publish only the merged graph for the original operation, after all
    // children/retries complete. Never publish an individual child's graph.
    void SetLatencyDiagnostics(NProto::TLatencyDiagnostics diagnostics);
    std::shared_ptr<const NProto::TLatencyDiagnostics>
    GetLatencyDiagnostics() const;
};

////////////////////////////////////////////////////////////////////////////////

inline TCallContextPtr CreateCallContext(ui64 requestId = 0)
{
    return MakeIntrusive<TCallContext>(requestId);
}

////////////////////////////////////////////////////////////////////////////////

TCallContextPtr ToBlockStoreCallContext(TCallContextBasePtr callContext);

}   // namespace NCloud::NBlockStore
