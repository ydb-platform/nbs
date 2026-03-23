#include "mpi.h"

// Stub implementation used when MPI is not available.
// Single-rank semantics: rank=0, size=1.

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

TMpiContext MpiInit(int* argc, char*** argv)
{
    Y_UNUSED(argc);
    Y_UNUSED(argv);
    return {};
}

void MpiFinalize()
{}

void MpiBarrier(const TMpiContext& ctx)
{
    Y_UNUSED(ctx);
}

ui64 MpiReduceSum(const TMpiContext& ctx, ui64 value)
{
    Y_UNUSED(ctx);
    return value;
}

TVector<TString> MpiGatherStrings(const TMpiContext& ctx, const TString& local)
{
    Y_UNUSED(ctx);
    return {local};
}

}   // namespace NCloud::NFileStore
