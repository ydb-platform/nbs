#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

// Thin MPI abstraction. When built without MPI (USE_MPI != "local"),
// all operations behave as if there is a single rank.

struct TMpiContext
{
    int Rank = 0;
    int Size = 1;

    bool IsRoot() const
    {
        return Rank == 0;
    }
};

// Initialize MPI (no-op when MPI is not available).
// Must be called before any other MPI function.
TMpiContext MpiInit(int* argc, char*** argv);

// Finalize MPI (no-op when MPI is not available).
void MpiFinalize();

// Barrier across all ranks (no-op for single rank).
void MpiBarrier(const TMpiContext& ctx);

// Sum-reduce ui64 values from all ranks to rank 0.
// Returns the global sum on rank 0, local value on other ranks.
ui64 MpiReduceSum(const TMpiContext& ctx, ui64 value);

// Gather a string from every rank onto rank 0.
// Returns a vector of Size strings on rank 0, empty vector on other ranks.
TVector<TString> MpiGatherStrings(const TMpiContext& ctx, const TString& local);

}   // namespace NCloud::NFileStore
