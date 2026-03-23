#include "mpi.h"

#include <util/generic/yexception.h>

#include <mpi.h>

// Real MPI implementation. Compiled only when USE_MPI == "local".

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

TMpiContext MpiInit(int* argc, char*** argv)
{
    int provided = 0;
    // Request MPI_THREAD_FUNNELED: only the main thread calls MPI functions.
    MPI_Init_thread(argc, argv, MPI_THREAD_FUNNELED, &provided);

    TMpiContext ctx;
    MPI_Comm_rank(MPI_COMM_WORLD, &ctx.Rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ctx.Size);
    return ctx;
}

void MpiFinalize()
{
    MPI_Finalize();
}

void MpiBarrier(const TMpiContext& ctx)
{
    Y_UNUSED(ctx);
    MPI_Barrier(MPI_COMM_WORLD);
}

ui64 MpiReduceSum(const TMpiContext& ctx, ui64 value)
{
    ui64 result = 0;
    MPI_Reduce(
        &value,
        &result,
        1,
        MPI_UINT64_T,
        MPI_SUM,
        /*root*/ 0,
        MPI_COMM_WORLD);
    return ctx.IsRoot() ? result : value;
}

TVector<TString> MpiGatherStrings(const TMpiContext& ctx, const TString& local)
{
    // First exchange lengths
    int localLen = static_cast<int>(local.size());
    TVector<int> lengths(ctx.IsRoot() ? ctx.Size : 0);
    MPI_Gather(
        &localLen, 1, MPI_INT,
        lengths.data(), 1, MPI_INT,
        /*root*/ 0,
        MPI_COMM_WORLD);

    if (!ctx.IsRoot()) {
        MPI_Gatherv(
            local.data(), localLen, MPI_CHAR,
            nullptr, nullptr, nullptr, MPI_CHAR,
            /*root*/ 0,
            MPI_COMM_WORLD);
        return {};
    }

    // Build displacements
    TVector<int> displs(ctx.Size);
    int total = 0;
    for (int i = 0; i < ctx.Size; ++i) {
        displs[i] = total;
        total += lengths[i];
    }

    TVector<char> buf(total);
    MPI_Gatherv(
        local.data(), localLen, MPI_CHAR,
        buf.data(), lengths.data(), displs.data(), MPI_CHAR,
        /*root*/ 0,
        MPI_COMM_WORLD);

    TVector<TString> result(ctx.Size);
    for (int i = 0; i < ctx.Size; ++i) {
        result[i] = TString(buf.data() + displs[i], lengths[i]);
    }
    return result;
}

}   // namespace NCloud::NFileStore
