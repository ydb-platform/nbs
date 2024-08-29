#include "large_blocks.h"

#include "alloc.h"

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TLargeBlocks::TImpl
{
    // TODO

    IAllocator* Alloc;
};

////////////////////////////////////////////////////////////////////////////////

TLargeBlocks::TLargeBlocks(IAllocator* alloc)
    : Impl(new TImpl{alloc})
{}

TLargeBlocks::~TLargeBlocks() = default;

void TLargeBlocks::AddDeletionMarker(TDeletionMarker deletionMarker)
{
    Y_UNUSED(deletionMarker);
    // TODO
}

void TLargeBlocks::ApplyDeletionMarkers(TVector<TBlock>& blocks) const
{
    Y_UNUSED(blocks);
    // TODO
}

void TLargeBlocks::ApplyAndUpdateDeletionMarkers(TVector<TBlock>& blocks)
{
    Y_UNUSED(blocks);
    // TODO
}

TVector<TDeletionMarker> TLargeBlocks::ExtractProcessedDeletionMarkers()
{
    // TODO
    return {};
}

}   // namespace NCloud::NFileStore::NStorage
