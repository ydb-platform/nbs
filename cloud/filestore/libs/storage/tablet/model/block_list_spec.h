#pragma once

#include "public.h"

namespace NCloud::NFileStore::NStorage {
namespace NBlockListSpec {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t MergedGroupMinSize = 10;

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TListHeader
{
    enum ETypes
    {
        Blocks = 1,
        DeletionMarkers = 2,
    };

    ui32 ListType : 8;
    ui32 Unused : 24;

    TListHeader(ui8 listType)
        : ListType(listType)
        , Unused(0)
    {}
};

static_assert(sizeof(TListHeader) == 4, "");

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TGroupHeader
{
    ui64 NodeId;
    ui64 CommitId : 63;
    ui64 IsMulti : 1;

    TGroupHeader(ui64 nodeId, ui64 commitId, bool isMulti)
        : NodeId(nodeId)
        , CommitId(commitId)
        , IsMulti(isMulti)
    {}
};

static_assert(sizeof(TGroupHeader) == 16, "");

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TMultiGroupHeader
{
    enum ETypes
    {
        MergedGroup = 1,
        MixedGroup = 2,
    };

    ui32 GroupType : 8;
    ui32 Count : 16;
    ui32 Unused : 8;

    TMultiGroupHeader(ui8 groupType, ui16 count)
        : GroupType(groupType)
        , Count(count)
        , Unused(0)
    {}
};

static_assert(sizeof(TMultiGroupHeader) == 4, "");

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TBlockEntry
{
    ui32 BlockIndex;
    ui32 BlobOffset : 16;
    ui32 Unused : 16;

    TBlockEntry(ui32 blockIndex, ui16 blobOffset)
        : BlockIndex(blockIndex)
        , BlobOffset(blobOffset)
        , Unused(0)
    {}
};

static_assert(sizeof(TBlockEntry) == 8, "");

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TSingleBlockEntry
    : TGroupHeader
    , TBlockEntry
{
    TSingleBlockEntry(
        ui64 nodeId,
        ui64 commitId,
        ui32 blockIndex,
        ui16 blobOffset)
        : TGroupHeader(nodeId, commitId, false)
        , TBlockEntry(blockIndex, blobOffset)
    {}
};

static_assert(sizeof(TSingleBlockEntry) == 16 + 8);

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TMergedBlockGroup
    : TGroupHeader
    , TMultiGroupHeader
    , TBlockEntry
{
    TMergedBlockGroup(
        ui64 nodeId,
        ui64 commitId,
        ui16 count,
        ui32 blockIndex,
        ui16 blobOffset)
        : TGroupHeader(nodeId, commitId, true)
        , TMultiGroupHeader(TMultiGroupHeader::MergedGroup, count)
        , TBlockEntry(blockIndex, blobOffset)
    {}
};

static_assert(sizeof(TMergedBlockGroup) == 16 + 4 + 8);

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TMixedBlockGroup
    : TGroupHeader
    , TMultiGroupHeader
{
    TMixedBlockGroup(ui64 nodeId, ui64 commitId, ui16 count)
        : TGroupHeader(nodeId, commitId, true)
        , TMultiGroupHeader(TMultiGroupHeader::MixedGroup, count)
    {}

    // ui32 BlockIndex[]
    // ui16 BlobOffset[]
};

static_assert(sizeof(TMixedBlockGroup) == 16 + 4);

////////////////////////////////////////////////////////////////////////////////

struct TDeletionMarker
{
    ui32 BlobOffset : 16;
    ui32 Unused : 16;

    TDeletionMarker(ui16 blobOffset)
        : BlobOffset(blobOffset)
        , Unused(0)
    {}
};

static_assert(sizeof(TDeletionMarker) == 4);

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TSingleDeletionMarker
    : TGroupHeader
    , TDeletionMarker
{
    TSingleDeletionMarker(ui64 commitId, ui16 blobOffset)
        : TGroupHeader(0, commitId, false)
        , TDeletionMarker(blobOffset)
    {}
};

static_assert(sizeof(TSingleDeletionMarker) == 16 + 4);

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TMergedDeletionGroup
    : TGroupHeader
    , TMultiGroupHeader
    , TDeletionMarker
{
    TMergedDeletionGroup(ui64 commitId, ui16 count, ui16 blobOffset)
        : TGroupHeader(0, commitId, true)
        , TMultiGroupHeader(TMultiGroupHeader::MergedGroup, count)
        , TDeletionMarker(blobOffset)
    {}
};

static_assert(sizeof(TMergedDeletionGroup) == 16 + 4 + 4);

////////////////////////////////////////////////////////////////////////////////

struct Y_PACKED TMixedDeletionGroup
    : TGroupHeader
    , TMultiGroupHeader
{
    TMixedDeletionGroup(ui64 commitId, ui16 count)
        : TGroupHeader(0, commitId, true)
        , TMultiGroupHeader(TMultiGroupHeader::MixedGroup, count)
    {}

    // ui16 BlobOffset[]
};

static_assert(sizeof(TMixedDeletionGroup) == 16 + 4);

}   // namespace NBlockListSpec
}   // namespace NCloud::NFileStore::NStorage
