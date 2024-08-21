#pragma once

#include "commit.h"

#include <cloud/storage/core/libs/common/alloc.h>

#include <util/digest/multi.h>
#include <util/generic/hash_set.h>
#include <util/stream/output.h>
#include <util/system/defaults.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

class TPartialBlobId
{
private:
    union
    {
        struct
        {
            ui64 Step: 32;
            ui64 Generation: 32;
        };
        ui64 Raw;
    } CommitId_;

    union
    {
        struct
        {
            ui64 Channel: 8;
            ui64 Cookie: 24;
            ui64 BlobSize: 28;
            ui64 PartId: 4;
        };
        ui64 Raw;
    } UniqueId_;

public:
    TPartialBlobId()
    {
        CommitId_.Raw = 0;
        UniqueId_.Raw = 0;
    }

    TPartialBlobId(ui64 commitId, ui64 uniqueId)
    {
        CommitId_.Raw = commitId;
        UniqueId_.Raw = uniqueId;
    }

    TPartialBlobId(ui32 generation,
                   ui32 step,
                   ui32 channel,
                   ui32 blobSize,
                   ui32 cookie,
                   ui32 partId)
    {
        CommitId_.Step = step;
        CommitId_.Generation = generation;

        UniqueId_.Channel = channel;
        UniqueId_.Cookie = cookie;
        UniqueId_.BlobSize = blobSize;
        UniqueId_.PartId = partId;
    }

    ui64 CommitId() const
    {
        return CommitId_.Raw;
    }

    ui32 Generation() const
    {
        return CommitId_.Generation;
    }

    ui32 Step() const
    {
        return CommitId_.Step;
    }

    ui64 UniqueId() const
    {
        return UniqueId_.Raw;
    }

    ui32 Channel() const
    {
        return UniqueId_.Channel;
    }

    ui32 Cookie() const
    {
        return UniqueId_.Cookie;
    }

    ui32 BlobSize() const
    {
        return UniqueId_.BlobSize;
    }

    ui32 PartId() const
    {
        return UniqueId_.PartId;
    }

    ui64 GetHash() const
    {
        return MultiHash(CommitId_.Raw, UniqueId_.Raw);
    }

    explicit operator bool() const
    {
        return CommitId_.Raw != 0 || UniqueId_.Raw != 0;
    }

    bool operator ==(const TPartialBlobId& other) const
    {
        return CommitId_.Raw == other.CommitId_.Raw
            && UniqueId_.Raw == other.UniqueId_.Raw;
    }

    bool operator !=(const TPartialBlobId& other) const
    {
        return CommitId_.Raw != other.CommitId_.Raw
            || UniqueId_.Raw != other.UniqueId_.Raw;
    }

    bool operator <(const TPartialBlobId& other) const
    {
        return CommitId_.Raw < other.CommitId_.Raw
            || CommitId_.Raw == other.CommitId_.Raw && UniqueId_.Raw < other.UniqueId_.Raw;
    }

    bool operator <=(const TPartialBlobId& other) const
    {
        return CommitId_.Raw < other.CommitId_.Raw
            || CommitId_.Raw == other.CommitId_.Raw && UniqueId_.Raw <= other.UniqueId_.Raw;
    }

    bool operator >(const TPartialBlobId& other) const
    {
        return other < *this;
    }

    bool operator >=(const TPartialBlobId& other) const
    {
        return other <= *this;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TPartialBlobIdHash
{
    ui64 operator ()(const TPartialBlobId& blobId) const
    {
        return blobId.GetHash();
    }
};

using TPartialBlobIdHashSet = THashSet<
    TPartialBlobId,
    TPartialBlobIdHash,
    TEqualTo<TPartialBlobId>,
    TStlAlloc<TPartialBlobId>>;

////////////////////////////////////////////////////////////////////////////////

inline TPartialBlobId MakePartialBlobId(ui64 commitId, ui64 uniqueId)
{
    return TPartialBlobId(commitId, uniqueId);
}

inline bool IsDeletionMarker(const TPartialBlobId& blobId)
{
    return !blobId.BlobSize();
}

inline TPartialBlobId NextBlobId(TPartialBlobId blobId, ui64 maxUniqueId)
{
    if (blobId.UniqueId() == maxUniqueId) {
        return MakePartialBlobId(blobId.CommitId() + 1, 0);
    }
    return MakePartialBlobId(blobId.CommitId(), blobId.UniqueId() + 1);
}

////////////////////////////////////////////////////////////////////////////////

const TPartialBlobId InvalidPartialBlobId = TPartialBlobId(InvalidCommitId, 0);

}   // namespace NCloud

////////////////////////////////////////////////////////////////////////////////

template <>
inline void Out<NCloud::TPartialBlobId>(
    IOutputStream& out,
    const NCloud::TPartialBlobId& pbid)
{
    out << pbid.Generation()
        << "::" << pbid.Step()
        << "::" << pbid.Channel()
        << "::" << pbid.Cookie()
        << "::" << pbid.BlobSize()
        << "::" << pbid.PartId();
}
