#pragma once
#include "defs.h"

#include <contrib/ydb/core/blobstorage/base/blobstorage_vdiskid.h>

namespace NKikimr {

struct TTestConfig : public TThrRefBase {
    TVDiskID VDiskID;
    TActorId YardActorID;

    TTestConfig(const TVDiskID &vDiskID, const TActorId &yardActorID)
        : VDiskID(vDiskID)
        , YardActorID(yardActorID)
    {}
};


} // NKikimr
