#include <contrib/ydb/core/driver_lib/version/version.h>

namespace NKikimr::NPDisk {
extern const ui64 YdbDefaultPDiskSequence = 0;
}

NKikimrConfig::TCurrentCompatibilityInfo
NKikimr::TCompatibilityInfo::MakeCurrent()
{
    using TCurrentConstructor = NKikimr::TCompatibilityInfo::TProtoConstructor::
        TCurrentCompatibilityInfo;

    return TCurrentConstructor{
        .Application = "nbs",
    }.ToPB();
}
