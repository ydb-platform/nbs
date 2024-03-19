#include <contrib/ydb/core/driver_lib/version/version.h>

namespace NKikimr::NPDisk {

////////////////////////////////////////////////////////////////////////////////

extern const ui64 YdbDefaultPDiskSequence = 0;

}   // namespace NKikimr::NPDisk

NKikimrConfig::TCurrentCompatibilityInfo
NKikimr::TCompatibilityInfo::MakeCurrent()
{
    using namespace NKikimr;
    using TCurrentConstructor =
        TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo;

    return TCurrentConstructor{
        .Application = "nbs",
    }.ToPB();
}
