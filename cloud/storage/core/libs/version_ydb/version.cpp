#include <ydb/core/driver_lib/version/version.h>

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
