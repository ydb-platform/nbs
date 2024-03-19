#include <ydb/core/driver_lib/version/version.h>

NKikimrConfig::TCurrentCompatibilityInfo NKikimr::TCompatibilityInfo::MakeCurrent() {
    using TCurrentConstructor = NKikimr::TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo;

    return TCurrentConstructor{
        .Application = "ydb",
    }.ToPB();
}
