#include <ydb/core/driver_lib/version/version.h>

NKikimrConfig::TCurrentCompatibilityInfo NKikimr::TCompatibilityInfo::MakeCurrent() {
    using TCurrentConstructor = NKikimr::TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo;
    using TVersionConstructor = NKikimr::TCompatibilityInfo::TProtoConstructor::TVersion;

    return TCurrentConstructor{
        .Application = "ydb",
        .Version = TVersionConstructor{
            .Year = 23,
            .Major = 3,
        }
    }.ToPB();
}
