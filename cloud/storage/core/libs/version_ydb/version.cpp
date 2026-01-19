#include <contrib/ydb/core/driver_lib/version/version.h>

NKikimrConfig::TCurrentCompatibilityInfo
NKikimr::TCompatibilityInfo::MakeCurrent()
{
    using namespace NKikimr;
    using TCurrentConstructor =
        TCompatibilityInfo::TProtoConstructor::TCurrentCompatibilityInfo;
    using TVersionConstructor =
        NKikimr::TCompatibilityInfo::TProtoConstructor::TVersion;
    using TCompatibilityRuleConstructor =
        NKikimr::TCompatibilityInfo::TProtoConstructor::TCompatibilityRule;

    return TCurrentConstructor{
        .Application = "nbs",
        .Version =
            TVersionConstructor{
                .Year = 24,
                .Major = 4,
            },
        .CanLoadFrom =
            {
                TCompatibilityRuleConstructor{
                    .LowerLimit = TVersionConstructor{.Year = 24, .Major = 1},
                    .UpperLimit = TVersionConstructor{.Year = 25, .Major = 1},
                },
            },
        .StoresReadableBy =
            {
                TCompatibilityRuleConstructor{
                    .LowerLimit = TVersionConstructor{.Year = 24, .Major = 1},
                    .UpperLimit = TVersionConstructor{.Year = 25, .Major = 1},
                },
            },
        .CanConnectTo =
            {
                TCompatibilityRuleConstructor{
                    .LowerLimit = TVersionConstructor{.Year = 24, .Major = 1},
                    .UpperLimit = TVersionConstructor{.Year = 25, .Major = 1},
                },
            }}
        .ToPB();
}
