#pragma once

#include <library/cpp/charset/ci_string.h>
#include <util/system/compat.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>


// This enumeration defines both
// 1. which settings of HTTP Accept-Language should be used when fetching the page
// 2. what region the page is fetched from; by 'region' we understand 'geo ip'
enum ELangRegion {
    ELR_RU = 0,         // RU is preffered language, fetch from russian ips
    ELR_US = 1,         // EN is preffered language, fetch from US ips
    ELR_TR = 2,
    ELR_DE = 3,
    ELR_IT = 4,
    ELR_UA = 5,
    ELR_UZ = 6,
    ELR_MAX
};

struct TLangRegionInfo {
    ELangRegion LanguageRegion;
    TString RobotName;
    TString SmallRobotName;
    TString Alpha3IsoName;
};

const TLangRegionInfo& GetLangRegionInfo(ELangRegion langRegion);

// returns info about region whose name is `str`
// checks all names: RobotName,SmallRobotName,Alpha3IsoName
//
// returns nullptr on invalid str
const TLangRegionInfo* ParseLangRegionInfo(TStringBuf str);

inline const char* LangRegion2Str(ELangRegion langRegion, bool robot = true) {
    if (langRegion < ELR_MAX) {
        const TLangRegionInfo& info = GetLangRegionInfo(langRegion);
        return robot ? info.RobotName.data() : info.SmallRobotName.data();
    }
    return robot ? "Unknown" : "";
}

inline ELangRegion Str2LangRegion(const char *str) {
    const TLangRegionInfo* info = ParseLangRegionInfo(str);
    if (info) {
        return info->LanguageRegion;
    }
    return ELR_MAX;
}

inline ui32 GetAttrValueByLangRegion(const ELangRegion langRegion) {
    switch (langRegion) {
    case ELR_RU:
        return 225; // here and later, all "magic numbers" are from geo.c2n
    case ELR_US:
        return 84;
    case ELR_TR:
        return 983;
    default:
        return 318; // we write "universal" value, if we don't know lang region. it's possible if spider has bigger list of lang regions than robot
    }
}
