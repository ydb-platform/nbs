#include "langregion.h"

#include <library/cpp/charset/ci_string.h>
#include <util/generic/array_size.h>
#include <util/generic/yexception.h>

namespace {

const TLangRegionInfo LANGUAGE_REGION_INFO_ARRAY[] = {
    {ELR_RU, "RU", "ru", "RUS"},
    {ELR_US, "US", "us", "USA"},
    {ELR_TR, "TUR", "tur", "TUR"},
    {ELR_DE, "DE", "de", "DEU"},
    {ELR_IT, "IT", "it", "ITA"},
    {ELR_UA, "UA", "ua", "UKR"},
    {ELR_UZ, "UZ", "uz", "UZB"},
};

static_assert(Y_ARRAY_SIZE(LANGUAGE_REGION_INFO_ARRAY) == ELR_MAX,
    "not all of ELangRegion values are listed in LANGUAGE_REGION_INFO_ARRAY");

} // namespace // anonymous

const TLangRegionInfo& GetLangRegionInfo(ELangRegion langRegion) {
    if (langRegion >= Y_ARRAY_SIZE(LANGUAGE_REGION_INFO_ARRAY)) {
        ythrow yexception() << "invalid language region: " << int(langRegion);
    }

    return LANGUAGE_REGION_INFO_ARRAY[langRegion];
}

const TLangRegionInfo* ParseLangRegionInfo(TStringBuf str) {
    for (const auto& langRegionInfo : LANGUAGE_REGION_INFO_ARRAY) {
        // use case insensitive comparison
        if (TCiString::compare(langRegionInfo.RobotName, str) == 0 ||
            TCiString::compare(langRegionInfo.Alpha3IsoName, str) == 0)
        {
            return &langRegionInfo;
        }
    }
    return nullptr;
}
