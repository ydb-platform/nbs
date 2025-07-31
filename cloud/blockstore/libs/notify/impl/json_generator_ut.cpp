#include "json_generator.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NNotify {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool AreJsonMapsEqual(
    const NJson::TJsonValue& lhs,
    const NJson::TJsonValue& rhs)
{
    using namespace NJson;

    Y_ABORT_UNLESS(lhs.GetType() == JSON_MAP, "lhs has not a JSON_MAP type.");

    if (rhs.GetType() != JSON_MAP) {
        return false;
    }

    typedef TJsonValue::TMapType TMapType;
    const TMapType& lhsMap = lhs.GetMap();
    const TMapType& rhsMap = rhs.GetMap();

    if (lhsMap.size() != rhsMap.size()) {
        return false;
    }

    for (const auto& lhsIt: lhsMap) {
        TMapType::const_iterator rhsIt = rhsMap.find(lhsIt.first);
        if (rhsIt == rhsMap.end()) {
            return false;
        }

        if (lhsIt.second != rhsIt->second) {
            return false;
        }
    }

    return true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TJsonGeneratorSuite)
{
    Y_UNIT_TEST(ShouldGenerateCorrectJsonWithoutDiskError)
    {
        IJsonGeneratorPtr jsonGenerator = std::make_unique<TJsonGenerator>();

        auto json = jsonGenerator->Generate({
            .CloudId = "yc-nbs",
            .FolderId = "yc-nbs.folder",
            .Timestamp = TInstant::ParseIso8601("2024-04-01T00:00:01Z"),
            .Event = TDiskBackOnline{.DiskId = "nrd0"},
        });

        NJson::TJsonMap correctJsonMap{
            {"type", "nbs.nonrepl.back-online"},
            {"data",
             NJson::TJsonMap{
                 {"cloudId", "yc-nbs"},
                 {"folderId", "yc-nbs.folder"},
                 {"diskId", "nrd0"}}},
            {"cloudId", "yc-nbs"}};

        UNIT_ASSERT(AreJsonMapsEqual(json, correctJsonMap));
    }

    Y_UNIT_TEST(ShouldGenerateCorrectJsonWithDiskError)
    {
        IJsonGeneratorPtr jsonGenerator = std::make_unique<TJsonGenerator>();

        auto json = jsonGenerator->Generate({
            .CloudId = "yc-nbs",
            .FolderId = "yc-nbs.folder",
            .Timestamp = TInstant::ParseIso8601("2024-04-01T00:00:01Z"),
            .Event = TDiskError{.DiskId = "DiskError"},
        });

        NJson::TJsonMap correctJsonMap{
            {"type", "nbs.nonrepl.error"},
            {"data",
             NJson::TJsonMap{
                 {"cloudId", "yc-nbs"},
                 {"folderId", "yc-nbs.folder"},
                 {"diskId", "DiskError"}}},
            {"cloudId", "yc-nbs"}};

        UNIT_ASSERT(AreJsonMapsEqual(json, correctJsonMap));
    }
}

}   // namespace NCloud::NBlockStore::NNotify
