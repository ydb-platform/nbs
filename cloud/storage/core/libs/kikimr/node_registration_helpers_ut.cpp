#include "node_registration_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

namespace NCloud {

using namespace NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TMessage>
void CheckAllFieldsSet(
    const TMessage& msg,
    const std::unordered_set<TString>& skipFields)
{
    for (int i = 0; i < msg.GetDescriptor()->field_count(); ++i) {
        const auto* field = msg.GetDescriptor()->field(i);
        if (skipFields.contains(field->name())) {
            continue;
        }

        if (field->is_repeated()) {
            // TODO: handle repeated and depricated fields
            continue;
        }

        UNIT_ASSERT_C(
            field->has_presence(),
            TStringBuilder()
                << "Field "
                << field->DebugString()
                << "does not track presence");

        UNIT_ASSERT_C(
            msg.GetMetadata().reflection->HasField(msg, field),
            TStringBuilder()
                << "Unset field: "
                << field->DebugString());
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNodeRegistrationHelpersTest)
{
    Y_UNIT_TEST(ShouldFillNodeInfo)
    {
        NYdb::NDiscovery::TNodeInfo info;
        auto msg =  CreateNodeInfo(info);
        CheckAllFieldsSet(msg, {});
    }

    Y_UNIT_TEST(ShouldFillLocationInfo)
    {
        NYdb::NDiscovery::TNodeLocation location;
        auto msg =  CreateNodeLocation(location);
        CheckAllFieldsSet(msg, {});
    }

    Y_UNIT_TEST(ShouldFillStaticNodeInfo)
    {
        {
            NYdb::NDiscovery::TNodeInfo info;
            auto msg =  CreateStaticNodeInfo(info);
            CheckAllFieldsSet(msg, {"WalleLocation"});
        }

        {
            NKikimrNodeBroker::TNodeInfo info;
            auto msg =  CreateStaticNodeInfo(info);
            CheckAllFieldsSet(msg, {"WalleLocation"});
        }
    }
}

}   // namespace NCloud
