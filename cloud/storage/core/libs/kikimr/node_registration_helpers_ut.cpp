#include "node_registration_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

#include <unordered_set>

namespace NCloud {

using namespace NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

// This function can only check that primitive type fields are set.
// "presence" does not work for repeated fields and function will fail.
// So you have to add such fields to skipFields (to guarantee that you know what
// you are doing) and check them outside of this function.

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

        if (field->options().deprecated()) {
            continue;
        }

        UNIT_ASSERT_C(
            field->has_presence(),
            TStringBuilder()
                << "Field "
                << field->DebugString()
                << "does not track presence");

        if (field->label() != NProtoBuf::FieldDescriptor::LABEL_OPTIONAL) {
            UNIT_ASSERT_C(
                msg.GetMetadata().reflection->HasField(msg, field),
                TStringBuilder()
                    << "Unset field: "
                    << field->DebugString());
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNodeRegistrationHelpersTest)
{
    Y_UNIT_TEST(ShouldFillNodeInfo)
    {
        NYdb::NDiscovery::TNodeInfo info;

        {
            auto msg = CreateNodeInfo(info, {});
            CheckAllFieldsSet(msg, {"Name"});
        }

        {
            auto msg = CreateNodeInfo(info, "xyz");
            CheckAllFieldsSet(msg, {});
        }
    }

    Y_UNIT_TEST(ShouldFillLocationInfo)
    {
        NYdb::NDiscovery::TNodeLocation location;
        location.DataCenter = "data center";
        location.Module = "module";
        location.Rack = "rack";
        location.Unit = "unit";
        auto msg = CreateNodeLocation(location);
        CheckAllFieldsSet(msg, {});
    }

    Y_UNIT_TEST(ShouldFillStaticNodeInfo)
    {
        {
            NYdb::NDiscovery::TNodeInfo info;
            auto msg = CreateStaticNodeInfo(info);
            CheckAllFieldsSet(msg, {"Endpoint"});
        }

        {
            NKikimrNodeBroker::TNodeInfo info;
            auto msg = CreateStaticNodeInfo(info);
            CheckAllFieldsSet(msg, {"Endpoint"});
        }
    }
}

}   // namespace NCloud
