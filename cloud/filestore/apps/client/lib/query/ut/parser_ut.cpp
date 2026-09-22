#include <cloud/filestore/apps/client/lib/query/parser.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NClient::NQuery {

Y_UNIT_TEST_SUITE(TQueryParserTest)
{
    Y_UNIT_TEST(ShouldParseSelectAll)
    {
        auto query = Parse(
            "SELECT * FROM NodeRefs WHERE child_id IN {1, 2, 3} LIMIT 10");

        UNIT_ASSERT(query);
        UNIT_ASSERT_VALUES_EQUAL(query->Table, "NodeRefs");
        UNIT_ASSERT(query->Columns.empty());
        UNIT_ASSERT_VALUES_EQUAL(query->Conditions.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(query->Conditions[0].Column, "child_id");
        UNIT_ASSERT_VALUES_EQUAL(query->Conditions[0].Values.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(*query->Limit, 10);
    }

    Y_UNIT_TEST(ShouldParseProjectionAndString)
    {
        auto query = Parse(
            "select name, child_id from NodeRefs "
            "where name = 'hello' and node_id = 42");

        UNIT_ASSERT(query);
        UNIT_ASSERT_VALUES_EQUAL(query->Columns.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(query->Conditions.size(), 2);
        UNIT_ASSERT(std::holds_alternative<TString>(
            query->Conditions[0].Values[0]));
    }

    Y_UNIT_TEST(ShouldRejectEmptyIn)
    {
        TParseError error;
        UNIT_ASSERT(!Parse("SELECT * FROM NodeRefs WHERE child_id IN {}", &error));
        UNIT_ASSERT(!error.Message.empty());
    }

    Y_UNIT_TEST(ShouldRejectUnknownCharacters)
    {
        TParseError error;
        UNIT_ASSERT(!Parse(
            "SELECT * FROM NodeRefs @ WHERE child_id = 1",
            &error));
        UNIT_ASSERT(!error.Message.empty());
    }
}

}   // namespace NCloud::NFileStore::NClient::NQuery
