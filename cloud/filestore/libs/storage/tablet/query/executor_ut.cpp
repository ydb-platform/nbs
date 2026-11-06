#include "executor.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage::NQuery {
namespace {

NKikimr::NTable::TScheme MakeSchema()
{
    using namespace NKikimr;
    NTable::TScheme schema;
    schema.TableNames.emplace("NodeRefs", 9);
    auto& table =
        schema.Tables.emplace(9, NTable::TScheme::TTableInfo("NodeRefs", 9))
            .first->second;
    const auto addColumn = [&](ui32 id, TString name, NScheme::TTypeId type)
    {
        table.ColumnNames.emplace(name, id);
        table.Columns.emplace(
            id, NTable::TColumn(name, id, NScheme::TTypeInfo(type), ""));
    };
    addColumn(1, "NodeId", NScheme::NTypeIds::Uint64);
    addColumn(3, "Name", NScheme::NTypeIds::String);
    addColumn(4, "ChildId", NScheme::NTypeIds::Uint64);
    addColumn(7, "Count", NScheme::NTypeIds::Uint32);
    table.KeyColumns = {1, 3};
    return schema;
}

TResultOrError<TBoundSelect> BindQuery(TStringBuf text)
{
    auto parsed = Parse(text);
    UNIT_ASSERT_C(!HasError(parsed.GetError()), parsed.GetError());
    return Bind(parsed.ExtractResult(), MakeSchema());
}

}   // namespace

Y_UNIT_TEST_SUITE(TQueryBinderTest)
{
    Y_UNIT_TEST(ShouldResolveProjectionAndPredicateColumns)
    {
        auto result = BindQuery(
            "SELECT Name, Name FROM NodeRefs "
            "WHERE ChildId = NodeId AND (Name SUBSTR 'foo' OR Count IN {1, 2}) "
            "LIMIT 0");
        UNIT_ASSERT_C(!HasError(result.GetError()), result.GetError());
        auto bound = result.ExtractResult();
        UNIT_ASSERT_VALUES_EQUAL(bound.TableId, 9);
        UNIT_ASSERT_VALUES_EQUAL(bound.ReadColumnIds.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(bound.ReadColumnIds[0], 3);
        UNIT_ASSERT_VALUES_EQUAL(bound.ReadColumnIds[1], 4);
        UNIT_ASSERT_VALUES_EQUAL(bound.ReadColumnIds[2], 1);
        UNIT_ASSERT_VALUES_EQUAL(bound.ReadColumnIds[3], 7);
        UNIT_ASSERT_VALUES_EQUAL(bound.OutputPositions.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(bound.OutputPositions[0], 0);
        UNIT_ASSERT_VALUES_EQUAL(bound.OutputPositions[1], 0);
        UNIT_ASSERT_VALUES_EQUAL(bound.Columns.at("ChildId").ReadPosition, 1);
        UNIT_ASSERT_VALUES_EQUAL(
            bound.Columns.at("ChildId").Type.GetTypeId(),
            NKikimr::NScheme::NTypeIds::Uint64);
        UNIT_ASSERT(bound.Query.Where);
        UNIT_ASSERT(bound.Query.Limit);
        UNIT_ASSERT_VALUES_EQUAL(*bound.Query.Limit, 0);
    }

    Y_UNIT_TEST(ShouldExpandStarInColumnIdOrder)
    {
        auto result = BindQuery("SELECT * FROM NodeRefs");
        UNIT_ASSERT_C(!HasError(result.GetError()), result.GetError());
        const auto bound = result.ExtractResult();
        UNIT_ASSERT_VALUES_EQUAL(bound.ReadColumnIds.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(bound.OutputPositions.size(), 4);
        const TVector<ui32> expected = {1, 3, 4, 7};
        for (size_t i = 0; i < expected.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(bound.ReadColumnIds[i], expected[i]);
            UNIT_ASSERT_VALUES_EQUAL(bound.OutputPositions[i], i);
        }
    }

    Y_UNIT_TEST(ShouldRejectUnknownNamesAndInvalidTypes)
    {
        const TVector<TString> queries = {
            "SELECT * FROM Missing",
            "SELECT Missing FROM NodeRefs",
            "SELECT Name FROM NodeRefs WHERE Missing = 1",
            "SELECT Name FROM NodeRefs WHERE ChildId = Missing",
            "SELECT Name FROM NodeRefs WHERE ChildId = 'hello'",
            "SELECT Name FROM NodeRefs WHERE Name = 1",
            "SELECT Name FROM NodeRefs WHERE ChildId = Name",
            "SELECT Name FROM NodeRefs WHERE ChildId SUBSTR 'x'",
            "SELECT Name FROM NodeRefs WHERE Name SUBSTR ChildId",
            "SELECT Name FROM NodeRefs WHERE Count = 4294967296",
            "SELECT Name FROM NodeRefs WHERE ChildId IN {1, 'x'}",
            "SELECT Name FROM NodeRefs WHERE Name = 'x' OR Missing = 1",
        };
        for (const auto& query: queries) {
            auto result = BindQuery(query);
            UNIT_ASSERT_C(HasError(result.GetError()), query);
            UNIT_ASSERT_VALUES_EQUAL(result.GetError().GetCode(), E_ARGUMENT);
            UNIT_ASSERT(!result.GetError().GetMessage().empty());
        }
    }

    Y_UNIT_TEST(ShouldAcceptNumericBoundariesAndColumnComparisons)
    {
        auto result = BindQuery(
            "SELECT Count FROM NodeRefs WHERE Count = 4294967295 "
            "AND ChildId = 18446744073709551615 AND Count < NodeId");
        UNIT_ASSERT_C(!HasError(result.GetError()), result.GetError());
    }

    Y_UNIT_TEST(ShouldResolveColumnOperandsInInPredicate)
    {
        auto result = BindQuery(
            "SELECT Name FROM NodeRefs WHERE ChildId IN {NodeId, Count, 42}");
        UNIT_ASSERT_C(!HasError(result.GetError()), result.GetError());
        const auto bound = result.ExtractResult();
        UNIT_ASSERT_VALUES_EQUAL(bound.ReadColumnIds.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(bound.Columns.at("NodeId").ReadPosition, 2);
        UNIT_ASSERT_VALUES_EQUAL(bound.Columns.at("Count").ReadPosition, 3);
    }

    Y_UNIT_TEST(ShouldRejectUnsupportedColumnTypes)
    {
        auto schema = MakeSchema();
        schema.Tables.at(9).Columns.at(7).PType =
            NKikimr::NScheme::TTypeInfo(NKikimr::NScheme::NTypeIds::Double);
        auto parsed = Parse("SELECT * FROM NodeRefs");
        UNIT_ASSERT(!HasError(parsed.GetError()));
        auto result = Bind(parsed.ExtractResult(), schema);
        UNIT_ASSERT(HasError(result.GetError()));
        UNIT_ASSERT_STRING_CONTAINS(
            result.GetError().GetMessage(),
            "Unsupported type for column NodeRefs.Count");
    }
}

}   // namespace NCloud::NFileStore::NStorage::NQuery
