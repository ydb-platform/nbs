#include "executor.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage::NQuery {
namespace {

////////////////////////////////////////////////////////////////////////////////

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
            id,
            NTable::TColumn(name, id, NScheme::TTypeInfo(type), ""));
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
        UNIT_ASSERT_VALUES_EQUAL(9, bound.TableId);
        UNIT_ASSERT_VALUES_EQUAL(4, bound.ReadColumnIds.size());
        UNIT_ASSERT_VALUES_EQUAL(3, bound.ReadColumnIds[0]);
        UNIT_ASSERT_VALUES_EQUAL(4, bound.ReadColumnIds[1]);
        UNIT_ASSERT_VALUES_EQUAL(1, bound.ReadColumnIds[2]);
        UNIT_ASSERT_VALUES_EQUAL(7, bound.ReadColumnIds[3]);
        UNIT_ASSERT_VALUES_EQUAL(2, bound.OutputPositions.size());
        UNIT_ASSERT_VALUES_EQUAL(0, bound.OutputPositions[0]);
        UNIT_ASSERT_VALUES_EQUAL(0, bound.OutputPositions[1]);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            bound.Columns.at("ChildId").IndexInReadColumnIds);
        UNIT_ASSERT_VALUES_EQUAL(
            NKikimr::NScheme::NTypeIds::Uint64,
            bound.Columns.at("ChildId").Type.GetTypeId());
        UNIT_ASSERT(bound.Query.Where);
        UNIT_ASSERT(bound.Query.Limit);
        UNIT_ASSERT_VALUES_EQUAL(0, *bound.Query.Limit);
    }

    Y_UNIT_TEST(ShouldExpandStarInColumnIdOrder)
    {
        auto result = BindQuery("SELECT * FROM NodeRefs");
        UNIT_ASSERT_C(!HasError(result.GetError()), result.GetError());
        const auto bound = result.ExtractResult();
        UNIT_ASSERT_VALUES_EQUAL(4, bound.ReadColumnIds.size());
        UNIT_ASSERT_VALUES_EQUAL(4, bound.OutputPositions.size());
        const TVector<ui32> expected = {1, 3, 4, 7};
        for (size_t i = 0; i < expected.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(expected[i], bound.ReadColumnIds[i]);
            UNIT_ASSERT_VALUES_EQUAL(i, bound.OutputPositions[i]);
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
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, result.GetError().GetCode());
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
        UNIT_ASSERT_VALUES_EQUAL(4, bound.ReadColumnIds.size());
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            bound.Columns.at("NodeId").IndexInReadColumnIds);
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            bound.Columns.at("Count").IndexInReadColumnIds);
    }

    Y_UNIT_TEST(ShouldAllowProjectionOfTypesUnsupportedInPredicates)
    {
        auto schema = MakeSchema();
        schema.Tables.at(9).Columns.at(7).PType =
            NKikimr::NScheme::TTypeInfo(NKikimr::NScheme::NTypeIds::Double);
        const TVector<TString> queries = {
            "SELECT Count FROM NodeRefs",
            "SELECT * FROM NodeRefs",
        };
        for (const auto& query: queries) {
            auto parsed = Parse(query);
            UNIT_ASSERT_C(!HasError(parsed.GetError()), parsed.GetError());
            auto result = Bind(parsed.ExtractResult(), schema);
            UNIT_ASSERT_C(!HasError(result.GetError()), result.GetError());
            const auto bound = result.ExtractResult();
            UNIT_ASSERT_VALUES_EQUAL(
                NKikimr::NScheme::NTypeIds::Double,
                bound.Columns.at("Count").Type.GetTypeId());
        }
    }

    Y_UNIT_TEST(ShouldRejectUnsupportedTypesInPredicates)
    {
        auto schema = MakeSchema();
        schema.Tables.at(9).Columns.at(7).PType =
            NKikimr::NScheme::TTypeInfo(NKikimr::NScheme::NTypeIds::Double);
        const TVector<TString> queries = {
            "SELECT Name FROM NodeRefs WHERE Count = Count",
            "SELECT Name FROM NodeRefs WHERE Count IN {Count}",
        };
        for (const auto& query: queries) {
            auto parsed = Parse(query);
            UNIT_ASSERT_C(!HasError(parsed.GetError()), parsed.GetError());
            auto result = Bind(parsed.ExtractResult(), schema);
            UNIT_ASSERT_C(HasError(result.GetError()), query);
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, result.GetError().GetCode());
            UNIT_ASSERT_STRING_CONTAINS(
                result.GetError().GetMessage(),
                "WHERE predicates support only Uint32, Uint64 and String "
                "columns");
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage::NQuery
