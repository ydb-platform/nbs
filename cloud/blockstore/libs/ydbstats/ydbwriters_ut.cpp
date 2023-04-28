#include "ydbwriters.h"

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>


namespace NCloud::NBlockStore::NYdbStats {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(YdbWritersTests)
{
    Y_UNIT_TEST(EmptyWriterTest)
    {
        IYdbWriter writer;
        TStringStream streamOut;
        NYdb::TParamsBuilder bufferOut;

        writer.Declare(streamOut);
        writer.Replace(streamOut);
        writer.PushData(bufferOut);

        UNIT_ASSERT(!writer.IsValid());
        UNIT_ASSERT(streamOut.empty());
        UNIT_ASSERT(bufferOut.Build().Empty());
    }

    Y_UNIT_TEST(ReplaceWriterTest)
    {
        static const TString resultReplace =
            "REPLACE INTO `TestTable` "
            "SELECT * FROM AS_TABLE(ItemName);\n";

        TYdbReplaceWriter writer("TestTable", "ItemName");
        TStringStream streamOut;
        NYdb::TParamsBuilder bufferOut;

        writer.Declare(streamOut);
        writer.Replace(streamOut);
        writer.PushData(bufferOut);

        UNIT_ASSERT(writer.IsValid());
        UNIT_ASSERT_VALUES_EQUAL(streamOut.Str(), resultReplace);
        UNIT_ASSERT(bufferOut.Build().Empty());
    }

    Y_UNIT_TEST(RowWriterEmptyTest)
    {
        static const TVector<TYdbRow> data;
        TYdbRowWriter writer(data, "TestTable");
        UNIT_ASSERT(!writer.IsValid());
    }

    Y_UNIT_TEST(RowWriterTest)
    {
        static const TVector<TYdbRow> data = { {} };
        static const TString resultReplace =
            TString("DECLARE $items AS List<Struct<") +
            TYdbRow::GetYdbRowDefinition() +
            ">>;\n"
            "REPLACE INTO `TestTable` "
            "SELECT * FROM AS_TABLE($items);\n";

        TYdbRowWriter writer({data}, "TestTable");
        TStringStream streamOut;
        NYdb::TParamsBuilder bufferOut;

        writer.Declare(streamOut);
        writer.Replace(streamOut);
        writer.PushData(bufferOut);

        UNIT_ASSERT(writer.IsValid());
        UNIT_ASSERT_VALUES_EQUAL(streamOut.Str(), resultReplace);
        UNIT_ASSERT(!bufferOut.Build().Empty());
    }

    Y_UNIT_TEST(BlobWriterEmptyTest)
    {
        static const TVector<TYdbBlobLoadMetricRow> data;
        TYdbBlobLoadMetricWriter writer(data, "TestTable");
        UNIT_ASSERT(!writer.IsValid());
    }

    Y_UNIT_TEST(BlobWriterTest)
    {
        static const TVector<TYdbBlobLoadMetricRow> data = { {} };
        static const TString resultReplace =
            TString("DECLARE $metrics_items AS List<Struct<") +
            TYdbBlobLoadMetricRow::GetYdbRowDefinition() +
            ">>;\n"
            "REPLACE INTO `TestTable` "
            "SELECT * FROM AS_TABLE($metrics_items);\n";

        TYdbBlobLoadMetricWriter writer({data}, "TestTable");
        TStringStream streamOut;
        NYdb::TParamsBuilder bufferOut;

        writer.Declare(streamOut);
        writer.Replace(streamOut);
        writer.PushData(bufferOut);

        UNIT_ASSERT(writer.IsValid());
        UNIT_ASSERT_VALUES_EQUAL(streamOut.Str(), resultReplace);
        UNIT_ASSERT(!bufferOut.Build().Empty());
    }
}

} // namespace NCloud::NBlockStore::NYdbStats

