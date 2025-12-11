#include "label.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NMetrics {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLabelTest)
{
    Y_UNIT_TEST(ShouldGetCorrectNameAndValue)
    {
        const TLabel label = CreateLabel("test_name", "test_value");

        UNIT_ASSERT_VALUES_EQUAL("test_name", label.GetName());
        UNIT_ASSERT_VALUES_EQUAL("test_value", label.GetValue());
    }

    Y_UNIT_TEST(ShouldGetCorrectSensorValue)
    {
        const TLabel label = CreateSensor("test_value");

        UNIT_ASSERT_VALUES_EQUAL("sensor", label.GetName());
        UNIT_ASSERT_VALUES_EQUAL("test_value", label.GetValue());
    }

    Y_UNIT_TEST(ShouldGetCorrectHashes)
    {
        const TLabel label = CreateLabel("test_name", "test_value");
        const TLabel sameLabel = CreateLabel("test_name", "test_value");
        const TLabel sensor = CreateSensor("test_value");
        const TLabel sameSensor = CreateSensor("test_value");

        UNIT_ASSERT_VALUES_EQUAL(label.GetHash(), sameLabel.GetHash());
        UNIT_ASSERT_VALUES_UNEQUAL(sameLabel.GetHash(), sensor.GetHash());
        UNIT_ASSERT_VALUES_EQUAL(sensor.GetHash(), sameSensor.GetHash());
    }

    Y_UNIT_TEST(ShouldCorrectlyCheckEquality)
    {
        const TLabel label = CreateLabel("test_name", "test_value");
        const TLabel sameLabel = CreateLabel("test_name", "test_value");
        const TLabel wrongName = CreateLabel("test_wrong_name", "test_value");
        const TLabel wrongValue = CreateLabel("test_name", "test_wrong_value");
        const TLabel wrongLabel =
            CreateLabel("test_wrong_name", "test_wrong_value");

        UNIT_ASSERT(label == sameLabel);
        UNIT_ASSERT(label == sameLabel);
        UNIT_ASSERT(label == sameLabel);
        UNIT_ASSERT(label != wrongName);
        UNIT_ASSERT(label != wrongValue);
        UNIT_ASSERT(label != wrongLabel);

        const TLabel sensor = CreateSensor("test_sensor");
        const TLabel sameSensor = CreateSensor("test_sensor");
        const TLabel wrongSensor = CreateSensor("test_wrong_sensor");

        UNIT_ASSERT(sensor == sameSensor);
        UNIT_ASSERT(sensor != wrongSensor);
    }

    Y_UNIT_TEST(ShouldGetCorrectLabels)
    {
        TLabels labels{
            CreateLabel("test_name", "test_value"),
            CreateSensor("test_sensor")};

        UNIT_ASSERT_VALUES_EQUAL(2, labels.size());

        UNIT_ASSERT_VALUES_EQUAL("test_name", labels[0].GetName());
        UNIT_ASSERT_VALUES_EQUAL("test_value", labels[0].GetValue());

        UNIT_ASSERT_VALUES_EQUAL("sensor", labels[1].GetName());
        UNIT_ASSERT_VALUES_EQUAL("test_sensor", labels[1].GetValue());
    }

    Y_UNIT_TEST(ShouldCorrectlyCheckLabelsEquality)
    {
        {
            TLabels lhv{
                CreateLabel("test_name", "test_value"),
                CreateSensor("test_sensor")};

            TLabels rhv{
                CreateLabel("test_name", "test_value"),
                CreateSensor("test_sensor")};

            UNIT_ASSERT(lhv == rhv);
        }

        {
            TLabels lhv{
                CreateLabel("test_name", "test_value"),
                CreateSensor("test_sensor")};

            TLabels rhv{CreateLabel("test_name", "test_value")};

            UNIT_ASSERT(lhv != rhv);
        }

        {
            TLabels lhv{
                CreateLabel("test_name", "test_value"),
                CreateSensor("first_sensor")};

            TLabels rhv{
                CreateLabel("test_name", "test_value"),
                CreateSensor("second_sensor")};

            UNIT_ASSERT(lhv != rhv);
        }

        {
            TLabels lhv{
                CreateLabel("test_name", "test_value"),
                CreateSensor("test")};

            TLabels rhv{
                CreateSensor("test"),
                CreateLabel("test_name", "test_value")};

            UNIT_ASSERT(lhv != rhv);
        }
    }
}

}   // namespace NCloud::NFileStore::NMetrics
