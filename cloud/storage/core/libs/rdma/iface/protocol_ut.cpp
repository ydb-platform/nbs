#include "protocol.h"

#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/string.h>

namespace NCloud::NStorage::NRdma {

namespace {

////////////////////////////////////////////////////////////////////////////////

enum {
    TEST_OPTION_VALUE = 1,
    TEST_OPTION_DATA = 2,
    TEST_OPTION_NEW_VALUE = 3,
};

struct TOptions
{
    ui16 Value = 0;
    TStringBuf Data;
    ui32 NewValue = 0;
};

bool ReadOptions(TStringBuf buffer, TOptions* options)
{
    TOptionReader reader(buffer.data(), buffer.size());
    return reader.ReadValue(TEST_OPTION_VALUE, &options->Value) &&
           reader.ReadData(TEST_OPTION_DATA, &options->Data) &&
           reader.ReadValue(TEST_OPTION_NEW_VALUE, &options->NewValue);
}

// knows nothing about TEST_OPTION_NEW_VALUE
bool ReadOldOptions(TStringBuf buffer, TOptions* options)
{
    TOptionReader reader(buffer.data(), buffer.size());
    return reader.ReadValue(TEST_OPTION_VALUE, &options->Value) &&
           reader.ReadData(TEST_OPTION_DATA, &options->Data);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(TOptionsTest, ShouldWriteAndReadOptions)
{
    char buffer[32];
    TOptionWriter writer(buffer, sizeof(buffer));
    ASSERT_EQ(0u, writer.Size());

    writer.WriteValue(TEST_OPTION_VALUE, ui16{0x0201});
    writer.WriteData(TEST_OPTION_DATA, "xyz");
    writer.WriteValue(TEST_OPTION_NEW_VALUE, ui32{0x08070605});

    // value, data with its size, new value
    const TString expected("\x01\x01\x02\x02\x03xyz\x03\x05\x06\x07\x08", 13);
    ASSERT_EQ(expected, TString(buffer, writer.Size()));

    TOptions options;
    ASSERT_TRUE(ReadOptions(expected, &options));
    EXPECT_EQ(0x0201, options.Value);
    EXPECT_EQ("xyz", options.Data);
    EXPECT_EQ(0x08070605u, options.NewValue);
}

TEST(TOptionsTest, ShouldLeaveAbsentOptionsIntact)
{
    char buffer[32];
    TOptionWriter writer(buffer, sizeof(buffer));
    writer.WriteData(TEST_OPTION_DATA, "xyz");

    TOptions options = {.Value = 42, .NewValue = 43};
    ASSERT_TRUE(ReadOptions(TStringBuf(buffer, writer.Size()), &options));
    EXPECT_EQ(42, options.Value);
    EXPECT_EQ("xyz", options.Data);
    EXPECT_EQ(43u, options.NewValue);

    options = {.Value = 42, .Data = "def", .NewValue = 43};
    ASSERT_TRUE(ReadOptions({}, &options));
    EXPECT_EQ(42, options.Value);
    EXPECT_EQ("def", options.Data);
    EXPECT_EQ(43u, options.NewValue);
}

TEST(TOptionsTest, ShouldWriteEmptyData)
{
    char buffer[32];
    TOptionWriter writer(buffer, sizeof(buffer));
    writer.WriteData(TEST_OPTION_DATA, {});
    ASSERT_EQ(2u, writer.Size());

    TOptions options = {.Data = "def"};
    ASSERT_TRUE(ReadOptions(TStringBuf(buffer, writer.Size()), &options));
    EXPECT_EQ("", options.Data);
}

TEST(TOptionsTest, ShouldIgnoreOptionsAfterUnknownOne)
{
    char buffer[32];
    TOptionWriter writer(buffer, sizeof(buffer));
    writer.WriteValue(TEST_OPTION_VALUE, ui16{1});
    writer.WriteValue(TEST_OPTION_NEW_VALUE, ui32{2});

    TOptions options;
    ASSERT_TRUE(ReadOldOptions(TStringBuf(buffer, writer.Size()), &options));
    EXPECT_EQ(1, options.Value);
    EXPECT_EQ("", options.Data);

    // a known flag after an unknown one may be a part of the unknown option
    const TString unknownFirst("\x7f\x01\x02\x03", 4);
    options = {};
    ASSERT_TRUE(ReadOptions(unknownFirst, &options));
    EXPECT_EQ(0, options.Value);
}

TEST(TOptionsTest, ShouldStopAtZeroPadding)
{
    char buffer[32] = {};
    TOptionWriter writer(buffer, sizeof(buffer));
    writer.WriteValue(TEST_OPTION_VALUE, ui16{1});

    TOptions options;
    ASSERT_TRUE(ReadOptions(TStringBuf(buffer, sizeof(buffer)), &options));
    EXPECT_EQ(1, options.Value);
    EXPECT_EQ("", options.Data);
    EXPECT_EQ(0u, options.NewValue);
}

TEST(TOptionsTest, ShouldNotReadOptionsCutShort)
{
    const TString testCases[] = {
        // value
        TString("\x01", 1),
        TString("\x01\x01", 2),
        TString("\x03\x01\x02\x03", 4),
        // data
        TString("\x02", 1),
        TString("\x02\x01", 2),
        TString("\x02\x04xyz", 5),
        // after a good option
        TString("\x01\x01\x02\x02\x04xyz", 8),
    };

    for (const auto& testCase: testCases) {
        TOptions options;
        EXPECT_FALSE(ReadOptions(testCase, &options)) << testCase.Quote();
    }
}

}   // namespace NCloud::NStorage::NRdma
