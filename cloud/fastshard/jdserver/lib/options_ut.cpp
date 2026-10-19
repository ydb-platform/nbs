#include "options.h"

#include <library/cpp/getopt/small/last_getopt.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

#include <gtest/gtest.h>

namespace NCloud::NFastShard::NJdServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

TOptions Parse(TVector<const char*> args)
{
    args.insert(args.begin(), "fastshard-jdserver");

    TOptions options;
    options.Parse(static_cast<int>(args.size()), args.data());
    return options;
}

////////////////////////////////////////////////////////////////////////////////

TEST(TJdServerOptionsTest, ShouldParseSizesWithSuffixes)
{
    const auto options = Parse(
        {"--port",
         "1234",
         "--device-path",
         "/dev/null",
         "--log-meta-size",
         "16M",
         "--log-data-size",
         "512M",
         "--data-size",
         "2G",
         "--block-size",
         "8K"});

    EXPECT_EQ(1234, options.Port);
    EXPECT_EQ("/dev/null", options.DevicePath);
    EXPECT_EQ(8_KB, options.BlockSize);
    EXPECT_EQ(16_MB, options.GetLogMetaSize());
    EXPECT_EQ(512_MB, options.GetLogDataSize());
    EXPECT_EQ(2_GB, options.DataSize);
    EXPECT_EQ(16_MB + 512_MB + 2_GB, options.GetDeviceSize());
}

TEST(TJdServerOptionsTest, ShouldSplitTheDeviceInThreeGigabytesByDefault)
{
    const auto options = Parse({"--port", "1234", "--device-path", "dev"});

    EXPECT_EQ(4_KB, options.BlockSize);
    EXPECT_EQ(1_GB, options.GetLogMetaSize());
    EXPECT_EQ(1_GB, options.GetLogDataSize());
    EXPECT_EQ(1_GB, options.DataSize);
    EXPECT_EQ(3_GB, options.GetDeviceSize());
}

TEST(TJdServerOptionsTest, ShouldDropTheJournalPartsWithoutAJournal)
{
    const auto options =
        Parse({"--port", "1234", "--device-path", "dev", "--no-journal"});

    EXPECT_EQ(0u, options.GetLogMetaSize());
    EXPECT_EQ(0u, options.GetLogDataSize());
    EXPECT_EQ(1_GB, options.GetDeviceSize());
}

TEST(TJdServerOptionsTest, ShouldRejectInvalidOptions)
{
    using NLastGetopt::TUsageException;

    EXPECT_THROW(Parse({"--device-path", "dev"}), TUsageException);
    EXPECT_THROW(Parse({"--port", "1234"}), TUsageException);
    EXPECT_THROW(
        Parse({"--port", "1234", "--device-path", "dev", "--data-size", "0"}),
        TUsageException);
    EXPECT_THROW(
        Parse(
            {"--port", "1234", "--device-path", "dev", "--data-size", "4097"}),
        TUsageException);
    EXPECT_THROW(
        Parse(
            {"--port",
             "1234",
             "--device-path",
             "dev",
             "--log-meta-size",
             "1K"}),
        TUsageException);
    EXPECT_THROW(
        Parse(
            {"--port", "1234", "--device-path", "dev", "--data-size", "1X"}),
        TUsageException);
}

}   // namespace

}   // namespace NCloud::NFastShard::NJdServer
