#include "write_back_cache.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/random/random.h>
#include <util/system/tempfile.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

struct TCalculateDataPartsToReadTestBootstrap
{
    using TWriteDataEntry = TWriteBackCache::TWriteDataEntry;
    using TWriteDataEntryPart = TWriteBackCache::TWriteDataEntryPart;

    ILoggingServicePtr Logging;
    TLog Log;

    TCalculateDataPartsToReadTestBootstrap()
    {
        Logging = CreateLoggingService("console", TLogSettings{});
        Logging->Start();
        Log = Logging->CreateLog("WRITE_BACK_CACHE");
    }

    ~TCalculateDataPartsToReadTestBootstrap() = default;

    TVector<TWriteDataEntryPart> CalculateDataPartsToRead(
        const TVector<TWriteDataEntry*>& entries,
        ui64 startingFromOffset,
        ui64 length)
    {
        return TWriteBackCache::CalculateDataPartsToRead(
            entries,
            startingFromOffset,
            length);
    }
};

using TWriteDataEntry = TCalculateDataPartsToReadTestBootstrap::TWriteDataEntry;
using TWriteDataEntryPart =
    TCalculateDataPartsToReadTestBootstrap::TWriteDataEntryPart;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCalculateDataPartsToReadTest)
{
    Y_UNIT_TEST(ShouldCorrectlyCalculateDataPartsToRead)
    {
    }
}

}   // namespace NCloud::NFileStore::NFuse
