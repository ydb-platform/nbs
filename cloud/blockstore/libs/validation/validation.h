#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service/service.h>

#include <util/generic/size_literals.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

const auto DefaultValidationRange = TBlockRange64::WithLength(0, 100_GB / 4_KB);

////////////////////////////////////////////////////////////////////////////////

struct IValidationCallback
{
    virtual ~IValidationCallback() = default;

    virtual void ReportError(const TString& message) = 0;
};

////////////////////////////////////////////////////////////////////////////////

constexpr TAtomicBase InvalidDigest = Max();

struct IBlockDigestCalculator
{
    virtual ~IBlockDigestCalculator() = default;

    virtual ui64 Calculate(ui64 blockIndex, const TStringBuf block) const = 0;
};

namespace NServer {

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateValidationService(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IBlockStorePtr service,
    IBlockDigestCalculatorPtr digestCalculator,
    TDuration inactivityTimeout = {},
    IValidationCallbackPtr callback = {});

}   // namespace NServer

namespace NClient {

////////////////////////////////////////////////////////////////////////////////

struct IBlockStoreValidationClient: public IBlockStore
{
    virtual void InitializeBlockChecksums(const TString& volumeName) = 0;
    virtual void SetBlockChecksums(
        const TString& volumeName,
        const TVector<std::pair<ui64, ui64>>& checksums) = 0;
};

IBlockStoreValidationClientPtr CreateValidationClient(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IBlockStorePtr client,
    IBlockDigestCalculatorPtr digestCalculator,
    IValidationCallbackPtr callback = {},
    TString loggingTag = {},
    const TBlockRange64& validationRange = DefaultValidationRange);

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateDataIntegrityClient(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IBlockStorePtr client,
    ui32 blockSize);

}   // namespace NClient

////////////////////////////////////////////////////////////////////////////////

IBlockDigestCalculatorPtr CreateCrcDigestCalculator();

TVector<ui64> CalculateBlocksDigest(
    const TSgList& blocks,
    const IBlockDigestCalculator& digestCalculator,
    ui32 blockSize,
    ui64 blockIndex,
    ui32 blockCount,
    ui64 zeroBlockDigest);

}   // namespace NCloud::NBlockStore
