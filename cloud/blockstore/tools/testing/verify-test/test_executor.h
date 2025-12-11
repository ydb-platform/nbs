#pragma once

#include "private.h"

#include <library/cpp/deprecated/atomic/atomic.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

enum class ETestExecutorType
{
    Read,
    Write
};

////////////////////////////////////////////////////////////////////////////////

enum class ETestPattern
{
    Direct,
    Reverse,
    Random,
    Max,
    CheckZero
};

////////////////////////////////////////////////////////////////////////////////

struct TTestExecutorConfig
{
    ui64 StartOffset;
    ui64 EndOffset;
    ui64 Step;
    ui32 BlockSize;
    ETestPattern TestPattern;
    bool DirectIo;

    TTestExecutorConfig(
        ui64 startOffset,
        ui64 endOffset,
        ui64 step,
        ui32 blockSize,
        ETestPattern testPattern,
        bool directIo)
        : StartOffset(startOffset)
        , EndOffset(endOffset)
        , Step(step)
        , BlockSize(blockSize)
        , TestPattern(testPattern)
        , DirectIo(directIo)
    {}

    bool operator<(const TTestExecutorConfig& other) const
    {
        return StartOffset < other.StartOffset;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestExecutorReport
{
    TInstant StartTime;
    TInstant FinishTime;
};

////////////////////////////////////////////////////////////////////////////////

struct ITestExecutor
{
    virtual ~ITestExecutor() = default;

    virtual TTestExecutorReport Run(
        TAtomic& waitingForStart,
        TAtomic& shouldStart) = 0;

    virtual void Stop() = 0;
};

////////////////////////////////////////////////////////////////////////////////

ITestExecutorPtr CreateTestExecutor(
    const ETestExecutorType& type,
    TString filePath,
    TTestExecutorConfigPtr testExecutorConfig);

}   // namespace NCloud::NBlockStore
