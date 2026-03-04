#pragma once

namespace NCloud::NBlockStore {

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

class TVolumePerformanceProfile;

}   // namespace NProto

////////////////////////////////////////////////////////////////////////////////

enum class EVolumeThrottlingOpType
{
    Read,
    Write,
    Zero,
    Describe,

    Last = Describe,
};

}   // namespace NCloud::NBlockStore
