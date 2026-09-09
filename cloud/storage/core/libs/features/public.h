#pragma once

#include <memory>

namespace NCloud::NFeatures {

////////////////////////////////////////////////////////////////////////////////

class TFeaturesConfig;
using TFeaturesConfigPtr = std::shared_ptr<TFeaturesConfig>;
using TFeaturesConfigConstPtr = std::shared_ptr<const TFeaturesConfig>;

}   // namespace NCloud::NFeatures
