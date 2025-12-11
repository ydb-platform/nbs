#pragma once

#include <library/cpp/monlib/service/pages/mon_page.h>

namespace NCloud::NStorage::NUserStats {

////////////////////////////////////////////////////////////////////////////////

class TMonPageWrapper: public NMonitoring::IMonPage
{
public:
    using TFunction = std::function<void(IOutputStream&)>;

    TMonPageWrapper(const TString& path, TFunction function)
        : NMonitoring::IMonPage(path)
        , Function(function)
    {}

    void Output(NMonitoring::IMonHttpRequest& request) override
    {
        return Function(request.Output());
    }

private:
    TFunction Function;
};

}   // namespace NCloud::NStorage::NUserStats
