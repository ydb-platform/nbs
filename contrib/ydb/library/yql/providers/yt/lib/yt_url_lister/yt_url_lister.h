#pragma once

#include <contrib/ydb/library/yql/core/url_lister/interface/url_lister.h>


namespace NYql {

IUrlListerPtr MakeYtUrlLister();

}
