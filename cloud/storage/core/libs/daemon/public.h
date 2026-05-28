#pragma once

#include <util/generic/yexception.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

// When this exception is thrown, TApp handles it by calling _exit.
// Traditional exit handlers and destructors are not invoked, which allows the
// program to exit even when safe shutdown is impossible or very hard
class TAppShouldExitWithoutShutdownException
    : public yexception
{};

}   // namespace NCloud

////////////////////////////////////////////////////////////////////////////////

class TProgramShouldContinue;
