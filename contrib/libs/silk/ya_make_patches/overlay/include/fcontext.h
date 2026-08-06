// Shim: silk includes <fcontext.h> expecting a flat C API.
// We forward to the repo's boost fcontext and re-export the symbols.
#pragma once

#include <boost/context/detail/fcontext.hpp>

using boost::context::detail::fcontext_t;
using boost::context::detail::transfer_t;
using boost::context::detail::make_fcontext;
using boost::context::detail::jump_fcontext;
