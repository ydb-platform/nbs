#pragma once

#include "spdk.h"

namespace NCloud::NBlockStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

// breaks iso c++ rules by reordering field designators
#undef SPDK_LOG_REGISTER_COMPONENT

#define SPDK_LOG_REGISTER_COMPONENT(FLAG) \
    struct spdk_log_flag SPDK_LOG_##FLAG; \
    TSpdkLogFlag register_flag_##FLAG(#FLAG, &SPDK_LOG_##FLAG); \

////////////////////////////////////////////////////////////////////////////////

struct TSpdkLogFlag
{
    TSpdkLogFlag(const char* name, spdk_log_flag* flag)
    {
        flag->name = name;
        flag->enabled = false;
        spdk_log_register_flag(name, flag);
    }
};

}   // NCloud::NBlockStore::NSpdk
