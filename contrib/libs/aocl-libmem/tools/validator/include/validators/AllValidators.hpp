/* Copyright (C) 2026 Advanced Micro Devices, Inc. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its contributors
 *    may be used to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef LIBMEM_VALIDATOR_ALL_VALIDATORS_HPP
#define LIBMEM_VALIDATOR_ALL_VALIDATORS_HPP

/**
 * @file AllValidators.hpp
 * @brief Master include file for all validators
 * 
 * This file includes all validator implementations. Include this single file
 * to get access to all 16 function validators.
 * 
 * Structure:
 * - validators/memory/  - Memory function validators (memcpy, memmove, etc.)
 * - validators/string/  - String function validators (strcpy, strcmp, etc.)
 * 
 * Each validator is in its own file for easy navigation and modification.
 */

// ============================================================================
// Memory Function Validators
// ============================================================================
#include "validators/memory/MemcpyValidator.hpp"
#include "validators/memory/MempcpyValidator.hpp"
#include "validators/memory/MemmoveValidator.hpp"
#include "validators/memory/MemsetValidator.hpp"
#include "validators/memory/MemcmpValidator.hpp"
#include "validators/memory/MemchrValidator.hpp"

// ============================================================================
// String Function Validators
// ============================================================================
#include "validators/string/StrcpyValidator.hpp"
#include "validators/string/StrncpyValidator.hpp"
#include "validators/string/StrcatValidator.hpp"
#include "validators/string/StrncatValidator.hpp"
#include "validators/string/StrcmpValidator.hpp"
#include "validators/string/StrncmpValidator.hpp"
#include "validators/string/StrlenValidator.hpp"
#include "validators/string/StrnlenValidator.hpp"
#include "validators/string/StrchrValidator.hpp"
#include "validators/string/StrrchrValidator.hpp"
#include "validators/string/StrstrValidator.hpp"
#include "validators/string/StrspnValidator.hpp"

#endif // LIBMEM_VALIDATOR_ALL_VALIDATORS_HPP

