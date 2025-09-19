/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

/** \file
 * Lookup table (LUT) implementation
 *
 * This is an implementation of a simple LUT - an array of that allows indexing, maps key (index) into a value (pointer)
 * and allows direct addressing of the value by key.
 *
 * The array is allocated dynamically using malloc. It's initial capacity is \p init_size elements and it's growable.
 * Once full, it's being extended by \p growth_step new elements - up to the limit of \p max_size elements.
 *
 * NOTE: the LUT is spinlock-protected, so it must be used cautiously, especially in the code meant to be lockless.
 */

#ifndef SPDK_LUT_H
#define SPDK_LUT_H

#include "spdk/stdinc.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SPDK_LUT_MAX_SIZE (1024 * 1024 * 1024)
#define SPDK_LUT_INVALID_KEY UINT64_MAX
#define SPDK_LUT_INVALID_VALUE ((void *)UINTPTR_MAX)

/**
 * \brief SPDK LUT object.
 *
 * This is a LUT object.
 *
 * SPDK Look Up Tables are not, in general, thread safe. For multi-threaded access, take a lock around each operation.
 * However, it is thread safe to call spdk_lut_get() and spdk_lut_for_each() concurrently with all other operations without
 * holding a lock.
 */
struct spdk_lut;

/**
 * Per-element callback to be called during the LUT enumeration
 *
 * \param cb_arg Context passed to the \p spdk_lut_foreach API
 * \param key Element key.
 * \param value Element's value.
 *
 * \return 0 to continue the enumeration, non-zero value otherwise
 */
typedef int (*spdk_lut_foreach_cb)(void *cb_arg, uint64_t key, void *value);

/**
 * Create LUT
 *
 * \param init_size Initial table size.
 * \param growth_step How the table will grow when there are no more free elements remained.
 * \param max_size Max table size.
 *
 * \return LUT object on success, NULL otherwise.
 */
struct spdk_lut *spdk_lut_create(uint64_t init_size, uint64_t growth_step, uint64_t max_size);

/**
 * \brief Context for restoring a LUT.
 *
 * This is a context for restoring a LUT.
 *
 * It is used for alternative way of creating a LUT - restoring it and inserting elements one by one.
 * This is useful when specific LUT keys must be assigned to specific values.
 *
 * \return Context object on success, NULL otherwise.
 */
struct spdk_lut_restore_ctx;

/**
 * \brief Begin restoring a LUT.
 *
 * \param init_size Initial table size.
 * \param growth_step How the table will grow when there are no more free elements remained.
 * \param max_size Max table size.
 *
 * \return Context object on success, NULL otherwise.
 */
struct spdk_lut_restore_ctx *spdk_lut_restore_begin(uint64_t init_size, uint64_t growth_step,
		uint64_t max_size);

/**
 * \brief Insert a value into the LUT at a specific key.
 *
 * \param ctx Context object.
 * \param value Value to insert.
 * \param key Key to insert the element at.
 *
 * \return 0 on success, a negative error code otherwise.
 */
int spdk_lut_restore_insert_at(struct spdk_lut_restore_ctx *ctx, void *value, uint64_t key);

/**
 * \brief End restoring a LUT.
 *
 * \param ctx Context object.
 * \param status Status of the restore operation. If non-zero, the function will free the context and return NULL.
 *
 * \return LUT object on success, NULL otherwise.
 */
struct spdk_lut *spdk_lut_restore_end(struct spdk_lut_restore_ctx *ctx, int status);

/**
 * Insert 'value' into the look up table and return the associated key.
 *
 * \param lut LUT object.
 * \param value Value to insert.
 *
 * \return Key of the inserted element on success, SPDK_LUT_INVALID_KEY otherwise.
 */
uint64_t spdk_lut_insert(struct spdk_lut *lut, void *value);

/**
 * Insert 'value' into the look up table and associate it with the key.
 *
 * This is not a high performance operation and it should be used only in code paths
 * that are not performance sensitive.
 *
 * \param lut LUT object.
 * \param value Value to insert.
 * \param key Key to insert the element at.
 *
 * \return 0 on success, a negative error code otherwise (an invalid key etc.).
 */
int spdk_lut_insert_at(struct spdk_lut *lut, void *value, uint64_t key);

/**
 * Get the value associated with key.
 *
 * \param lut LUT object.
 * \param key Element's key.
 *
 * \return Value on success, otherwise (for a non-valid element etc.) - SPDK_LUT_INVALID_VALUE.
 */
void *spdk_lut_get(struct spdk_lut *lut, uint64_t key);

/**
 * Call 'cb_fn' on each LUT element.
 *
 * \param lut LUT object.
 * \param cb_fn Called on each LUT element.
 * \param cb_arg Argument to be passed to the \p cb_fn
 *
 * \return 0 on success, a value returned by the \p cb_fn otherwise.
 */
int spdk_lut_foreach(struct spdk_lut *lut, spdk_lut_foreach_cb cb_fn, void *cb_arg);

/**
 * Remove the value associated with key.
 *
 * \param lut LUT object.
 * \param key Element's key.
 *
 * \return 0 on success, a negative error code otherwise (an invalid element etc.).
 */
int spdk_lut_remove(struct spdk_lut *lut, uint64_t key);

/**
 * Reset the LUT.
 *
 * \param lut LUT object.
 *
 * \return 0 on success, a negative error code otherwise.
 *
 * \note This function removes all currently inserted elements from the LUT.
 * \note This function is not thread safe. It should be called while no other threads are accessing the LUT.
 */
int spdk_lut_reset(struct spdk_lut *lut);

/**
 * Destroy LUT
 *
 * \param lut LUT object.
 */
void spdk_lut_free(struct spdk_lut *lut);

#ifdef __cplusplus
}
#endif

#endif /* SPDK_LUT_H */
