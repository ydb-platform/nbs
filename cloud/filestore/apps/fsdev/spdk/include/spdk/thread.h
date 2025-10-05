/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2016 Intel Corporation.
 *   All rights reserved.
 *   Copyright (c) 2022-2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

/** \file
 * Thread
 */

#ifndef SPDK_THREAD_H_
#define SPDK_THREAD_H_

#include "spdk/config.h"
#include "spdk/fd_group.h"
#include "spdk/stdinc.h"
#include "spdk/assert.h"
#include "spdk/cpuset.h"
#include "spdk/env.h"
#include "spdk/util.h"
#include "spdk/likely.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Pollers should always return a value of this type
 * indicating whether they did real work or not.
 */
enum spdk_thread_poller_rc {
	SPDK_POLLER_IDLE,
	SPDK_POLLER_BUSY,
};

/**
 * A stackless, lightweight thread.
 */
struct spdk_thread;

/**
 * A function repeatedly called on the same spdk_thread.
 */
struct spdk_poller;

struct spdk_io_channel_iter;
struct spdk_io_channel;

/**
 * A function that is called each time a new thread is created.
 * The implementer of this function should frequently call
 * spdk_thread_poll() on the thread provided.
 *
 * \param thread The new spdk_thread.
 */
typedef int (*spdk_new_thread_fn)(struct spdk_thread *thread);

/**
 * SPDK thread operation type.
 */
enum spdk_thread_op {
	/* Called each time a new thread is created. The implementer of this operation
	 * should frequently call spdk_thread_poll() on the thread provided.
	 */
	SPDK_THREAD_OP_NEW,

	/* Called when SPDK thread needs to be rescheduled. (e.g., when cpumask of the
	 * SPDK thread is updated.
	 */
	SPDK_THREAD_OP_RESCHED,
};

/**
 * Function to be called for SPDK thread operation.
 */
typedef int (*spdk_thread_op_fn)(struct spdk_thread *thread, enum spdk_thread_op op);

/**
 * Function to check whether the SPDK thread operation is supported.
 */
typedef bool (*spdk_thread_op_supported_fn)(enum spdk_thread_op op);

/**
 * A function that will be called on the target thread.
 *
 * \param ctx Context passed as arg to spdk_thread_pass_msg().
 */
typedef void (*spdk_msg_fn)(void *ctx);

/**
 * Function to be called to pass a message to a thread.
 *
 * \param fn Callback function for a thread.
 * \param ctx Context passed to fn.
 * \param thread_ctx Context for the thread.
 */
typedef void (*spdk_thread_pass_msg)(spdk_msg_fn fn, void *ctx,
				     void *thread_ctx);

/**
 * Callback function for a poller.
 *
 * \param ctx Context passed as arg to spdk_poller_register().
 * \return value of type `enum spdk_thread_poller_rc` (ex: SPDK_POLLER_IDLE
 * if no work was done or SPDK_POLLER_BUSY if work was done.)
 */
typedef int (*spdk_poller_fn)(void *ctx);

/**
 * Callback function to set poller into interrupt mode or back to poll mode.
 *
 * \param poller Poller to set interrupt or poll mode.
 * \param cb_arg Argument passed to the callback function.
 * \param interrupt_mode Set interrupt mode for true, or poll mode for false
 */
typedef void (*spdk_poller_set_interrupt_mode_cb)(struct spdk_poller *poller, void *cb_arg,
		bool interrupt_mode);

/**
 * Mark that the poller is capable of entering interrupt mode.
 *
 * When registering the poller set interrupt callback, the callback will get
 * executed immediately if its spdk_thread is in the interrupt mode.
 *
 * The callback is also called when the poller is being paused/resumed.
 *
 * Callers may pass NULL for the cb_fn, signifying that no callback is
 * necessary when the interrupt mode changes.
 *
 * \param poller The poller to register callback function.
 * \param cb_fn Callback function called when the poller must transition into or out of interrupt mode
 * \param cb_arg Argument passed to the callback function.
 */
void spdk_poller_register_interrupt(struct spdk_poller *poller,
				    spdk_poller_set_interrupt_mode_cb cb_fn,
				    void *cb_arg);

/**
 * I/O channel creation callback.
 *
 * \param io_device I/O device associated with this channel.
 * \param ctx_buf Context for the I/O device.
 */
typedef int (*spdk_io_channel_create_cb)(void *io_device, void *ctx_buf);

/**
 * I/O channel destruction callback.
 *
 * \param io_device I/O device associated with this channel.
 * \param ctx_buf Context for the I/O device.
 */
typedef void (*spdk_io_channel_destroy_cb)(void *io_device, void *ctx_buf);

/**
 * I/O device unregister callback.
 *
 * \param io_device Unregistered I/O device.
 */
typedef void (*spdk_io_device_unregister_cb)(void *io_device);

/**
 * Called on the appropriate thread for each channel associated with io_device.
 *
 * \param i I/O channel iterator.
 */
typedef void (*spdk_channel_msg)(struct spdk_io_channel_iter *i);

/**
 * spdk_for_each_channel() callback.
 *
 * \param i I/O channel iterator.
 * \param status 0 if it completed successfully, or negative errno if it failed.
 */
typedef void (*spdk_channel_for_each_cpl)(struct spdk_io_channel_iter *i, int status);

/**
 * Called on the specified channel associated with io_device.
 *
 * \param ch I/O channel
 * \param ctx a pointer to the context bbuffer.
 */
typedef void (*spdk_channel_msg_fn)(struct spdk_io_channel *ch, void *ctx);

#define SPDK_IO_CHANNEL_STRUCT_SIZE	96

/**
 * Message memory pool size definitions
 */
#define SPDK_MSG_MEMPOOL_CACHE_SIZE	1024
/* Power of 2 minus 1 is optimal for memory consumption */
#define SPDK_DEFAULT_MSG_MEMPOOL_SIZE (262144 - 1)

/**
 * Initialize the threading library. Must be called once prior to allocating any threads.
 *
 * \param new_thread_fn Called each time a new SPDK thread is created. The implementer
 * is expected to frequently call spdk_thread_poll() on the provided thread.
 * \param ctx_sz For each thread allocated, an additional region of memory of
 * size ctx_size will also be allocated, for use by the thread scheduler. A pointer
 * to this region may be obtained by calling spdk_thread_get_ctx().
 *
 * \return 0 on success. Negated errno on failure.
 */
int spdk_thread_lib_init(spdk_new_thread_fn new_thread_fn, size_t ctx_sz);

/**
 * Initialize the threading library. Must be called once prior to allocating any threads
 *
 * Both thread_op_fn and thread_op_type_supported_fn have to be specified or not
 * specified together.
 *
 * \param thread_op_fn Called for SPDK thread operation.
 * \param thread_op_supported_fn Called to check whether the SPDK thread operation is supported.
 * \param ctx_sz For each thread allocated, for use by the thread scheduler. A pointer
 * to this region may be obtained by calling spdk_thread_get_ctx().
 * \param msg_mempool_size Size of the allocated spdk_msg_mempool.
 *
 * \return 0 on success. Negated errno on failure.
 */
int spdk_thread_lib_init_ext(spdk_thread_op_fn thread_op_fn,
			     spdk_thread_op_supported_fn thread_op_supported_fn,
			     size_t ctx_sz, size_t msg_mempool_size);

/**
 * Release all resources associated with this library.
 */
void spdk_thread_lib_fini(void);

/**
 * Creates a new SPDK thread object.
 *
 * Note that the first thread created via spdk_thread_create() will be designated as
 * the app thread.  Other SPDK libraries may place restrictions on certain APIs to
 * only be called in the context of this app thread.
 *
 * \param name Human-readable name for the thread; can be retrieved with spdk_thread_get_name().
 * The string is copied, so the pointed-to data only needs to be valid during the
 * spdk_thread_create() call. May be NULL to specify no name.
 * \param cpumask Optional mask of CPU cores on which to schedule this thread. This is only
 * a suggestion to the scheduler. The value is copied, so cpumask may be released when
 * this function returns. May be NULL if no mask is required.
 *
 * \return a pointer to the allocated thread on success or NULL on failure..
 */
struct spdk_thread *spdk_thread_create(const char *name, const struct spdk_cpuset *cpumask);

/**
 * Return the app thread.
 *
 * The app thread is the first thread created using spdk_thread_create().
 *
 * \return a pointer to the app thread, or NULL if no thread has been created yet.
 */
struct spdk_thread *spdk_thread_get_app_thread(void);

/**
 * Check if the specified spdk_thread is the app thread.
 *
 * \param thread The thread to check. If NULL, check the current spdk_thread.
 * \return true if the specified spdk_thread is the app thread, false otherwise.
 */
bool spdk_thread_is_app_thread(struct spdk_thread *thread);

/**
 * Get the next registered SPDK thread.
 *
 * \param prev The current SPDK thread. This can be NULL to get the first SPDK thread.
 * \return The next registered SPDK thread.
 */
struct spdk_thread *spdk_thread_get_next_thread(struct spdk_thread *prev);

/**
 * Force the current system thread to act as if executing the given SPDK thread.
 *
 * \param thread The thread to set.
 */
void spdk_set_thread(struct spdk_thread *thread);

/**
 * Bind or unbind spdk_thread to its current CPU core.
 *
 * If spdk_thread is bound, it couldn't be rescheduled to other CPU cores until it is unbound.
 *
 * \param thread The thread to bind or not.
 * \param bind true for bind, false for unbind.
 */
void spdk_thread_bind(struct spdk_thread *thread, bool bind);

/**
 * Returns whether the thread is bound to its current CPU core.
 *
 * \param thread The thread to query.
 *
 * \return true if bound, false otherwise
 */
bool spdk_thread_is_bound(struct spdk_thread *thread);

/**
 * Mark the thread as exited, failing all future spdk_thread_send_msg(),
 * spdk_poller_register(), and spdk_get_io_channel() calls. May only be called
 * within an spdk poller or message.
 *
 * All I/O channel references associated with the thread must be released
 * using spdk_put_io_channel(), and all active pollers associated with the thread
 * should be unregistered using spdk_poller_unregister(), prior to calling
 * this function. This function will complete these processing. The completion can
 * be queried by spdk_thread_is_exited().
 *
 * Note that this function must not be called on the app thread until after it
 * has been called for all other threads.
 *
 * \param thread The thread to exit.
 *
 * \return always 0. (return value was deprecated but keep it for ABI compatibility.)
 */
int spdk_thread_exit(struct spdk_thread *thread);

/**
 * Returns whether the thread is marked as exited.
 *
 * A thread is exited only after it has spdk_thread_exit() called on it, and
 * it has been polled until any outstanding operations targeting this
 * thread have completed.  This may include poller unregistrations, io channel
 * unregistrations, or outstanding spdk_thread_send_msg calls.
 *
 * \param thread The thread to query.
 *
 * \return true if marked as exited, false otherwise.
 */
bool spdk_thread_is_exited(struct spdk_thread *thread);

/**
 * Returns whether the thread is still running.
 *
 * A thread is considered running until it has * spdk_thread_exit() called on it.
 *
 * \param thread The thread to query.
 *
 * \return true if still running, false otherwise.
 */
bool spdk_thread_is_running(struct spdk_thread *thread);

/**
 * Destroy a thread, releasing all of its resources. May only be called
 * on a thread previously marked as exited.
 *
 * \param thread The thread to destroy.
 *
 */
void spdk_thread_destroy(struct spdk_thread *thread);

/**
 * Return a pointer to this thread's context.
 *
 * \param thread The thread on which to get the context.
 *
 * \return a pointer to the per-thread context, or NULL if there is
 * no per-thread context.
 */
void *spdk_thread_get_ctx(struct spdk_thread *thread);

/**
 * Get the thread's cpumask.
 *
 * \param thread The thread to get the cpumask for.
 *
 * \return cpuset pointer
 */
struct spdk_cpuset *spdk_thread_get_cpumask(struct spdk_thread *thread);

/**
 * Set the current thread's cpumask to the specified value. The thread may be
 * rescheduled to one of the CPUs specified in the cpumask.
 *
 * This API requires SPDK thread operation supports SPDK_THREAD_OP_RESCHED.
 *
 * \param cpumask The new cpumask for the thread.
 *
 * \return 0 on success, negated errno otherwise.
 */
int spdk_thread_set_cpumask(struct spdk_cpuset *cpumask);

/**
 * Return the thread object associated with the context handle previously
 * obtained by calling spdk_thread_get_ctx().
 *
 * \param ctx A context previously obtained by calling spdk_thread_get_ctx()
 *
 * \return The associated thread.
 */
struct spdk_thread *spdk_thread_get_from_ctx(void *ctx);

/**
 * Perform one iteration worth of processing on the thread. This includes
 * both expired and continuous pollers as well as messages. If the thread
 * has exited, return immediately.
 *
 * \param thread The thread to process
 * \param max_msgs The maximum number of messages that will be processed.
 *                 Use 0 to process the default number of messages (8).
 * \param now The current time, in ticks. Optional. If 0 is passed, this
 *            function will call spdk_get_ticks() to get the current time.
 *            The current time is used as start time and this function
 *            will call spdk_get_ticks() at its end to know end time to
 *            measure run time of this function.
 *
 * \return 1 if work was done. 0 if no work was done.
 */
int spdk_thread_poll(struct spdk_thread *thread, uint32_t max_msgs, uint64_t now);

/**
 * Return the number of ticks until the next timed poller
 * would expire. Timed pollers are pollers for which
 * period_microseconds is greater than 0.
 *
 * \param thread The thread to check poller expiration times on
 *
 * \return Number of ticks. If no timed pollers, return 0.
 */
uint64_t spdk_thread_next_poller_expiration(struct spdk_thread *thread);

/**
 * Returns whether there are any active pollers (pollers for which
 * period_microseconds equals 0) registered to be run on the thread.
 *
 * \param thread The thread to check.
 *
 * \return 1 if there is at least one active poller, 0 otherwise.
 */
int spdk_thread_has_active_pollers(struct spdk_thread *thread);

/**
 * Returns whether there are any pollers registered to be run
 * on the thread.
 *
 * \param thread The thread to check.
 *
 * \return true if there is any active poller, false otherwise.
 */
bool spdk_thread_has_pollers(struct spdk_thread *thread);

/**
 * Returns whether there are scheduled operations to be run on the thread.
 *
 * \param thread The thread to check.
 *
 * \return true if there are no scheduled operations, false otherwise.
 */
bool spdk_thread_is_idle(struct spdk_thread *thread);

/**
 * Get count of allocated threads.
 */
uint32_t spdk_thread_get_count(void);

/**
 * Get a handle to the current thread.
 *
 * This handle may be passed to other threads and used as the target of
 * spdk_thread_send_msg().
 *
 * \sa spdk_io_channel_get_thread()
 *
 * \return a pointer to the current thread on success or NULL on failure.
 */
struct spdk_thread *spdk_get_thread(void);

/**
 * Get a thread's name.
 *
 * \param thread Thread to query.
 *
 * \return the name of the thread.
 */
const char *spdk_thread_get_name(const struct spdk_thread *thread);

/**
 * Get a thread's ID.
 *
 * \param thread Thread to query.
 *
 * \return the ID of the thread..
 */
uint64_t spdk_thread_get_id(const struct spdk_thread *thread);

/**
 * Get the thread by the ID.
 *
 * \param id ID of the thread.
 * \return Thread whose ID matches or NULL otherwise.
 */
struct spdk_thread *spdk_thread_get_by_id(uint64_t id);

struct spdk_thread_stats {
	uint64_t busy_tsc;
	uint64_t idle_tsc;
};

/**
 * Get statistics about the current thread.
 *
 * Copy cumulative thread stats values to the provided thread stats structure.
 *
 * \param stats User's thread_stats structure.
 */
int spdk_thread_get_stats(struct spdk_thread_stats *stats);

/**
 * Return the TSC value from the end of the last time this thread was polled.
 *
 * \param thread Thread to query.  If NULL, use current thread.
 *
 * \return TSC value from the end of the last time this thread was polled.
 */
uint64_t spdk_thread_get_last_tsc(struct spdk_thread *thread);

/**
 * Send a message to the given thread.
 *
 * The message will be sent asynchronously - i.e. spdk_thread_send_msg will always return
 * prior to `fn` being called.
 *
 * \param thread The target thread.
 * \param fn This function will be called on the given thread.
 * \param ctx This context will be passed to fn when called.
 *
 * \return 0 on success
 * \return -ENOMEM if the message could not be allocated
 * \return -EIO if the message could not be sent to the destination thread
 */
int spdk_thread_send_msg(const struct spdk_thread *thread, spdk_msg_fn fn, void *ctx);

/**
 * Send a message to the given thread. Only one critical message can be outstanding at the same
 * time. It's intended to use this function in any cases that might interrupt the execution of the
 * application, such as signal handlers.
 *
 * The message will be sent asynchronously - i.e. spdk_thread_send_critical_msg will always return
 * prior to `fn` being called.
 *
 * \param thread The target thread.
 * \param fn This function will be called on the given thread.
 *
 * \return 0 on success
 * \return -EIO if the message could not be sent to the destination thread, due to an already
 * outstanding critical message
 */
int spdk_thread_send_critical_msg(struct spdk_thread *thread, spdk_msg_fn fn);

/**
 * Run the msg callback on the given thread. If this happens to be the current
 * thread, the callback is executed immediately; otherwise a message is sent to
 * the thread, and it's run asynchronously.
 *
 * \param thread The target thread.
 * \param fn This function will be called on the given thread.
 * \param ctx This context will be passed to fn when called.
 *
 * \return 0 on success
 * \return -ENOMEM if the message could not be allocated
 * \return -EIO if the message could not be sent to the destination thread
 */
static inline int
spdk_thread_exec_msg(const struct spdk_thread *thread, spdk_msg_fn fn, void *ctx)
{
	assert(thread != NULL);

	if (spdk_get_thread() == thread) {
		fn(ctx);
		return 0;
	}

	return spdk_thread_send_msg(thread, fn, ctx);
}

/**
 * Send a message to each thread, serially.
 *
 * The message is sent asynchronously - i.e. spdk_for_each_thread will return
 * prior to `fn` being called on each thread.
 *
 * \param fn This is the function that will be called on each thread.
 * \param ctx This context will be passed to fn when called.
 * \param cpl This will be called on the originating thread after `fn` has been
 * called on each thread.
 */
void spdk_for_each_thread(spdk_msg_fn fn, void *ctx, spdk_msg_fn cpl);

/**
 * Set current spdk_thread into interrupt mode or back to poll mode.
 *
 * Only valid when thread interrupt facility is enabled by
 * spdk_interrupt_mode_enable().
 *
 * \param enable_interrupt Set interrupt mode for true, or poll mode for false
 */
void spdk_thread_set_interrupt_mode(bool enable_interrupt);

/**
 * Get trace id.
 *
 * \param thread Thread to get trace_id from.
 *
 * \return Trace id of the specified thread.
 */
uint16_t spdk_thread_get_trace_id(struct spdk_thread *thread);

/**
 * Register a poller on the current thread.
 *
 * The poller can be unregistered by calling spdk_poller_unregister().
 *
 * \param fn This function will be called every `period_microseconds`.
 * \param arg Argument passed to fn.
 * \param period_microseconds How often to call `fn`. If 0, call `fn` as often
 *  as possible.
 *
 * \return a pointer to the poller registered on the current thread on success
 * or NULL on failure.
 */
struct spdk_poller *spdk_poller_register(spdk_poller_fn fn,
		void *arg,
		uint64_t period_microseconds);

/**
 * Register a poller on the current thread with arbitrary name.
 *
 * The poller can be unregistered by calling spdk_poller_unregister().
 *
 * \param fn This function will be called every `period_microseconds`.
 * \param arg Argument passed to fn.
 * \param period_microseconds How often to call `fn`. If 0, call `fn` as often
 *  as possible.
 * \param name Human readable name for the poller. Pointer of the poller function
 * name is set if NULL.
 *
 * \return a pointer to the poller registered on the current thread on success
 * or NULL on failure.
 */
struct spdk_poller *spdk_poller_register_named(spdk_poller_fn fn,
		void *arg,
		uint64_t period_microseconds,
		const char *name);

/*
 * \brief Register a poller on the current thread with setting its name
 * to the string of the poller function name. The poller being registered
 * should return a value of type `enum spdk_thread_poller_rc`. See
 * \ref spdk_poller_fn for more information.
 */
#define SPDK_POLLER_REGISTER(fn, arg, period_microseconds)	\
	spdk_poller_register_named(fn, arg, period_microseconds, #fn)

/**
 * Unregister a poller on the current thread.
 *
 * This function will also write NULL to the spdk_poller pointer pointed
 * to by ppoller, to help encourage a poller pointer not getting reused
 * after it has been unregistered.
 *
 * It is OK to pass a ppoller parameter that points to NULL, in this case
 * the function is a nop.
 *
 * \param ppoller The poller to unregister.
 */
void spdk_poller_unregister(struct spdk_poller **ppoller);

/**
 * Pause a poller on the current thread.
 *
 * The poller is not run until it is resumed with spdk_poller_resume().  It is
 * perfectly fine to pause an already paused poller. When interrupt mode is enabled,
 * a paused poller is moved to the non-interrupt mode; in that case the user receives
 * a callback if one was set with \ref spdk_poller_register_interrupt
 *
 *
 * \param poller The poller to pause.
 */
void spdk_poller_pause(struct spdk_poller *poller);

/**
 * Resume a poller on the current thread.
 *
 * Resumes a poller paused with spdk_poller_pause().  It is perfectly fine to
 * resume an unpaused poller. When interrupt mode is enabled, a resumed poller
 * is moved to the interrupt mode; in that case the user receives
  * a callback if one was set with \ref spdk_poller_register_interrupt
 *
 * \param poller The poller to resume.
 */
void spdk_poller_resume(struct spdk_poller *poller);

/**
 * Register the opaque io_device context as an I/O device.
 *
 * After an I/O device is registered, it can return I/O channels using the
 * spdk_get_io_channel() function.
 *
 * \param io_device The pointer to io_device context.
 * \param create_cb Callback function invoked to allocate any resources required
 * for a new I/O channel.
 * \param destroy_cb Callback function invoked to release the resources for an
 * I/O channel.
 * \param ctx_size The size of the context buffer allocated to store references
 * to allocated I/O channel resources.
 * \param name A string name for the device used only for debugging. Optional -
 * may be NULL.
 */
void spdk_io_device_register(void *io_device, spdk_io_channel_create_cb create_cb,
			     spdk_io_channel_destroy_cb destroy_cb, uint32_t ctx_size,
			     const char *name);

/**
 * Unregister the opaque io_device context as an I/O device.
 *
 * The actual unregistration might be deferred until all active I/O channels are
 * destroyed.
 *
 * \param io_device The pointer to io_device context.
 * \param unregister_cb An optional callback function invoked to release any
 * references to this I/O device.
 */
void spdk_io_device_unregister(void *io_device, spdk_io_device_unregister_cb unregister_cb);

/**
 * Get an I/O channel for the specified io_device to be used by the calling thread.
 *
 * The io_device context pointer specified must have previously been registered
 * using spdk_io_device_register(). If an existing I/O channel does not exist
 * yet for the given io_device on the calling thread, it will allocate an I/O
 * channel and invoke the create_cb function pointer specified in spdk_io_device_register().
 * If an I/O channel already exists for the given io_device on the calling thread,
 * its reference is returned rather than creating a new I/O channel.
 *
 * \param io_device The pointer to io_device context.
 *
 * \return a pointer to the I/O channel for this device on success or NULL on failure.
 */
struct spdk_io_channel *spdk_get_io_channel(void *io_device);

/**
 * Release a reference to an I/O channel. This happens asynchronously.
 *
 * This must be called on the same thread that called spdk_get_io_channel()
 * for the specified I/O channel. If this releases the last reference to the
 * I/O channel, The destroy_cb function specified in spdk_io_device_register()
 * will be invoked to release any associated resources.
 *
 * \param ch I/O channel to release a reference.
 */
void spdk_put_io_channel(struct spdk_io_channel *ch);

/**
 * Get the context buffer associated with an I/O channel.
 *
 * \param ch I/O channel.
 *
 * \return a pointer to the context buffer.
 */
static inline void *
spdk_io_channel_get_ctx(struct spdk_io_channel *ch)
{
	if (spdk_unlikely(!ch)) {
		assert(false);
		return NULL;
	}

	return (uint8_t *)ch + SPDK_IO_CHANNEL_STRUCT_SIZE;
}

/**
 * Get I/O channel from the context buffer. This is the inverse of
 * spdk_io_channel_get_ctx().
 *
 * \param ctx The pointer to the context buffer.
 *
 * \return a pointer to the I/O channel associated with the context buffer.
 */
struct spdk_io_channel *spdk_io_channel_from_ctx(void *ctx);

/**
 * Get the thread associated with an I/O channel.
 *
 * \param ch I/O channel.
 *
 * \return a pointer to the thread associated with the I/O channel
 */
struct spdk_thread *spdk_io_channel_get_thread(struct spdk_io_channel *ch);

/**
 * Call 'fn' on each channel associated with io_device.
 *
 * This happens asynchronously, so fn may be called after spdk_for_each_channel
 * returns. 'fn' will be called for each channel serially, such that two calls
 * to 'fn' will not overlap in time. After 'fn' has been called, call
 * spdk_for_each_channel_continue() to continue iterating.
 *
 * \param io_device 'fn' will be called on each channel associated with this io_device.
 * \param fn Called on the appropriate thread for each channel associated with io_device.
 * \param ctx Context buffer registered to spdk_io_channel_iter that can be obtained
 * form the function spdk_io_channel_iter_get_ctx().
 * \param cpl Called on the thread that spdk_for_each_channel was initially called
 * from when 'fn' has been called on each channel.
 */
void spdk_for_each_channel(void *io_device, spdk_channel_msg fn, void *ctx,
			   spdk_channel_for_each_cpl cpl);

/**
 * Get io_device from the I/O channel iterator.
 *
 * \param i I/O channel iterator.
 *
 * \return a pointer to the io_device.
 */
void *spdk_io_channel_iter_get_io_device(struct spdk_io_channel_iter *i);

/**
 * Get I/O channel from the I/O channel iterator.
 *
 * \param i I/O channel iterator.
 *
 * \return a pointer to the I/O channel.
 */
struct spdk_io_channel *spdk_io_channel_iter_get_channel(struct spdk_io_channel_iter *i);

/**
 * Get context buffer from the I/O channel iterator.
 *
 * \param i I/O channel iterator.
 *
 * \return a pointer to the context buffer.
 */
void *spdk_io_channel_iter_get_ctx(struct spdk_io_channel_iter *i);

/**
 * Call 'fn' on the channel associated with io_device.
 *
 * If channel was deleted before 'fn' had a chance to execute, calling 'fn'
 * is silently skipped on the thread.
 *
 * \param thread The target thread.
 * \param io_device 'fn' is called on the channel associated with this io_device.
 * \param fn Called on the channel associated with io_device.
 * \param ctx Context buffer passed to 'fn'.
 */
void spdk_io_channel_send_msg(struct spdk_thread *thread, void *io_device,
			      spdk_channel_msg_fn fn, void *ctx);

/**
 * Call 'fn' on all channels associated with io_device by broadcast manner.
 *
 * \param io_device 'fn' will be called on all channels associated with this io_device.
 * \param fn Called on the appropriate thread for all channels associated with io_device.
 * \param ctx Context buffer passed to 'fn'.
 */
void spdk_for_each_channel_broadcast(void *io_device, spdk_channel_msg_fn fn, void *ctx);

/**
 * Get the io_device for the specified I/O channel.
 *
 * \param ch I/O channel.
 *
 * \return a pointer to the io_device for the I/O channel
 */
void *spdk_io_channel_get_io_device(struct spdk_io_channel *ch);

/**
 * Helper function to iterate all channels for spdk_for_each_channel().
 *
 * \param i I/O channel iterator.
 * \param status Status for the I/O channel iterator;
 * for non 0 status remaining iterations are terminated.
 */
void spdk_for_each_channel_continue(struct spdk_io_channel_iter *i, int status);

/**
 * A representative for registered interrupt file descriptor.
 */
struct spdk_interrupt;

/**
 * Callback function registered for interrupt file descriptor.
 *
 * \param ctx Context passed as arg to spdk_interrupt_register().
 *
 * \return 0 to indicate that interrupt took place but no events were found;
 * positive to indicate that interrupt took place and some events were processed;
 * negative if no event information is provided.
 */
typedef int (*spdk_interrupt_fn)(void *ctx);

/**
 * Register an spdk_interrupt on the current thread.
 *
 * The provided function will be called any time a SPDK_INTERRUPT_EVENT_IN event
 * triggers on the associated file descriptor.
 *
 * \param efd File descriptor of the spdk_interrupt.
 * \param fn Called each time there are events in spdk_interrupt.
 * \param arg Function argument for fn.
 * \param name Human readable name for the spdk_interrupt. Pointer of the spdk_interrupt
 * name is set if NULL.
 *
 * \return a pointer to the spdk_interrupt registered on the current thread on success
 * or NULL on failure.
 */
struct spdk_interrupt *spdk_interrupt_register(int efd, spdk_interrupt_fn fn,
		void *arg, const char *name);

/**
 * Register an spdk_interrupt with specific event types on the current thread.
 *
 * The provided function will be called any time one of specified event types triggers on
 * the associated file descriptor.
 * Event types argument is a bit mask composed by ORing together
 * enum spdk_interrupt_event_types values.
 *
 * \param efd File descriptor of the spdk_interrupt.
 * \param events Event notification types.
 * \param fn Called each time there are events in spdk_interrupt.
 * \param arg Function argument for fn.
 * \param name Human readable name for the spdk_interrupt. Pointer of the spdk_interrupt
 * name is set if NULL.
 *
 * \return a pointer to the spdk_interrupt registered on the current thread on success
 * or NULL on failure.
 */
struct spdk_interrupt *spdk_interrupt_register_for_events(int efd, uint32_t events,
		spdk_interrupt_fn fn, void *arg, const char *name);

/**
 * Register an spdk_interrupt with specific event type stated in spdk_event_handler_opts argument
 * on the current thread.
 *
 * The provided function will be called any time one of specified event types from
 * spdk_event_handler_opts argument triggers on the associated file descriptor.
 * Event types argument in spdk_event_handler_opts is a bit mask composed by ORing together
 * enum spdk_interrupt_event_types values.
 *
 * \param efd File descriptor of the spdk_interrupt.
 * \param fn Called each time there are events in spdk_interrupt.
 * \param arg Function argument for fn.
 * \param name Human readable name for the spdk_interrupt. Pointer of the spdk_interrupt
 * name is set if NULL.
 * \param opts Extended event handler option.
 *
 * \return a pointer to the spdk_interrupt registered on the current thread on success
 * or NULL on failure.
 */
struct spdk_interrupt *spdk_interrupt_register_ext(int efd, spdk_interrupt_fn fn, void *arg,
		const char *name, struct spdk_event_handler_opts *opts);

/*
 * \brief Register an spdk_interrupt on the current thread with setting its name
 * to the string of the spdk_interrupt function name.
 */
#define SPDK_INTERRUPT_REGISTER(efd, fn, arg)	\
	spdk_interrupt_register(efd, fn, arg, #fn)

/*
 * \brief Register an spdk_interrupt on the current thread with specific event types
 * and with setting its name to the string of the spdk_interrupt function name.
 */
#define SPDK_INTERRUPT_REGISTER_FOR_EVENTS(efd, events, fn, arg)	\
	spdk_interrupt_register_for_events(efd, events, fn, arg, #fn)

/*
 * \brief Register an spdk_interrupt on the current thread with specific event types provided
 * in opts and with setting its name to the string of the spdk_interrupt function name.
 */
#define SPDK_INTERRUPT_REGISTER_EXT(efd, fn, arg, opts)	\
	spdk_interrupt_register_ext(efd, fn, arg, #fn, opts)

/**
 * Register an interrupt listening for all events associated with an fd_group on current thread.
 *
 * \param fgrp fd_group describing the events to listen for.
 * \param name Name of the interrupt.
 *
 * return Pointer to spdk_interrupt or NULL in case of failure.
 */
struct spdk_interrupt *spdk_interrupt_register_fd_group(struct spdk_fd_group *fgrp,
		const char *name);

/**
 * Unregister an spdk_interrupt on the current thread.
 *
 * \param pintr The spdk_interrupt to unregister.
 */
void spdk_interrupt_unregister(struct spdk_interrupt **pintr);

enum spdk_interrupt_event_types {
#ifdef __linux__
	SPDK_INTERRUPT_EVENT_IN = EPOLLIN,
	SPDK_INTERRUPT_EVENT_OUT = EPOLLOUT,
	SPDK_INTERRUPT_EVENT_ET = EPOLLET
#else
	SPDK_INTERRUPT_EVENT_IN =  0x001,
	SPDK_INTERRUPT_EVENT_OUT = 0x004,
	SPDK_INTERRUPT_EVENT_ET = 1u << 31
#endif
};

/**
 * Change the event_types associated with the spdk_interrupt on the current thread.
 *
 * \param intr The pointer to the spdk_interrupt registered on the current thread.
 * \param event_types New event_types for the spdk_interrupt.
 *
 * \return 0 if success or -errno if failed.
 */
int spdk_interrupt_set_event_types(struct spdk_interrupt *intr,
				   enum spdk_interrupt_event_types event_types);

/**
 * Return a file descriptor that becomes ready whenever any of the registered
 * interrupt file descriptors are ready
 *
 * \param thread The thread to get.
 *
 * \return The spdk_interrupt fd of thread itself.
 */
int spdk_thread_get_interrupt_fd(struct spdk_thread *thread);

/**
 * Return an fd_group that becomes ready whenever any of the registered
 * interrupt file descriptors are ready
 *
 *
 * \param thread The thread to get.
 *
 * \return The spdk_fd_group of the thread itself.
 */
struct spdk_fd_group *spdk_thread_get_interrupt_fd_group(struct spdk_thread *thread);

/**
 * Set SPDK run as event driven mode
 *
 * \return 0 on success or -errno on failure
 */
int spdk_interrupt_mode_enable(void);

/**
 * Reports whether interrupt mode is set.
 *
 * \return True if interrupt mode is set, false otherwise.
 */
bool spdk_interrupt_mode_is_enabled(void);

/**
 * A spinlock augmented with safety checks for use with SPDK.
 *
 * SPDK code that uses spdk_spinlock runs from an SPDK thread, which itself is associated with a
 * pthread. There are typically many SPDK threads associated with each pthread. The SPDK application
 * may migrate SPDK threads between pthreads from time to time to balance the load on those threads.
 * Migration of SPDK threads only happens when the thread is off CPU, and as such it is only safe to
 * hold a lock so long as an SPDK thread stays on CPU.
 *
 * It is not safe to lock a spinlock, return from the event or poller, then unlock it at some later
 * time because:
 *
 *   - Even though the SPDK thread may be the same, the SPDK thread may be running on different
 *     pthreads during lock and unlock. A pthread spinlock may consider this to be an unlock by a
 *     non-owner, which results in undefined behavior.
 *   - A lock that is acquired by a poller or event may be needed by another poller or event that
 *     runs on the same pthread. This can lead to deadlock or detection of deadlock.
 *   - A lock that is acquired by a poller or event that is needed by another poller or event that
 *     runs on a second pthread will block the second pthread from doing any useful work until the
 *     lock is released. Because the lock holder and the lock acquirer are on the same pthread, this
 *     would lead to deadlock.
 *
 * If an SPDK spinlock is used erroneously, the program will abort.
 */
struct spdk_spinlock {
	pthread_spinlock_t spinlock;
	struct spdk_thread *thread;
	struct spdk_spinlock_internal *internal;
	uint32_t lock_count;
	bool recursive;
	bool initialized;
	bool destroyed;
};

/**
 * Initialize an spdk_spinlock.
 *
 * \param sspin The SPDK spinlock to initialize.
 */
void spdk_spin_init(struct spdk_spinlock *sspin);

/**
 * Initialize a recursive spdk_spinlock.
 *
 * It can be locked several times by the same thread. It should be unlocked by the same amount that it is locked for
 * release.
 *
 * \param sspin The SPDK spinlock to initialize.
 */
void spdk_spin_init_recursive(struct spdk_spinlock *sspin);

/**
 * Destroy an spdk_spinlock.
 *
 * \param sspin The SPDK spinlock to initialize.
 */
void spdk_spin_destroy(struct spdk_spinlock *sspin);

/**
 * Lock an SPDK spin lock.
 *
 * \param sspin An SPDK spinlock.
 */
void spdk_spin_lock(struct spdk_spinlock *sspin);

/**
 * Unlock an SPDK spinlock.
 *
 * \param sspin An SPDK spinlock.
 */
void spdk_spin_unlock(struct spdk_spinlock *sspin);

/**
 * Determine if the caller holds this SPDK spinlock.
 *
 * \param sspin An SPDK spinlock.
 * \return true if spinlock is held by this thread, else false
 */
bool spdk_spin_held(struct spdk_spinlock *sspin);

struct spdk_iobuf_opts {
	/** Maximum number of small buffers */
	uint64_t small_pool_count;
	/** Maximum number of large buffers */
	uint64_t large_pool_count;
	/** Size of a single small buffer */
	uint32_t small_bufsize;
	/** Size of a single large buffer */
	uint32_t large_bufsize;

	/**
	 * The size of spdk_iobuf_opts according to the caller of this library is used for ABI
	 * compatibility.  The library uses this field to know how many fields in this
	 * structure are valid. And the library will populate any remaining fields with default values.
	 * New added fields should be put at the end of the struct.
	 */
	size_t opts_size;

	/** Enable per-NUMA node buffer pools */
	uint8_t	enable_numa;
};

struct spdk_iobuf_pool_stats {
	/** Buffer got from local per-thread cache */
	uint64_t	cache;
	/** Buffer got from the main shared pool */
	uint64_t	main;
	/** Buffer missed and request to get buffer was queued */
	uint64_t	retry;
};

struct spdk_iobuf_module_stats {
	struct spdk_iobuf_pool_stats	small_pool;
	struct spdk_iobuf_pool_stats	large_pool;
	const char			*module;
};

struct spdk_iobuf_entry;

typedef void (*spdk_iobuf_get_cb)(struct spdk_iobuf_entry *entry, void *buf);

/** iobuf queue entry */
struct spdk_iobuf_entry {
	spdk_iobuf_get_cb		cb_fn;
	const void			*module;
	STAILQ_ENTRY(spdk_iobuf_entry)	stailq;
};

struct spdk_iobuf_buffer {
	STAILQ_ENTRY(spdk_iobuf_buffer)	stailq;
};

typedef STAILQ_HEAD(, spdk_iobuf_entry) spdk_iobuf_entry_stailq_t;
typedef STAILQ_HEAD(, spdk_iobuf_buffer) spdk_iobuf_buffer_stailq_t;

struct spdk_iobuf_pool_cache {
	/** Buffer pool */
	struct spdk_ring		*pool;
	/** Buffer cache */
	spdk_iobuf_buffer_stailq_t	cache;
	/** Number of elements in the cache */
	uint32_t			cache_count;
	/** Size of the cache */
	uint32_t			cache_size;
	/** Buffer wait queue */
	spdk_iobuf_entry_stailq_t	*queue;
	/** Buffer size */
	uint32_t			bufsize;
	/** Pool usage statistics */
	struct spdk_iobuf_pool_stats	stats;
};

struct spdk_iobuf_node_cache {
	/** Small buffer memory pool cache */
	struct spdk_iobuf_pool_cache	small;
	/** Large buffer memory pool cache */
	struct spdk_iobuf_pool_cache	large;
};

#ifndef SPDK_CONFIG_MAX_NUMA_NODES
/* Set this default temporarily, for users that may pull latest code without
 * re-running configure.
 */
#define SPDK_CONFIG_MAX_NUMA_NODES 1
#endif

/** iobuf channel */
struct spdk_iobuf_channel {
	/** Module pointer */
	const void			*module;
	/** Parent IO channel */
	struct spdk_io_channel		*parent;
	/* Buffer cache */
	struct spdk_iobuf_node_cache	cache[SPDK_CONFIG_MAX_NUMA_NODES];
};

/**
 * Initialize and allocate iobuf pools.
 *
 * \return 0 on success, negative errno otherwise.
 */
int spdk_iobuf_initialize(void);

typedef void (*spdk_iobuf_finish_cb)(void *cb_arg);

/**
 * Clean up and free iobuf pools.
 *
 * \param cb_fn Callback to be executed once the clean up is completed.
 * \param cb_arg Callback argument.
 */
void spdk_iobuf_finish(spdk_iobuf_finish_cb cb_fn, void *cb_arg);

/**
 * Set iobuf options.  These options will be used during `spdk_iobuf_initialize()`.
 *
 * \param opts Options describing the size of the pools to reserve.
 *
 * \return 0 on success, negative errno otherwise.
 */
int spdk_iobuf_set_opts(const struct spdk_iobuf_opts *opts);

/**
 * Get iobuf options.
 *
 * \param opts Output parameter for options.
 * \param opts_size sizeof(*opts)
 */
void spdk_iobuf_get_opts(struct spdk_iobuf_opts *opts, size_t opts_size);

/**
 * Register a module as an iobuf pool user.  Only registered users can request buffers from the
 * iobuf pool.
 *
 * \name Name of the module.
 *
 * \return 0 on success, negative errno otherwise.
 */
int spdk_iobuf_register_module(const char *name);

/**
 * Unregister an iobuf pool user from a module.
 *
 * \name Name of the module.
 *
 * \return 0 on success, negative errno otherwise.
 */
int spdk_iobuf_unregister_module(const char *name);

/**
 * Initialize an iobuf channel.
 *
 * \param ch iobuf channel to initialize.
 * \param name Name of the module registered via `spdk_iobuf_register_module()`.
 * \param small_cache_size Number of small buffers to be cached by this channel.
 * \param large_cache_size Number of large buffers to be cached by this channel.
 *
 * \return 0 on success, negative errno otherwise.
 */
int spdk_iobuf_channel_init(struct spdk_iobuf_channel *ch, const char *name,
			    uint32_t small_cache_size, uint32_t large_cache_size);

/**
 * Release resources tied to an iobuf channel.
 *
 * \param ch iobuf channel.
 */
void spdk_iobuf_channel_fini(struct spdk_iobuf_channel *ch);

typedef int (*spdk_iobuf_for_each_entry_fn)(struct spdk_iobuf_channel *ch,
		struct spdk_iobuf_entry *entry, void *ctx);

/**
 * Iterate over all entries on a given channel and execute a callback on those that were requested.
 * The iteration is stopped if the callback returns non-zero status.
 *
 * \param ch iobuf channel to iterate over.
 * \param cb_fn Callback to execute on each entry on the channel that was requested.
 * \param cb_ctx Argument passed to `cb_fn`.
 *
 * \return status of the last callback.
 */
int spdk_iobuf_for_each_entry(struct spdk_iobuf_channel *ch,
			      spdk_iobuf_for_each_entry_fn cb_fn, void *cb_ctx);

/**
 * Abort an outstanding request waiting for a buffer.
 *
 * \param ch iobuf channel on which the entry is waiting.
 * \param entry Entry to remove from the wait queue.
 * \param len Length of the requested buffer (must be the exact same value as specified in
 *            `spdk_iobuf_get()`.
 */
void spdk_iobuf_entry_abort(struct spdk_iobuf_channel *ch, struct spdk_iobuf_entry *entry,
			    uint64_t len);

/**
 * Get a buffer from the iobuf pool. If no buffers are available and entry with cb_fn provided
 * then the request is queued until a buffer becomes available.
 *
 * \param ch iobuf channel.
 * \param len Length of the buffer to retrieve. The user is responsible for making sure the length
 *            doesn't exceed large_bufsize.
 * \param entry Wait queue entry (optional).
 * \param cb_fn Callback to be executed once a buffer becomes available. If a buffer is available
 *              immediately, it is NOT executed. Mandatory only if entry provided.
 *
 * \return pointer to a buffer or NULL if no buffers are currently available.
 */
void *spdk_iobuf_get(struct spdk_iobuf_channel *ch, uint64_t len, struct spdk_iobuf_entry *entry,
		     spdk_iobuf_get_cb cb_fn);

/**
 * Release a buffer back to the iobuf pool.  If there are outstanding requests waiting for a buffer,
 * this buffer will be passed to one of them.
 *
 * \param ch iobuf channel.
 * \param buf Buffer to release
 * \param len Length of the buffer (must be the exact same value as specified in `spdk_iobuf_get()`).
 */
void spdk_iobuf_put(struct spdk_iobuf_channel *ch, void *buf, uint64_t len);

typedef void (*spdk_iobuf_get_stats_cb)(struct spdk_iobuf_module_stats *modules,
					uint32_t num_modules, void *cb_arg);

/**
 * Get iobuf statistics.
 *
 * \param cb_fn Callback to be executed once stats are gathered.
 * \param cb_arg Argument passed to the callback function.
 *
 * \return 0 on success, negative errno otherwise.
 */
int spdk_iobuf_get_stats(spdk_iobuf_get_stats_cb cb_fn, void *cb_arg);

typedef void (*spdk_post_poller_fn)(void *fn_arg);

/**
 * Register a function to be called after the current SPDK poller has completed. Once called,
 * this function is de-registered and won't called until the next registration call.
 *
 * \param fn Function to call
 * \param fn_arg Function argument
 */
void spdk_thread_register_post_poller_handler(spdk_post_poller_fn fn, void *fn_arg);

#ifdef __cplusplus
}
#endif

#endif /* SPDK_THREAD_H_ */
