/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2021 Intel Corporation.  All rights reserved.
 *   Copyright (c) 2023-2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

/**
 * \file
 * SPDK Initialization Helper
 */

#ifndef SPDK_INIT_H
#define SPDK_INIT_H

#include "spdk/stdinc.h"
#include "spdk/queue.h"
#include "spdk/log.h"
#include "spdk/assert.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SPDK_DEFAULT_RPC_ADDR "/var/tmp/spdk.sock"

/**
 * Structure with optional parameters for the JSON-RPC server initialization.
 */
struct spdk_rpc_opts {
	/* Size of this structure in bytes. */
	size_t size;
	/*
	 * A JSON-RPC log file pointer. The default value is NULL and used
	 * when options are omitted.
	 */
	FILE *log_file;
	/*
	 * JSON-RPC log level. Default value is SPDK_LOG_DISABLED and used
	 * when options are omitted.
	 */
	enum spdk_log_level log_level;
};
SPDK_STATIC_ASSERT(sizeof(struct spdk_rpc_opts) == 24, "Incorrect size");

/**
 * Create SPDK JSON-RPC server listening at provided address and start polling it for connections.
 *
 * The RPC server is optional and is independent of subsystem initialization.
 * The RPC server can be started and stopped at any time.
 *
 * \param listen_addr Path to a unix domain socket to listen on
 * \param opts Options for JSON-RPC server initialization. If NULL, default values are used.
 *
 * \return Negated errno on failure. 0 on success.
 */
int spdk_rpc_initialize(const char *listen_addr,
			const struct spdk_rpc_opts *opts);

/**
 * Stop SPDK JSON-RPC servers and stop polling for new connections on all addresses.
 */
void spdk_rpc_finish(void);

/**
 * Stop SPDK JSON-RPC server and stop polling for new connections on provided address.
 *
 * \param listen_addr Path to a unix domain socket.
 */
void spdk_rpc_server_finish(const char *listen_addr);

typedef void (*spdk_subsystem_init_fn)(int rc, void *ctx);

/**
 * Begin the initialization process for all SPDK subsystems. SPDK is divided into subsystems at a macro-level
 * and each subsystem automatically registers itself with this library at start up using a C
 * constructor. Further, each subsystem can declare other subsystems that it depends on.
 * Calling this function will correctly initialize all subsystems that are present, in the
 * required order.
 *
 * \param cb_fn Function called when the process is complete.
 * \param cb_arg User context passed to cb_fn.
 */
void spdk_subsystem_init(spdk_subsystem_init_fn cb_fn, void *cb_arg);

/**
 * Loads RPC configuration from provided JSON for current RPC state. The function will
 * automatically start a JSON RPC server for configuration purposes and then stop it.
 * JSON data will be copied, so parsing will not disturb the original memory.
 *
 * \param json Raw JSON data.
 * \param json_size Size of JSON data.
 * \param cb_fn Function called when the process is complete.
 * \param cb_arg User context passed to cb_fn.
 * \param stop_on_error Whether to stop initialization if one of the JSON RPCs fails.
 */
void spdk_subsystem_load_config(void *json, ssize_t json_size, spdk_subsystem_init_fn cb_fn,
				void *cb_arg, bool stop_on_error);

typedef void (*spdk_subsystem_fini_fn)(void *ctx);

/**
 * Tear down all of the subsystems in the correct order.
 *
 * \param cb_fn Function called when the process is complete.
 * \param cb_arg User context passed to cb_fn
 */
void spdk_subsystem_fini(spdk_subsystem_fini_fn cb_fn, void *cb_arg);

struct spdk_json_write_ctx;

/**
 * Check if the specified subsystem exists in the application.
 *
 * \param name Name of the subsystem to look for
 * \return true if it exists, false if not
 */
bool spdk_subsystem_exists(const char *name);

/**
 * Pause polling RPC server with given address.
 *
 * \param listen_addr Address, on which RPC server listens for connections.
 */
void spdk_rpc_server_pause(const char *listen_addr);

/**
 * Resume polling RPC server with given address.
 *
 * \param listen_addr Address, on which RPC server listens for connections.
 */
void spdk_rpc_server_resume(const char *listen_addr);

/**
 * Represents an SPDK subsystem.
 */
struct spdk_subsystem {
	const char *name;
	/**
	 * This function must initialize the subsystem.
	 *
	 * User must call `spdk_subsystem_init_next()` when they are done with their initialization.
	 */
	void (*init)(void);

	/**
	 * This function must finalize and release resources for the subsystem.
	 *
	 * User must call `spdk_subsystem_fini_next()` when they are done with their initialization.
	 */
	void (*fini)(void);

	/**
	 * Write JSON configuration handler.
	 *
	 * A subsystem should dump all state in the form of JSON-RPC calls to this write context.
	 *
	 * \param w JSON write context
	 */
	void (*write_config_json)(struct spdk_json_write_ctx *w);

	TAILQ_ENTRY(spdk_subsystem) tailq;

	/**
	 * Whether this subsystem is currently enabled. By default, all registered subsystems are enabled.
	 */
	bool enabled;
};

/**
 * Tracks SPDK subsystem dependencies.
 */
struct spdk_subsystem_depend {
	const char *name;
	const char *depends_on;
	TAILQ_ENTRY(spdk_subsystem_depend) tailq;
};

/**
 * Register a subsystem with SPDK. Prefer SPDK_SUBSYSTEM_REGISTER instead.
 *
 * \param subsystem The subsystem to register.
 */
void spdk_add_subsystem(struct spdk_subsystem *subsystem);

/**
 * Add a dependency to a subsystem. Prefer SPDK_SUBSYSTEM_DEPEND instead.
 *
 * \param depend The subsystem dependency.
 */
void spdk_add_subsystem_depend(struct spdk_subsystem_depend *depend);

/**
 * Indicate that the current subsystem is done initializing and the system can move to the next subsystem.
 *
 * This may only be called in response to an spdk_subsystem::init call.
 *
 * \param rc The return code for the initialization. 0 is success.
 */
void spdk_subsystem_init_next(int rc);

/**
 * Indicate that the current subsystem is done finalizing and the system can move to the next subsystem.
 *
 * This may only be called in response to an spdk_subsystem::fini call.
 *
 * \param rc The return code for the finalization. 0 is success.
 */
void spdk_subsystem_fini_next(void);

/**
 * \brief Register a new subsystem
 *
 * Typically, a `struct spdk_subsystem` object will be created statically and then this macro will register it.
 */
#define SPDK_SUBSYSTEM_REGISTER(_name) \
	__attribute__((constructor)) static void _name ## _register(void)	\
	{									\
		spdk_add_subsystem(&_name);					\
	}

/**
 * \brief Declare that a subsystem depends on another subsystem.
 */
#define SPDK_SUBSYSTEM_DEPEND(_name, _depends_on)						\
	static struct spdk_subsystem_depend __subsystem_ ## _name ## _depend_on ## _depends_on = { \
	.name = #_name,										\
	.depends_on = #_depends_on,								\
	};											\
	__attribute__((constructor)) static void _name ## _depend_on ## _depends_on(void)	\
	{											\
		spdk_add_subsystem_depend(&__subsystem_ ## _name ## _depend_on ## _depends_on); \
	}

#endif

#ifdef __cplusplus
}
#endif
