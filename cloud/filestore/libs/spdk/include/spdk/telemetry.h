/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
 *   All rights reserved.
 */

/** \file
 * Telemetry API
 */

#ifndef SPDK_TELEMETRY_H_
#define SPDK_TELEMETRY_H_

#include "spdk/stdinc.h"
#include "spdk/queue.h"
#include "spdk/util.h"
#include "spdk/json.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SPDK_TELEMETRY_DEFAULT_INTERVAL_MS 5000

/**
 * Telemetry state change done callback.
 *
 * \param cb_arg Callback argument.
 * \param rc 0 if telemetry state change completed successfully or negative errno if it failed.
 */
typedef void (*spdk_telemetry_done_cb)(void *cb_arg, int rc);

/**
 * Telemetry options.
 */
struct spdk_telemetry_opts {
	/** Telemetry interval in milliseconds. */
	uint64_t interval_ms;
};

/**
 * Initialize telemetry library.
 */
int spdk_telemetry_init(void);

/**
 * Finalize telemetry library.
 *
 * \param cb_fn Called when the finalization is complete.
 * \param cb_arg Argument passed to function cb_fn.
 */
void spdk_telemetry_fini(spdk_telemetry_done_cb cb_fn, void *cb_arg);

/**
 * Start telemetry.
 *
 * \param opts Telemetry options.
 * \param cb Called when the telemetry state change is complete.
 * \param cb_arg Argument passed to function cb.
 */
void spdk_telemetry_start(const struct spdk_telemetry_opts *opts, spdk_telemetry_done_cb cb,
			  void *cb_arg);

/**
 * Stop telemetry.
 *
 * \param cb Called when the telemetry state change is complete.
 * \param cb_arg Argument passed to function cb.
 */
void spdk_telemetry_stop(spdk_telemetry_done_cb cb, void *cb_arg);

/**
 * Dump telemetry information in JSON format.
 *
 * \param w JSON write context.
 */
void spdk_telemetry_dump_info_json(struct spdk_json_write_ctx *w);

/**
 * Write telemetry configuration in JSON format.
 *
 * \param w JSON write context.
 */
void spdk_telemetry_write_config_json(struct spdk_json_write_ctx *w);

/** Telemetry exporter module */
struct spdk_telemetry_exporter_module {
	/**
	 * Initialization function for the module. Called by the telemetry library
	 * during startup.
	 *
	 * Exporters are required to define this function.
	 */
	int (*init)(void);

	/**
	 * Finish function for the module. Called by the telemetry library
	 * after all telemetry exporters have been unregistered.  This allows
	 * the exporter to do any final cleanup before the telemetry library finishes operation.
	 *
	 * Modules are not required to define this function.
	 */
	void (*fini)(void);

	/**
	 * Function called to return a text string representing the module-level
	 * JSON RPCs required to regenerate the current configuration. This will
	 * include module-level configuration options.
	 */
	void (*config_json)(struct spdk_json_write_ctx *w);

	/**
	 * Function called to return a text string representing the module-level
	 * information to be dumped. This will include module-level information
	 * such as the current configuration.
	 */
	void (*dump_info_json)(struct spdk_json_write_ctx *w);

	/** Name for the module being defined. */
	const char *name;

	/**
	 * Fields that are used by the internal telemetry subsystem. Telemetry modules
	 *  must not read or write to these fields.
	 */
	struct __telemetry_module_internal_fields {
		TAILQ_ENTRY(spdk_telemetry_exporter_module) tailq;
	} internal;
};

/* Module-specific handle for a telemetry type */
struct spdk_telemetry_type_handle;

/* Module-specific handle for a telemetry source */
struct spdk_telemetry_source_handle;

/**
 * Function table for a telemetry exporter backend.
 *
 * The backend telemetry exporter function table provides a set of APIs to allow
 * communication with a backend.
 */
struct spdk_telemetry_exporter_fn_table {
	/**
	 * Destroy the backend telemetry exporter object. If the destruct process
	 *  for the telemetry exporter is asynchronous, return 1 from this function, and
	 *  then call spdk_telemetry_exporter_destruct_done() once the async work is
	 *  complete. If the destruct process is synchronous, return 0 if
	 *  successful, or <0 if unsuccessful.
	 */
	int (*destruct)(void *ctx);

	/** Register a telemetry type */
	struct spdk_telemetry_type_handle *(*register_type)(void *ctx, const char *name,
			const char **stat_names, uint64_t num_stats);

	/** Unregister a telemetry type */
	void (*unregister_type)(void *ctx, struct spdk_telemetry_type_handle *handle);

	/** Register a telemetry source */
	struct spdk_telemetry_source_handle *(*register_source)(void *ctx,
			struct spdk_telemetry_type_handle *type_handle, const char *name);

	/** Unregister a telemetry source */
	void (*unregister_source)(void *ctx, struct spdk_telemetry_source_handle *handle);

	/** Report telemetry stats
	 *
	 * \param ctx The context for the telemetry exporter.
	 * \param handle The handle for the telemetry source.
	 * \param stats The stats to report.
	 * \param num_stats The number of stats to report.
	 *
	 * \return \p true if the stats can be released immediately, \p false otherwise.
	 * If \p false, the exporter will call \p spdk_telemetry_exporter_release_stats() later to release the stats.
	 *
	 * \note The telemetry core ensures that only one stats array per source is reported at a time. So, if the exporter
	 * returns \p false, the telemetry core will not call this function again until the stats are released by the exporter
	 * using \p spdk_telemetry_exporter_release_stats() API.
	 */
	bool (*report_stats)(void *ctx, struct spdk_telemetry_source_handle *handle, const uint64_t *stats,
			     uint64_t num_stats);

	/**
	 * Output telemetry exporter-specific RPC configuration to a JSON stream. Optional - may be \p NULL.
	 *
	 * The JSON write context will be initialized with an open object, so the telemetry exporter
	 * driver should write all data necessary to recreate this telemetry exporter by invoking
	 * constructor method. No other data should be written.
	 */
	void (*write_config_json)(void *ctx, struct spdk_json_write_ctx *w);

	/**
	 * Output driver-specific information to a JSON stream. Optional - may be \p NULL.
	 *
	 * The JSON write context will be initialized with an open object, so the telemetry exporter
	 * should write a name (based on the exporter name) followed by a JSON value
	 * (most likely another nested object).
	 */
	void (*dump_info_json)(void *ctx, struct spdk_json_write_ctx *w);
};

struct spdk_telemetry_exporter {
	/** User context passed in by the backend */
	void *ctxt;

	/**
	 * Pointer to the telemetry module that registered this telemetry exporter.
	 */
	struct spdk_telemetry_exporter_module *module;

	/** function table for all ops */
	const struct spdk_telemetry_exporter_fn_table *fn_table;
};

/**
 * Register a telemetry exporter.
 *
 * \param telemetry_exporter Telemetry exporter to register.
 *
 * \return 0 on success.
 * \return -EINVAL if the telemetry exporter name is NULL.
 * \return -EEXIST if a telemetry exporter with the same name already exists.
 *
 * NOTE: Only one telemetry exporter can be registered at a time.
 */
int spdk_telemetry_exporter_register(struct spdk_telemetry_exporter *telemetry_exporter);

/**
 * Unregister a telemetry exporter.
 *
 * \param telemetry_exporter Telemetry exporter to unregister.
 * \return 0 on success.
 * \return -ENOENT if no telemetry exporter is registered.
 * \return -EINVAL if the telemetry exporter is not the one registered.
  */
int spdk_telemetry_exporter_unregister(struct spdk_telemetry_exporter *telemetry_exporter);

/**
 * Release stats received via the report_stat API
 *
 * \param handle The handle for the telemetry source.
 * \param stats The stats to release.
 * \param num_stats The number of stats to release.
 */
void spdk_telemetry_exporter_release_stats(struct spdk_telemetry_source_handle *handle,
		const uint64_t *stats, uint64_t num_stats);

/**
 * Notify the telemetry exporter layer that an asynchronous destruct operation is complete.
 *
 * A telemetry exporter with an asynchronous destruct path should return 1 from its
 * destruct function and call this function at the conclusion of that path.
 * Telemetry exporters with synchronous destruct paths should return 0 from their destruct
 * path.
 */
void spdk_telemetry_exporter_destruct_done(void);

/**
 * Add the given module to the list of registered modules.
 * This function should be invoked by referencing the macro
 * SPDK_TELEMETRY_MODULE_REGISTER in the module c file.
 *
 * \param telemetry_module Module to be added.
 */
void spdk_telemetry_module_list_add(struct spdk_telemetry_exporter_module *module);

/*
 *  Macro used to register exporter for later initialization.
 */
#define SPDK_TELEMETRY_MODULE_REGISTER(name, telemetry_module) \
static void __attribute__((constructor)) _spdk_telemetry_module_register_##name(void) \
{ \
	spdk_telemetry_module_list_add(telemetry_module); \
}

#ifdef __cplusplus
}
#endif

#endif /* SPDK_TELEMETRY_H_ */
