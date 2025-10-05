/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (C) 2016 Intel Corporation.
 *   Copyright (c) 2023-2025 NVIDIA CORPORATION & AFFILIATES.
 *   All rights reserved.
 */

/**
 * \file
 * Tracepoint library
 */

#ifndef _SPDK_TRACE_H_
#define _SPDK_TRACE_H_

#include "spdk/stdinc.h"
#include "spdk/assert.h"

#ifdef __cplusplus
extern "C" {
#endif

#define SPDK_TRACE_SHM_NAME_BASE "_trace."
#define SPDK_DEFAULT_NUM_TRACE_ENTRIES	 (32 * 1024)

struct spdk_trace_entry {
	uint64_t	tsc;
	uint16_t	tpoint_id;
	uint16_t	owner_id;
	uint32_t	size;
	uint64_t	object_id;
	uint8_t		args[8];
};

struct spdk_trace_entry_buffer {
	uint64_t	tsc;
	uint16_t	tpoint_id;
	uint8_t		data[22];
};

SPDK_STATIC_ASSERT(sizeof(struct spdk_trace_entry_buffer) == sizeof(struct spdk_trace_entry),
		   "Invalid size of trace entry buffer");

/* If type changes from a uint8_t, change this value. */
#define SPDK_TRACE_MAX_OWNER_TYPE (UCHAR_MAX + 1)

struct spdk_trace_owner_type {
	uint8_t	type;
	char	id_prefix;
};

struct spdk_trace_owner {
	uint64_t	tsc;
	uint8_t		type;
	char		description[];
} __attribute__((packed));

/* If type changes from a uint8_t, change this value. */
#define SPDK_TRACE_MAX_OBJECT (UCHAR_MAX + 1)

struct spdk_trace_object {
	uint8_t	type;
	char	id_prefix;
};

#define	SPDK_TRACE_THREAD_NAME_LEN 16
#define SPDK_TRACE_MAX_GROUP_ID  32
#define SPDK_TRACE_MAX_TPOINT_ID (SPDK_TRACE_MAX_GROUP_ID * 64)
#define SPDK_TPOINT_ID(group, tpoint)	((group * 64) + tpoint)

#define SPDK_TRACE_ARG_TYPE_INT 0
#define SPDK_TRACE_ARG_TYPE_PTR 1
#define SPDK_TRACE_ARG_TYPE_STR 2

#define SPDK_TRACE_MAX_ARGS_COUNT 8
#define SPDK_TRACE_MAX_RELATIONS 16

struct spdk_trace_argument {
	char	name[14];
	uint8_t	type;
	uint8_t	size;
};

struct spdk_trace_tpoint {
	char				name[24];
	uint16_t			tpoint_id;
	uint8_t				owner_type;
	uint8_t				object_type;
	uint8_t				new_object;
	uint8_t				num_args;
	struct spdk_trace_argument	args[SPDK_TRACE_MAX_ARGS_COUNT];
	/** Relations between tracepoint and trace object */
	struct {
		uint8_t object_type;
		uint8_t arg_index;
	} related_objects[SPDK_TRACE_MAX_RELATIONS];
};

struct spdk_trace_history {
	/** Logical core number associated with this structure instance. */
	int				lcore;

	/** Number of trace_entries contained in each trace_history. */
	uint64_t			num_entries;

	/**
	 * Running count of number of occurrences of each tracepoint on this
	 *  lcore.  Debug tools can use this to easily count tracepoints such as
	 *  number of SCSI tasks completed or PDUs read.
	 */
	uint64_t			tpoint_count[SPDK_TRACE_MAX_TPOINT_ID];

	/** Index to next spdk_trace_entry to fill. */
	uint64_t			next_entry;

	/**
	 * Circular buffer of spdk_trace_entry structures for tracing
	 *  tpoints on this core.  Debug tool spdk_trace reads this
	 *  buffer from shared memory to post-process the tpoint entries and
	 *  display in a human-readable format.
	 */
	struct spdk_trace_entry		entries[0];
};

#define SPDK_TRACE_MAX_LCORE		1024

struct spdk_trace_file {
	uint64_t			file_size;
	uint64_t			tsc_rate;
	uint64_t			tpoint_mask[SPDK_TRACE_MAX_GROUP_ID];
	char				tname[SPDK_TRACE_MAX_LCORE][SPDK_TRACE_THREAD_NAME_LEN];
	struct spdk_trace_owner_type	owner_type[SPDK_TRACE_MAX_OWNER_TYPE];
	struct spdk_trace_object	object[UCHAR_MAX + 1];
	struct spdk_trace_tpoint	tpoint[SPDK_TRACE_MAX_TPOINT_ID];

	uint16_t			num_owners;
	uint16_t			owner_description_size;
	uint8_t				reserved[4];

	/** Offset of each trace_history from the beginning of this data structure. */
	uint64_t			lcore_history_offsets[SPDK_TRACE_MAX_LCORE];

	/** Offset of beginning of struct spdk_trace_owner data. */
	uint64_t			owner_offset;

	/** Variable sized data sections are at the end of this data structure,
	 *  referenced by offsets defined in this structure.
	 */
	uint8_t	data[0];
};
extern struct spdk_trace_file *g_trace_file;

static inline uint64_t
spdk_get_trace_history_size(uint64_t num_entries)
{
	return sizeof(struct spdk_trace_history) + num_entries * sizeof(struct spdk_trace_entry);
}

static inline uint64_t
spdk_get_trace_file_size(struct spdk_trace_file *trace_file)
{
	return trace_file->file_size;
}

static inline struct spdk_trace_history *
spdk_get_per_lcore_history(struct spdk_trace_file *trace_file, unsigned lcore)
{
	uint64_t lcore_history_offset;

	if (lcore >= SPDK_TRACE_MAX_LCORE) {
		return NULL;
	}

	lcore_history_offset = trace_file->lcore_history_offsets[lcore];
	if (lcore_history_offset == 0) {
		return NULL;
	}

	return (struct spdk_trace_history *)(((char *)trace_file) + lcore_history_offset);
}

static inline struct spdk_trace_owner *
spdk_get_trace_owner(const struct spdk_trace_file *trace_file, uint16_t owner_id)
{
	uint64_t owner_size;

	if (owner_id >= trace_file->num_owners) {
		return NULL;
	}

	owner_size = sizeof(struct spdk_trace_owner) + trace_file->owner_description_size;
	return (struct spdk_trace_owner *)
	       (((char *)trace_file) + trace_file->owner_offset + owner_id * owner_size);
}

void _spdk_trace_record(uint64_t tsc, uint16_t tpoint_id, uint16_t owner_id,
			uint32_t size, uint64_t object_id, int num_args, ...);

#define spdk_trace_tpoint_enabled(tpoint_id)	\
	spdk_unlikely((g_trace_file != NULL  && \
	((1ULL << (tpoint_id & 0x3F)) &	g_trace_file->tpoint_mask[tpoint_id >> 6])))

#define _spdk_trace_record_tsc(tsc, tpoint_id, owner_id, size, object_id, num_args, ...)	\
	do {											\
		assert(tpoint_id < SPDK_TRACE_MAX_TPOINT_ID);					\
		if (!spdk_trace_tpoint_enabled(tpoint_id)) {					\
			break;									\
		}										\
		_spdk_trace_record(tsc, tpoint_id, owner_id, size, object_id,			\
				   num_args, ## __VA_ARGS__);					\
	} while (0)

/* Return the number of variable arguments. */
#define spdk_trace_num_args(...) _spdk_trace_num_args(, ## __VA_ARGS__)
#define _spdk_trace_num_args(...) __spdk_trace_num_args(__VA_ARGS__, 8, 7, 6, 5, 4, 3, 2, 1, 0)
#define __spdk_trace_num_args(v, a1, a2, a3, a4, a5, a6, a7, a8, count, ...) count

/**
 * Record the current trace state for tracing tpoints. Debug tool can read the
 * information from shared memory to post-process the tpoint entries and display
 * in a human-readable format.
 *
 * \param tsc Current tsc.
 * \param tpoint_id Tracepoint id to record.
 * \param owner_id Owner id to record.
 * \param size Size to record.
 * \param object_id Object id to record.
 * \param ... Extra tracepoint arguments. The number, types, and order of the arguments
 *	      must match the definition of the tracepoint.
 */
#define spdk_trace_record_tsc(tsc, tpoint_id, owner_id, size, object_id, ...)	\
	_spdk_trace_record_tsc(tsc, tpoint_id, owner_id, size, object_id,	\
			       spdk_trace_num_args(__VA_ARGS__), ## __VA_ARGS__)

/**
 * Record the current trace state for tracing tpoints. Debug tool can read the
 * information from shared memory to post-process the tpoint entries and display
 * in a human-readable format. This macro will call spdk_get_ticks() to get
 * the current tsc to save in the tracepoint.
 *
 * \param tpoint_id Tracepoint id to record.
 * \param owner_id Owner id to record.
 * \param size Size to record.
 * \param object_id Object id to record.
 * \param ... Extra tracepoint arguments. The number, types, and order of the arguments
 *	      must match the definition of the tracepoint.
 */
#define spdk_trace_record(tpoint_id, owner_id, size, object_id, ...) \
	spdk_trace_record_tsc(0, tpoint_id, owner_id, size, object_id, ## __VA_ARGS__)

/**
 * Get the current tpoint mask of the given tpoint group.
 *
 * \param group_id Tpoint group id associated with the tpoint mask.
 *
 * \return current tpoint mask.
 */
uint64_t spdk_trace_get_tpoint_mask(uint32_t group_id);

/**
 * Add the specified tpoints to the current tpoint mask for the given tpoint group.
 *
 * \param group_id Tpoint group id associated with the tpoint mask.
 * \param tpoint_mask Tpoint mask which indicates which tpoints to add to the
 * current tpoint mask.
 */
void spdk_trace_set_tpoints(uint32_t group_id, uint64_t tpoint_mask);

/**
 * Clear the specified tpoints from the current tpoint mask for the given tpoint group.
 *
 * \param group_id Tpoint group id associated with the tpoint mask.
 * \param tpoint_mask Tpoint mask which indicates which tpoints to clear from
 * the current tpoint mask.
 */
void spdk_trace_clear_tpoints(uint32_t group_id, uint64_t tpoint_mask);

/**
 * Get a mask of all tracepoint groups which have at least one tracepoint enabled.
 *
 * \return a mask of all tracepoint groups.
 */
uint64_t spdk_trace_get_tpoint_group_mask(void);

/**
 * For each tpoint group specified in the group mask, enable all of its tpoints.
 *
 * \param tpoint_group_mask Tpoint group mask that indicates which tpoints to enable.
 */
void spdk_trace_set_tpoint_group_mask(uint64_t tpoint_group_mask);

/**
 * For each tpoint group specified in the group mask, disable all of its tpoints.
 *
 * \param tpoint_group_mask Tpoint group mask that indicates which tpoints to disable.
 */
void spdk_trace_clear_tpoint_group_mask(uint64_t tpoint_group_mask);

/**
 * Initialize the trace environment. Debug tool can read the information from
 * the given shared memory to post-process the tpoint entries and display in a
 * human-readable format.
 *
 * \param shm_name Name of shared memory.
 * \param num_entries Number of trace entries per lcore.
 * \param num_threads Number of user created threads.
 * \return 0 on success, else non-zero indicates a failure.
 */
int spdk_trace_init(const char *shm_name, uint64_t num_entries, uint32_t num_threads);

/**
 * Initialize trace environment for an user created thread.
 *
 * \return 0 on success, else non-zero indicates a failure.
 */
int spdk_trace_register_user_thread(void);

/**
 * De-initialize trace environment for an user created thread.
 *
 * \return 0 on success, else non-zero indicates a failure.
 */
int spdk_trace_unregister_user_thread(void);

/**
 * Unmap global trace memory structs.
 */
void spdk_trace_cleanup(void);

#define OWNER_TYPE_NONE 0
#define OBJECT_NONE 0

/**
 * Register the trace owner type.
 *
 * \param type Type of the trace owner.
 * \param id_prefix Prefix of id for the trace owner.
 */
void spdk_trace_register_owner_type(uint8_t type, char id_prefix);

/**
 * Register the trace owner.
 *
 * \param owner_type Type of the owner being registered.
 * \param description Textual string describing the trace owner.
 * \return Allocated owner_id for the newly registered owner. Returns 0 if
 *         no trace_id could be allocated.
 */
uint16_t spdk_trace_register_owner(uint8_t owner_type, const char *description);

/**
 * Change the description for a previously registered owner.
 *
 * \param owner_id ID of previously registered owner.
 * \param description New description for the owner.
 */
void spdk_trace_owner_set_description(uint16_t owner_id, const char *description);

/**
 * Append to the description for a previously registered owner.
 *
 * A space will be inserted before the appended string.
 *
 * This may be useful for modules that are modifying an existing description
 * with additional information.
 *
 * Callers may pass 0 safely, in this case the function will just be a nop.
 *
 * \param owner_id ID of previously registered owner.
 * \param description Additional description for the owner
 */
void spdk_trace_owner_append_description(uint16_t owner_id, const char *description);

/**
 * Unregister a previously registered owner.
 *
 * Callers may pass 0 safely, in this case the function will just be a nop.
 *
 * \param owner_id ID of previously registered owner.
 */
void spdk_trace_unregister_owner(uint16_t owner_id);

/**
 * Register the trace object.
 *
 * \param type Type of the trace object.
 * \param id_prefix Prefix of id for the trace object.
 */
void spdk_trace_register_object(uint8_t type, char id_prefix);

/**
 * Register the description for a tpoint with a single argument.
 *
 * \param name Name for the tpoint.
 * \param tpoint_id Id for the tpoint.
 * \param owner_type Owner type for the tpoint.
 * \param object_type Object type for the tpoint.
 * \param new_object New object for the tpoint.
 * \param arg1_type Type of arg1.
 * \param arg1_name Name of argument.
 */
void spdk_trace_register_description(const char *name, uint16_t tpoint_id, uint8_t owner_type,
				     uint8_t object_type, uint8_t new_object,
				     uint8_t arg1_type, const char *arg1_name);

struct spdk_trace_tpoint_opts {
	const char	*name;
	uint16_t	tpoint_id;
	uint8_t		owner_type;
	uint8_t		object_type;
	uint8_t		new_object;
	struct {
		const char	*name;
		uint8_t		type;
		uint8_t		size;
	} args[SPDK_TRACE_MAX_ARGS_COUNT];
};

/**
 * Register the description for a number of tpoints. This function allows the user to register
 * tracepoints with multiple arguments.
 *
 * \param opts Array of structures describing tpoints and their arguments.
 * \param num_opts Number of tpoints to register (size of the opts array).
 */
void spdk_trace_register_description_ext(const struct spdk_trace_tpoint_opts *opts,
		size_t num_opts);

struct spdk_trace_register_fn *spdk_trace_get_first_register_fn(void);

struct spdk_trace_register_fn *spdk_trace_get_next_register_fn(struct spdk_trace_register_fn
		*register_fn);

/**
 * Bind trace type to a given trace object. This allows for matching traces
 * with the same parent trace object.
 *
 * \param tpoint_id Type of trace to be bound
 * \param object_type Tracepoint object type to bind to
 * \param arg_index Index of argument containing context information
 */
void spdk_trace_tpoint_register_relation(uint16_t tpoint_id, uint8_t object_type,
		uint8_t arg_index);

/**
 * Enable trace on specific tpoint group
 *
 * \param group_name Name of group to enable, "all" for enabling all groups.
 * \return 0 on success, else non-zero indicates a failure.
 */
int spdk_trace_enable_tpoint_group(const char *group_name);

/**
 * Disable trace on specific tpoint group
 *
 * \param group_name Name of group to disable, "all" for disabling all groups.
 * \return 0 on success, else non-zero indicates a failure.
 */
int spdk_trace_disable_tpoint_group(const char *group_name);

/**
 * Show trace mask and its usage.
 *
 * \param f File to hold the mask's information.
 * \param tmask_arg Command line option to set the trace group mask.
 */
void spdk_trace_mask_usage(FILE *f, const char *tmask_arg);

/**
 * Create a tracepoint group mask from tracepoint group name
 *
 * \param group_name tracepoint group name string
 * \return tpoint group mask on success, 0 on failure
 */
uint64_t spdk_trace_create_tpoint_group_mask(const char *group_name);

struct spdk_trace_register_fn {
	const char *name;
	uint8_t tgroup_id;
	void (*reg_fn)(void);
	struct spdk_trace_register_fn *next;
};

/**
 * Add new trace register function.
 *
 * \param reg_fn Trace register function to add.
 */
void spdk_trace_add_register_fn(struct spdk_trace_register_fn *reg_fn);

#define SPDK_TRACE_REGISTER_FN(fn, name_str, _tgroup_id)	\
	static void fn(void);					\
	struct spdk_trace_register_fn reg_ ## fn = {		\
		.name = name_str,				\
		.tgroup_id = _tgroup_id,			\
		.reg_fn = fn,					\
		.next = NULL,					\
	};							\
	__attribute__((constructor)) static void _ ## fn(void)	\
	{							\
		spdk_trace_add_register_fn(&reg_ ## fn);	\
	}

#ifdef __cplusplus
}
#endif

#endif
