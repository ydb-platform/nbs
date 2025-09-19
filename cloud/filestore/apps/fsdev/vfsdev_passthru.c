/*   SPDX-License-Identifier: BSD-3-Clause
 *   Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 */

/*
 * This is a simple example of a virtual file system device module that passes IO
 * down to a fsdev (or fsdevs) that its configured to attach to.
 */

#include "spdk/stdinc.h"

#include "vfsdev_passthru.h"
#include "spdk/rpc.h"
#include "spdk/env.h"
#include "spdk/endian.h"
#include "spdk/string.h"
#include "spdk/thread.h"
#include "spdk/util.h"

#include "spdk/fsdev_module.h"
#include "spdk/log.h"


static int vfsdev_passthru_init(void);
static int vfsdev_passthru_get_ctx_size(void);
static void vfsdev_passthru_finish(void);
static int vfsdev_passthru_config_json(struct spdk_json_write_ctx *w);

static struct spdk_fsdev_module passthru_if = {
	.name = "passthru_external",
	.module_init = vfsdev_passthru_init,
	.module_fini = vfsdev_passthru_finish,
	.config_json = vfsdev_passthru_config_json,
	.get_ctx_size = vfsdev_passthru_get_ctx_size
};

SPDK_FSDEV_MODULE_REGISTER(ext_passthru, &passthru_if)

/* List of passthru fsdev names and their base fsdevs via configuration file.
 * Used so we can parse the conf once at init and use this list in examine().
 */
struct passthru_associations {
	char			*passthru_name;
	char			*base_name;
	TAILQ_ENTRY(passthru_associations)	link;
};
static TAILQ_HEAD(, passthru_associations) g_passthru_associations = TAILQ_HEAD_INITIALIZER(
			g_passthru_associations);

/* List of virtual fsdevs and associated info for each. */
struct vfsdev_passthru {
	struct spdk_fsdev		*base_fsdev; /* the thing we're attaching to */
	struct spdk_fsdev_desc		*base_desc; /* its descriptor we get from open */
	struct spdk_fsdev		pt_fsdev;    /* the PT virtual fsdev */
	TAILQ_ENTRY(vfsdev_passthru)	link;
	struct spdk_thread		*thread;    /* thread where base device is opened */
};
static TAILQ_HEAD(, vfsdev_passthru) g_pt_nodes = TAILQ_HEAD_INITIALIZER(g_pt_nodes);

/* The pt vfsdev channel struct. It is allocated and freed on my behalf by the io channel code.
 * If this vfsdev needed to implement a poller or a queue for IO, this is where those things
 * would be defined. This passthru fsdev doesn't actually need to allocate a channel, it could
 * simply pass back the channel of the fsdev underneath it but for example purposes we will
 * present its own to the upper layers.
 */
struct pt_io_channel {
	struct spdk_io_channel	*base_ch; /* IO channel of base device */
};

/* This passthru_fsdev module doesn't need it but this is essentially a per IO
 * context that we get handed by the fsdev layer.
 */
struct passthru_fsdev_io {
	uint8_t test;

	struct spdk_fsdev_io *child_fsdev_io;
};

static void vfsdev_passthru_submit_request(struct spdk_io_channel *ch,
		struct spdk_fsdev_io *fsdev_io);


/* Callback for unregistering the IO device. */
static void
_device_unregister_cb(void *io_device)
{
	struct vfsdev_passthru *pt_node  = io_device;

	/* Done with this pt_node. */
	free(pt_node->pt_fsdev.name);
	free(pt_node);
}

/* Wrapper for the fsdev close operation. */
static void
_vfsdev_passthru_destruct(void *ctx)
{
	struct spdk_fsdev_desc *desc = ctx;

	spdk_fsdev_close(desc);
}

/* Called after we've unregistered following a hot remove callback.
 * Our finish entry point will be called next.
 */
static int
vfsdev_passthru_destruct(void *ctx)
{
	struct vfsdev_passthru *pt_node = (struct vfsdev_passthru *)ctx;

	/* It is important to follow this exact sequence of steps for destroying
	 * a vfsdev...
	 */
	TAILQ_REMOVE(&g_pt_nodes, pt_node, link);

	/* Close the underlying fsdev on its same opened thread. */
	if (pt_node->thread && pt_node->thread != spdk_get_thread()) {
		spdk_thread_send_msg(pt_node->thread, _vfsdev_passthru_destruct, pt_node->base_desc);
	} else {
		_vfsdev_passthru_destruct(pt_node->base_desc);
	}

	/* Unregister the io_device. */
	spdk_io_device_unregister(pt_node, _device_unregister_cb);

	return 0;
}

static void
vfsdev_passthru_cpl_cb(void *cb_arg, int status, struct spdk_fsdev_io *child_io)
{
	struct spdk_fsdev_io *fsdev_io = cb_arg;
	struct passthru_fsdev_io *io_ctx = (struct passthru_fsdev_io *)fsdev_io->driver_ctx;

	/* We setup this value in the submission routine, just showing here that it is
	 * passed back to us.
	 */
	if (io_ctx->test != 0x5a) {
		SPDK_ERRLOG("Error, original IO device_ctx is wrong! 0x%x\n", io_ctx->test);
	}

	memcpy(&fsdev_io->u_out, &child_io->u_out, sizeof(fsdev_io->u_out));
	free(child_io);
	spdk_fsdev_io_complete(fsdev_io, status);
}

/* Called when someone above submits IO to this pt vfsdev. We're simply passing it on here
 * via SPDK IO calls which in turn allocate another fsdev IO and call our cpl callback provided
 * below along with the original fsdev_io so that we can complete it once this IO completes.
 */
static void
vfsdev_passthru_submit_request(struct spdk_io_channel *ch, struct spdk_fsdev_io *fsdev_io)
{
	struct vfsdev_passthru *pt_node = SPDK_CONTAINEROF(fsdev_io->fsdev, struct vfsdev_passthru,
					  pt_fsdev);
	struct pt_io_channel *pt_ch = spdk_io_channel_get_ctx(ch);
	struct passthru_fsdev_io *io_ctx = (struct passthru_fsdev_io *)fsdev_io->driver_ctx;
	enum spdk_fsdev_io_type type = spdk_fsdev_io_get_type(fsdev_io);
	struct spdk_fsdev_io *child_io;

	child_io = malloc(spdk_fsdev_get_io_ctx_size());
	if (!child_io) {
		SPDK_ERRLOG("Cannot allocate child fsdev_io\n");
		spdk_fsdev_io_complete(fsdev_io, -ENOMEM);
		return;
	}

	/* Setup a per IO context value; we don't do anything with it in the vfsdev other
	 * than confirm we get the same thing back in the completion callback just to
	 * demonstrate.
	 */
	io_ctx->test = 0x5a;
	io_ctx->child_fsdev_io = child_io;

	spdk_fsdev_io_init(child_io, pt_node->base_desc, pt_ch->base_ch, spdk_fsdev_io_get_unique(fsdev_io),
			   type, spdk_fsdev_io_get_source_id(fsdev_io), spdk_fsdev_io_get_source_unique(fsdev_io),
			   vfsdev_passthru_cpl_cb, fsdev_io);

	memcpy(&child_io->u_in, &fsdev_io->u_in, sizeof(fsdev_io->u_in));

	spdk_fsdev_io_submit(child_io);
}

/* We supplied this as an entry point for upper layers who want to communicate to this
 * fsdev.  This is how they get a channel. We are passed the same context we provided when
 * we created our PT vfsdev in examine() which, for this fsdev, is the address of one of
 * our context nodes. From here we'll ask the SPDK channel code to fill out our channel
 * struct and we'll keep it in our PT node.
 */
static struct spdk_io_channel *
vfsdev_passthru_get_io_channel(void *ctx)
{
	struct vfsdev_passthru *pt_node = (struct vfsdev_passthru *)ctx;
	struct spdk_io_channel *pt_ch = NULL;

	/* The IO channel code will allocate a channel for us which consists of
	 * the SPDK channel structure plus the size of our pt_io_channel struct
	 * that we passed in when we registered our IO device. It will then call
	 * our channel create callback to populate any elements that we need to
	 * update.
	 */
	pt_ch = spdk_get_io_channel(pt_node);

	return pt_ch;
}

struct vfsdev_passthru_reset_ctx {
	spdk_fsdev_reset_done_cb cb;
	void *cb_arg;
};

static void
vfsdev_passthru_reset_completion_cb(struct spdk_fsdev_desc *desc, bool success, void *cb_arg)
{
    (void)desc;
	struct vfsdev_passthru_reset_ctx *reset_ctx = (struct vfsdev_passthru_reset_ctx *)cb_arg;

	reset_ctx->cb(reset_ctx->cb_arg, success ? 0 : -1);
	free(reset_ctx);
}

static int
vfsdev_passthru_reset(void *ctx, spdk_fsdev_reset_done_cb cb, void *cb_arg)
{
	struct vfsdev_passthru *pt_node = (struct vfsdev_passthru *)ctx;
	struct vfsdev_passthru_reset_ctx *reset_ctx;

	reset_ctx = (struct vfsdev_passthru_reset_ctx *)malloc(sizeof(*reset_ctx));
	if (!reset_ctx) {
		SPDK_ERRLOG("No memory to allocate reset context.\n");
		return -ENOMEM;
	}

	reset_ctx->cb = cb;
	reset_ctx->cb_arg = cb_arg;
	return spdk_fsdev_reset(pt_node->base_desc, vfsdev_passthru_reset_completion_cb, reset_ctx);
}

/* This is the output for fsdev_get_fsdevs() for this vfsdev */
static int
vfsdev_passthru_dump_info_json(void *ctx, struct spdk_json_write_ctx *w)
{
	struct vfsdev_passthru *pt_node = (struct vfsdev_passthru *)ctx;

	spdk_json_write_name(w, "passthru_external");
	spdk_json_write_object_begin(w);
	spdk_json_write_named_string(w, "name", spdk_fsdev_get_name(&pt_node->pt_fsdev));
	spdk_json_write_named_string(w, "base_fsdev_name", spdk_fsdev_get_name(pt_node->base_fsdev));
	spdk_json_write_object_end(w);

	return 0;
}

/* This is used to generate JSON that can configure this module to its current state. */
static int
vfsdev_passthru_config_json(struct spdk_json_write_ctx *w)
{
	struct vfsdev_passthru *pt_node;

	TAILQ_FOREACH(pt_node, &g_pt_nodes, link) {
		spdk_json_write_object_begin(w);
		spdk_json_write_named_string(w, "method", "construct_ext_passthru_fsdev");
		spdk_json_write_named_object_begin(w, "params");
		spdk_json_write_named_string(w, "base_fsdev_name", spdk_fsdev_get_name(pt_node->base_fsdev));
		spdk_json_write_named_string(w, "name", spdk_fsdev_get_name(&pt_node->pt_fsdev));
		spdk_json_write_object_end(w);
		spdk_json_write_object_end(w);
	}
	return 0;
}

/* We provide this callback for the SPDK channel code to create a channel using
 * the channel struct we provided in our module get_io_channel() entry point. Here
 * we get and save off an underlying base channel of the device below us so that
 * we can communicate with the base fsdev on a per channel basis.  If we needed
 * our own poller for this vfsdev, we'd register it here.
 */
static int
pt_fsdev_ch_create_cb(void *io_device, void *ctx_buf)
{
	struct pt_io_channel *pt_ch = ctx_buf;
	struct vfsdev_passthru *pt_node = io_device;

	pt_ch->base_ch = spdk_fsdev_get_io_channel(pt_node->base_desc);

	return 0;
}

/* We provide this callback for the SPDK channel code to destroy a channel
 * created with our create callback. We just need to undo anything we did
 * when we created. If this fsdev used its own poller, we'd unregister it here.
 */
static void
pt_fsdev_ch_destroy_cb(void *io_device, void *ctx_buf)
{
    (void)io_device;
	struct pt_io_channel *pt_ch = ctx_buf;

	spdk_put_io_channel(pt_ch->base_ch);
}

/* Create the passthru association from the base fsdev and passthru fsdev name and insert
 * on the global list. */
static int
vfsdev_passthru_insert_name(const char *base_name, const char *passthru_name)
{
	struct passthru_associations *assoc;

	TAILQ_FOREACH(assoc, &g_passthru_associations, link) {
		if (strcmp(passthru_name, assoc->passthru_name) == 0) {
			SPDK_ERRLOG("passthru fsdev %s already exists\n", passthru_name);
			return -EEXIST;
		}
	}

	assoc = calloc(1, sizeof(struct passthru_associations));
	if (!assoc) {
		SPDK_ERRLOG("could not allocate passthru_associations\n");
		return -ENOMEM;
	}

	assoc->base_name = strdup(base_name);
	if (!assoc->base_name) {
		SPDK_ERRLOG("could not allocate assoc->base_name\n");
		free(assoc);
		return -ENOMEM;
	}

	assoc->passthru_name = strdup(passthru_name);
	if (!assoc->passthru_name) {
		SPDK_ERRLOG("could not allocate assoc->passthru_name\n");
		free(assoc->base_name);
		free(assoc);
		return -ENOMEM;
	}

	TAILQ_INSERT_TAIL(&g_passthru_associations, assoc, link);

	return 0;
}

/* On init, just perform fsdev module specific initialization. */
static int
vfsdev_passthru_init(void)
{
	return 0;
}

/* Called when the entire module is being torn down. */
static void
vfsdev_passthru_finish(void)
{
	struct passthru_associations *assoc;

	while ((assoc = TAILQ_FIRST(&g_passthru_associations))) {
		TAILQ_REMOVE(&g_passthru_associations, assoc, link);
		free(assoc->base_name);
		free(assoc->passthru_name);
		free(assoc);
	}
}

/* During init we'll be asked how much memory we'd like passed to us
 * in fsdev_io structures as context. Here's where we specify how
 * much context we want per IO.
 */
static int
vfsdev_passthru_get_ctx_size(void)
{
	return sizeof(struct passthru_fsdev_io);
}

/* Where vfsdev_passthru_config_json() is used to generate per module JSON config data, this
 * function is called to output any per fsdev specific methods. For the PT module, there are
 * none.
 */
static void
vfsdev_passthru_write_config_json(struct spdk_fsdev *fsdev, struct spdk_json_write_ctx *w)
{
    (void)fsdev;
    (void)w;
	/* No config per fsdev needed */
}

/* When we register our fsdev this is how we specify our entry points. */
static const struct spdk_fsdev_fn_table vfsdev_passthru_fn_table = {
	.destruct		= vfsdev_passthru_destruct,
	.submit_request		= vfsdev_passthru_submit_request,
	.get_io_channel		= vfsdev_passthru_get_io_channel,
	.write_config_json	= vfsdev_passthru_write_config_json,
	.reset			= vfsdev_passthru_reset,
	.dump_info_json		= vfsdev_passthru_dump_info_json
};

static void
vfsdev_passthru_base_fsdev_hotremove_cb(struct spdk_fsdev *fsdev_find)
{
	struct vfsdev_passthru *pt_node, *tmp;

	TAILQ_FOREACH_SAFE(pt_node, &g_pt_nodes, link, tmp) {
		if (fsdev_find == pt_node->base_fsdev) {
			spdk_fsdev_unregister(&pt_node->pt_fsdev, NULL, NULL);
		}
	}
}

/* Called when the underlying base fsdev triggers asynchronous event such as fsdev removal. */
static void
vfsdev_passthru_base_fsdev_event_cb(enum spdk_fsdev_event_type type, struct spdk_fsdev *fsdev,
				    void *event_ctx)
{
    (void)event_ctx;
	switch (type) {
	case SPDK_FSDEV_EVENT_REMOVE:
		vfsdev_passthru_base_fsdev_hotremove_cb(fsdev);
		break;
	default:
		SPDK_NOTICELOG("Unsupported fsdev event: type %d\n", type);
		break;
	}
}

/* Create and register the passthru vfsdev if we find it in our list of fsdev names.
 * This can be called either by the examine path or RPC method.
 */
static int
vfsdev_passthru_register(const char *base_name)
{
	struct passthru_associations *assoc;
	struct vfsdev_passthru *pt_node;
	struct spdk_fsdev *fsdev;
	int rc = 0;

	/* Check our list of associations from config versus this fsdev and if
	 * there's a match, create the pt_node & fsdev accordingly.
	 */
	TAILQ_FOREACH(assoc, &g_passthru_associations, link) {
		if (strcmp(assoc->base_name, base_name) != 0) {
			continue;
		}

		SPDK_NOTICELOG("Match on %s\n", base_name);
		pt_node = calloc(1, sizeof(struct vfsdev_passthru));
		if (!pt_node) {
			rc = -ENOMEM;
			SPDK_ERRLOG("could not allocate pt_node\n");
			break;
		}

		pt_node->pt_fsdev.name = strdup(assoc->passthru_name);
		if (!pt_node->pt_fsdev.name) {
			rc = -ENOMEM;
			SPDK_ERRLOG("could not allocate pt_fsdev name\n");
			free(pt_node);
			break;
		}

		/* The base fsdev that we're attaching to. */
		rc = spdk_fsdev_open(base_name, vfsdev_passthru_base_fsdev_event_cb,
				     NULL, &pt_node->base_desc);
		if (rc) {
			if (rc != -ENODEV) {
				SPDK_ERRLOG("could not open fsdev %s\n", base_name);
			}
			free(pt_node->pt_fsdev.name);
			free(pt_node);
			break;
		}
		SPDK_NOTICELOG("base fsdev opened\n");

		fsdev = spdk_fsdev_desc_get_fsdev(pt_node->base_desc);
		pt_node->base_fsdev = fsdev;

		/* This is the context that is passed to us when the fsdev
		 * layer calls in so we'll save our pt_fsdev node here.
		 */
		pt_node->pt_fsdev.ctxt = pt_node;
		pt_node->pt_fsdev.fn_table = &vfsdev_passthru_fn_table;
		pt_node->pt_fsdev.module = &passthru_if;
		TAILQ_INSERT_TAIL(&g_pt_nodes, pt_node, link);

		spdk_io_device_register(pt_node, pt_fsdev_ch_create_cb, pt_fsdev_ch_destroy_cb,
					sizeof(struct pt_io_channel),
					assoc->passthru_name);
		SPDK_NOTICELOG("io_device created at: 0x%p\n", pt_node);

		/* Save the thread where the base device is opened */
		pt_node->thread = spdk_get_thread();

		rc = spdk_fsdev_register(&pt_node->pt_fsdev);
		if (rc) {
			SPDK_ERRLOG("could not register pt_fsdev\n");
			spdk_fsdev_close(pt_node->base_desc);
			TAILQ_REMOVE(&g_pt_nodes, pt_node, link);
			spdk_io_device_unregister(pt_node, NULL);
			free(pt_node->pt_fsdev.name);
			free(pt_node);
			break;
		}
		SPDK_NOTICELOG("ext_pt_fsdev registered\n");
		SPDK_NOTICELOG("created ext_pt_fsdev for: %s\n", assoc->passthru_name);
	}

	return rc;
}

/* Create the passthru fsdev from the given fsdev and vfsdev name. */
int
fsdev_passthru_external_create(const char *fsdev_name, const char *vfsdev_name)
{
	int rc;

	/* Insert the fsdev name into our global name list even if it doesn't exist yet,
	 * it may show up soon...
	 */
	rc = vfsdev_passthru_insert_name(fsdev_name, vfsdev_name);
	if (rc) {
		return rc;
	}

	rc = vfsdev_passthru_register(fsdev_name);
	if (rc == -ENODEV) {
		/* This is not an error, we tracked the name above and it still
		 * may show up later.
		 */
		SPDK_NOTICELOG("vfsdev creation deferred pending base fsdev arrival\n");
		rc = 0;
	}

	return rc;
}

void
fsdev_passthru_external_delete(const char *fsdev_name, spdk_fsdev_unregister_cb cb_fn,
			       void *cb_arg)
{
	struct passthru_associations *assoc;
	int rc;

	rc = spdk_fsdev_unregister_by_name(fsdev_name, &passthru_if, cb_fn, cb_arg);
	if (rc != 0) {
		cb_fn(cb_arg, rc);
		return;
	}

	/* Remove the association (passthru, base) from g_passthru_associations. This is required so that the
	 * passthru fsdev does not get re-created if the same base fsdev is constructed at some other time,
	 * unless the underlying fsdev was hot-removed.
	 */
	TAILQ_FOREACH(assoc, &g_passthru_associations, link) {
		if (strcmp(assoc->passthru_name, fsdev_name) == 0) {
			TAILQ_REMOVE(&g_passthru_associations, assoc, link);
			free(assoc->base_name);
			free(assoc->passthru_name);
			free(assoc);
			break;
		}
	}
}
