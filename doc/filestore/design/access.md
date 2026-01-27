## How access control works in filestore

For each node, we store `Uid`, `Gid`, and `Mode`. These can be interpreted in
the same way as traditional Unix file permissions.

### Where does `Mode` come from?

`Mode` is propagated from the guest to the tablet upon file creation and upon
`setattr` calls (see [FUSE lowlevel API](https://libfuse.github.io/doxygen/structfuse__lowlevel__ops.html)
for details):

* `void(*   mknod )(fuse_req_t req, fuse_ino_t parent, const char *name, `**`mode_t mode`**`, dev_t rdev)`
* `void(*   mkdir )(fuse_req_t req, fuse_ino_t parent, const char *name, `**`mode_t mode`**`)`
* `void(*   create )(fuse_req_t req, fuse_ino_t parent, const char *name, `**`mode_t mode`**`, struct fuse_file_info *fi)`
* `void(*   setattr )(fuse_req_t req, fuse_ino_t ino, `**`struct stat *attr`**`, int to_set, struct fuse_file_info *fi)`

Here, the mode is passed from the FUSE request to the service request: https://github.com/ydb-platform/nbs/blob/0428b498eb664bc28a81b7bf4c393b53517098b0/cloud/filestore/libs/vfs_fuse/fs_impl_node.cpp#L197.

### Where do `Uid` and `Gid` come from?

`Uid` and `Gid` are set by vhost from the `fuse_ctx->uid`, `fuse_ctx->gid` here:
https://github.com/ydb-platform/nbs/blob/0428b498eb664bc28a81b7bf4c393b53517098b0/cloud/filestore/libs/vfs_fuse/fs_impl.h#L374-L378.


### How does the guest kernel treat `Uid` and `Gid`?

To delegate access checking from the low-level FUSE API implementation to the
kernel, one can pass the `default_permissions` option. This is exactly what is
done by the virtiofs driver here: https://github.com/torvalds/linux/blob/372800cb95a35a7c40a07e2e0f7de4ce6786d230/fs/fuse/virtio_fs.c#L1531C7-L1531C26.

This way, the kernel will check access permissions based on the `Uid`, `Gid`,
and `Mode` stored in the filestore. Thus, if two VMs share the same filestore
and have the same UID shared by two different users in two VMs, they will be
able to access each other's files. The same applies to groups.

### `security.capability` xattr

`security.capability` is an extended attribute that stores granular capabilities
for a file. A process executing the file is granted these capabilities in
addition to the normal user/group/other permissions.

### `setgid` and `setuid` bits

When the `setgid` bit is set on a file, a process executing that file will have
its group ID set to the group ID of the file, rather than the group ID of the
user who executed the file.

When set on a directory, new files and subdirectories created within that
directory inherit the group ID of the directory, rather than the primary group
ID of the user who created the file. Similarly, when the `setuid` bit is set on
a file, a process executing that file will have its user ID set to the user ID
of the file owner.

### `KILLPRIV` and `KILLPRIV_V2` logic

The existence of inheritance of uid/gid from parent directories and the
existence of `setuid`/`setgid` bits creates a possibility for a following
attack: one can edit a file with `setuid`/`setgid` bits and thus gain elevated
privileges for their malicious code.

To prevent this, the kernel will drop `setuid`/`setgid` bits as well as
`security.capability` xattr upon each write/chown/truncate (see this code:
https://github.com/torvalds/linux/blob/0f61b1860cc3f52aef9036d7235ed1f017632193/fs/fuse/dir.c#L2321-L2327). Note that this might hinder the performance of write-heavy workloads (see https://github.com/ydb-platform/nbs/issues/3045
for more details).

To prevent this from happening, two FUSE flags exist:

* https://github.com/ydb-platform/nbs/blob/ddf2d2104d34d1a12e81eb5779a1cf212203c92c/contrib/libs/linux-headers/linux/fuse.h#L345
* https://github.com/ydb-platform/nbs/blob/ddf2d2104d34d1a12e81eb5779a1cf212203c92c/contrib/libs/linux-headers/linux/fuse.h#L356-L360

We pass the first flag by default here: https://github.com/ydb-platform/nbs/blob/d0d677eeb47087381560d7997cc4e92eea466d03/cloud/filestore/libs/vhost/request.h#L148,
and the second flag is passed under a feature flag here: https://github.com/ydb-platform/nbs/blob/d0d677eeb47087381560d7997cc4e92eea466d03/cloud/filestore/config/storage.proto#L677.

When these flags are set, the kernel *will not* drop `setuid`/`setgid` bits and
 `security.capability` xattr upon writes/truncates/chowns. Instead, the
 implementation of the lowlevel FUSE API is responsible for handling these cases
 by handling `FUSE_OPEN_KILL_SUIDGID` on open, `FATTR_KILL_SUIDGID` on setattr,
 `FUSE_WRITE_KILL_SUIDGID` on write (see [FUSE lowlevel API](https://libfuse.github.io/doxygen/include_2fuse__common_8h.html#:~:text=%E2%97%86-,FUSE_CAP_HANDLE_KILLPRIV_V2,-%23define%20FUSE_CAP_HANDLE_KILLPRIV_V2%C2%A0%C2%A0%C2%A0)
 for details).

### `setgid` and `setuid` bits on directories implementation

When creating a new file inside a directory, the kernel will set the group of
the new file to the group of the directory if the `setgid` bit is set on the
directory. Similarly, if the `setuid` bit is set on the directory, the user of
 the new file will be set to the user of the directory.

In ext4 and other filesystems, this group propagation is implemented in the
kernel function called `inode_init_owner`: https://github.com/torvalds/linux/blob/0f61b1860cc3f52aef9036d7235ed1f017632193/fs/inode.c#L2638-L2639.

This function is called in a lot of filesystem drivers:

* ext4: https://github.com/torvalds/linux/blob/0f61b1860cc3f52aef9036d7235ed1f017632193/fs/ext4/ialloc.c#L980
* jfs: https://github.com/torvalds/linux/blob/0f61b1860cc3f52aef9036d7235ed1f017632193/fs/jfs/jfs_inode.c#L67
* xfs: https://github.com/torvalds/linux/blob/0f61b1860cc3f52aef9036d7235ed1f017632193/fs/xfs/libxfs/xfs_inode_util.c#L300

However, virtiofs does not implement this logic. Thus, this group propagation is
implemented on the filestore-server side, where the `setgid` bit is treated in
the same way upon file creation. This logic is enabled only under the
`GidPropagationEnabled` flag.
