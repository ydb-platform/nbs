# Vendored libacl source

This directory contains the library sources and public headers from the
Ubuntu `acl` source package version `2.3.1-1`, matching `libacl1-dev` on
Ubuntu 22.04 (Jammy).

Source package:
`http://archive.ubuntu.com/ubuntu/pool/main/a/acl/acl_2.3.1-1.dsc`

Upstream archive SHA-256:
`c0234042e17f11306c23c038b08e5e070edb7be44bef6697fb8734dcff1c66b1`

The `ya.make` target builds the libacl API used by `fstest`. The two
`perm_copy_*` compatibility helpers are retained as source but are not built,
because they require libattr's `error_context` API and are not part of the
POSIX ACL API used by this suite.

