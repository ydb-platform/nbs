## nbsd-lightweight

Blockstore server version without KiKiMR (YDB BlobStorage) dependencies. This daemon creates volumes on top of local FS. Can be used for debugging purposes - e.g. debugging the layers over the storage layer - endpoints, nbd, vhost, etc. Can be build much faster than nbsd. The resulting binary is much smaller as well.
