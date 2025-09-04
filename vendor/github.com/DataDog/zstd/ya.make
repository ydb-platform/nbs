GO_LIBRARY()

LICENSE(BSD-3-Clause)

VERSION(v1.5.2)

PEERDIR(contrib/libs/zstd)

ADDINCL(contrib/libs/zstd/include)

SRCS(
    # cover.c
    # debug.c
    # divsufsort.c
    # entropy_common.c
    # error_private.c
    # fastcover.c
    # fse_compress.c
    # fse_decompress.c
    # hist.c
    # huf_compress.c
    # huf_decompress.c
    # pool.c
    # threading.c
    # xxhash.c
    # zbuff_common.c
    # zbuff_compress.c
    # zbuff_decompress.c
    # zdict.c
    # zstd_common.c
    # zstd_compress.c
    # zstd_compress_literals.c
    # zstd_compress_sequences.c
    # zstd_compress_superblock.c
    # zstd_ddict.c
    # zstd_decompress.c
    # zstd_decompress_block.c
    # zstd_double_fast.c
    # zstd_fast.c
    # zstd_lazy.c
    # zstd_ldm.c
    # zstd_opt.c
    # zstd_v01.c
    # zstd_v02.c
    # zstd_v03.c
    # zstd_v04.c
    # zstd_v05.c
    # zstd_v06.c
    # zstd_v07.c
    # zstdmt_compress.c
)

GO_TEST_SRCS(
    errors_test.go
    helpers_test.go
    zstd_bullk_test.go
    zstd_ctx_test.go
    zstd_stream_test.go
    zstd_test.go
)

IF (CGO_ENABLED)
    CGO_SRCS(
        errors.go
        zstd.go
        zstd_bulk.go
        zstd_ctx.go
        zstd_stream.go
    )
ENDIF()

END()

RECURSE(gotest)
