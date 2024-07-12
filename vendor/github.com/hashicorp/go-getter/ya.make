GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    checksum.go
    client.go
    client_mode.go
    client_option.go
    client_option_insecure.go
    client_option_progress.go
    common.go
    copy_dir.go
    decompress.go
    decompress_bzip2.go
    decompress_gzip.go
    decompress_tar.go
    decompress_tbz2.go
    decompress_testing.go
    decompress_tgz.go
    decompress_txz.go
    decompress_tzst.go
    decompress_xz.go
    decompress_zip.go
    decompress_zstd.go
    detect.go
    detect_bitbucket.go
    detect_file.go
    detect_gcs.go
    detect_git.go
    detect_github.go
    detect_gitlab.go
    detect_s3.go
    detect_ssh.go
    folder_storage.go
    get.go
    get_base.go
    get_file.go
    get_file_copy.go
    get_gcs.go
    get_git.go
    get_hg.go
    get_http.go
    get_mock.go
    get_s3.go
    netrc.go
    source.go
    storage.go
    url.go
)

GO_TEST_SRCS(
    client_option_progress_test.go
    decompress_bzip2_test.go
    decompress_gzip_test.go
    decompress_tar_test.go
    decompress_tbz2_test.go
    decompress_tgz_test.go
    decompress_txz_test.go
    decompress_tzst_test.go
    decompress_xz_test.go
    decompress_zip_test.go
    decompress_zstd_test.go
    detect_bitbucket_test.go
    detect_file_test.go
    detect_gcs_test.go
    detect_git_test.go
    detect_github_test.go
    detect_gitlab_test.go
    detect_s3_test.go
    detect_test.go
    folder_storage_test.go
    get_file_copy_test.go
    get_file_test.go
    get_gcs_test.go
    get_git_test.go
    get_hg_test.go
    get_http_test.go
    get_s3_test.go
    get_test.go
    module_test.go
    netrc_test.go
    source_test.go
    url_test.go
    util_test.go
)

IF (OS_LINUX)
    SRCS(
        get_file_unix.go
    )

    GO_TEST_SRCS(detect_file_unix_test.go)
ENDIF()

IF (OS_DARWIN)
    SRCS(
        get_file_unix.go
    )

    GO_TEST_SRCS(detect_file_unix_test.go)
ENDIF()

IF (OS_WINDOWS)
    SRCS(
        get_file_windows.go
    )
ENDIF()

END()

RECURSE(
    # gotest
    helper
)
