"""Bounded single-frame decoder using only the stable public libzstd API."""

from cpython.bytes cimport PyBytes_AS_STRING, PyBytes_FromStringAndSize
from libc.stddef cimport size_t


cdef extern from "zstd.h":
    unsigned long long ZSTD_CONTENTSIZE_UNKNOWN
    unsigned long long ZSTD_CONTENTSIZE_ERROR
    unsigned long long ZSTD_getFrameContentSize(const void* src, size_t src_size)
    size_t ZSTD_findFrameCompressedSize(const void* src, size_t src_size)
    size_t ZSTD_decompress(void* dst, size_t dst_capacity, const void* src, size_t src_size)
    unsigned int ZSTD_isError(size_t code)


def decompress(bytes data, size_t expected_size):
    cdef size_t input_size = len(data)
    cdef const char* source = PyBytes_AS_STRING(data)
    cdef size_t frame_size
    cdef unsigned long long content_size
    cdef bytes output
    cdef size_t decoded_size

    if expected_size == 0 or expected_size > 4 * 1024 * 1024:
        raise ValueError("Invalid expected Zstandard size")
    if input_size == 0 or input_size > 8 * 1024 * 1024:
        raise ValueError("Invalid compressed Zstandard size")
    frame_size = ZSTD_findFrameCompressedSize(source, input_size)
    if ZSTD_isError(frame_size) or frame_size != input_size:
        raise ValueError("Expected exactly one complete Zstandard frame")
    content_size = ZSTD_getFrameContentSize(source, input_size)
    if content_size == ZSTD_CONTENTSIZE_ERROR:
        raise ValueError("Invalid Zstandard frame")
    if content_size != ZSTD_CONTENTSIZE_UNKNOWN and content_size != expected_size:
        raise ValueError("Unexpected Zstandard frame size")

    # The public one-shot API writes into this fixed-size buffer. A corrupt or
    # oversized frame cannot make us allocate its advertised decompressed size.
    output = PyBytes_FromStringAndSize(NULL, expected_size)
    decoded_size = ZSTD_decompress(PyBytes_AS_STRING(output), expected_size, source, input_size)
    if ZSTD_isError(decoded_size) or decoded_size != expected_size:
        raise ValueError("Zstandard decompression failed or returned the wrong size")
    return output
