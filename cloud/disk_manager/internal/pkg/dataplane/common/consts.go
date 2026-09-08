package common

////////////////////////////////////////////////////////////////////////////////

// DefaultChunkSize is the legacy snapshot chunk size and the maximum payload
// size of an NBS read/write request, in bytes. Larger snapshot chunks are split
// into requests of at most this size.
const DefaultChunkSize = 4 * 1024 * 1024
