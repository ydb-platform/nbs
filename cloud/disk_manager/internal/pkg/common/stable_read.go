package common

////////////////////////////////////////////////////////////////////////////////

type stableReadDecision int

const (
	// Content equals the current one.
	stableReadUnchanged stableReadDecision = iota
	// Content has been read for the first time or differs from the content
	// read previously.
	stableReadWait
	// Content has been read unchanged twice in a row and can be applied.
	stableReadApply
)

// Guards against picking up a file that is still being rewritten: new content
// is applied only after two consecutive reads return it unchanged. The caller
// must space the reads apart: with one read per period a change takes effect
// within two periods. A best effort only: a writer that stalls longer than the
// interval between reads leaves a partial file that gets applied.
type stableRead[T comparable] struct {
	pending    T
	hasPending bool
}

func (r *stableRead[T]) observe(current T, content T) stableReadDecision {
	if content == current {
		r.reset()
		return stableReadUnchanged
	}

	if !r.hasPending || r.pending != content {
		r.pending = content
		r.hasPending = true
		return stableReadWait
	}

	return stableReadApply
}

// Forgets the pending content, e.g. after a read error: the count starts over
// when the content is read again.
func (r *stableRead[T]) reset() {
	var empty T
	r.pending = empty
	r.hasPending = false
}
