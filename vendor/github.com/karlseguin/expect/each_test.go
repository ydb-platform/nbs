package expect

import (
    "testing"
)

type EachTests struct {
    called bool
}

func Test_Each(t *testing.T) {
    Expectify(new(EachTests), t)
}

func (e *EachTests) RunsEach() {
    Expect(e.called).To.Equal(true)
}

func (e *EachTests) Each(f func()) {
    e.called = true
    f()
}
