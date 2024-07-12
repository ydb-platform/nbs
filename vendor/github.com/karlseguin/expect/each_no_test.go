package expect

import (
    "testing"
)

type EachNoTests struct {
    called int
}

func Test_EachNo(t *testing.T) {
    Expectify(new(EachNoTests), t)
}

func (et *EachNoTests) DoesNotRunEach() {
    Expect(et.called).Less.Than(2)
}

func (et *EachNoTests) Each() {
    et.called++
    Expect(et.called).Less.Than(2)
}
