package assertpb

import (
	"fmt"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/stretchr/testify/require"
)

type fakeT struct {
	Msg string
}

func (f *fakeT) Errorf(format string, args ...interface{}) {
	f.Msg = fmt.Sprintf(format, args...)
}

func (f *fakeT) FailNow() {
}

func (f *fakeT) Helper() {
}

var _ TestingT = (*fakeT)(nil)

func TestEqual(t *testing.T) {
	ts := ptypes.TimestampNow()
	tsCopy := proto.Clone(ts).(*timestamp.Timestamp)
	_, _ = proto.Marshal(ts)

	var fake fakeT
	require.True(t, Equal(&fake, ts, tsCopy))
}

func TestNotEqual(t *testing.T) {
	var (
		a = &timestamp.Timestamp{Seconds: 1, Nanos: 10}
		b = &timestamp.Timestamp{Seconds: 2, Nanos: 15}
	)

	var fake fakeT
	require.False(t, Equal(&fake, a, b))
}

func TestSlices(t *testing.T) {
	var (
		a = &timestamp.Timestamp{Seconds: 1, Nanos: 10}
		b = &timestamp.Timestamp{Seconds: 2, Nanos: 15}
	)

	var fake fakeT
	require.True(t, Equal(&fake, []*timestamp.Timestamp{a, b}, []*timestamp.Timestamp{a, b}))
	require.False(t, Equal(&fake, []*timestamp.Timestamp{a, b}, []*timestamp.Timestamp{b, a}))
}
