package common

import (
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/common/protos"
	"google.golang.org/protobuf/proto"
)

////////////////////////////////////////////////////////////////////////////////

func MarshalStrings(values []string) []byte {
	strings := &protos.Strings{
		Values: values,
	}

	bytes, err := proto.Marshal(strings)
	if err != nil {
		// TODO: Throw an error.
		return nil
	}

	return bytes
}

func UnmarshalStrings(bytes []byte) ([]string, error) {
	strings := &protos.Strings{}

	err := proto.Unmarshal(bytes, strings)
	if err != nil {
		return []string{}, err
	}

	return strings.Values, nil
}

func MarshalStringMap(values map[string]string) []byte {
	m := &protos.StringMap{
		Values: values,
	}

	bytes, err := proto.Marshal(m)
	if err != nil {
		// TODO: Throw an error.
		return nil
	}

	return bytes
}

func UnmarshalStringMap(bytes []byte) (map[string]string, error) {
	m := &protos.StringMap{}

	err := proto.Unmarshal(bytes, m)
	if err != nil {
		return map[string]string{}, err
	}

	return m.Values, nil
}

func MarshalInts(values []int64) []byte {
	ints := &protos.Ints{
		Values: values,
	}

	bytes, err := proto.Marshal(ints)
	if err != nil {
		// TODO: Throw an error.
		return nil
	}

	return bytes
}

func UnmarshalInts(bytes []byte) ([]int64, error) {
	ints := &protos.Ints{}

	err := proto.Unmarshal(bytes, ints)
	if err != nil {
		return []int64{}, err
	}

	return ints.Values, nil
}
