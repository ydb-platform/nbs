package expect

import (
	"reflect"
	"time"
)

const (
	Equals comparitorType = iota
	NotEqual
	LessThan
	LessThanOrEqual
	GreaterThan
	GreaterThanOrEqual
)

type comparitorType int
type comparitor func(k reflect.Kind, a, b interface{}) bool
type resolver func(a, b interface{}) bool

var (
	Comparitors = map[reflect.Kind]map[comparitorType]resolver{
		reflect.Int64: map[comparitorType]resolver{
			LessThan: func(a, b interface{}) bool { return a.(int64) < b.(int64) },
		},
		reflect.Uint64: map[comparitorType]resolver{
			LessThan: func(a, b interface{}) bool { return a.(uint64) < b.(uint64) },
		},
		reflect.Float32: map[comparitorType]resolver{
			LessThan: func(a, b interface{}) bool { return a.(float32) < b.(float32) },
		},
		reflect.Float64: map[comparitorType]resolver{
			LessThan: func(a, b interface{}) bool { return a.(float64) < b.(float64) },
		},
	}
)

func EqualsComparitor(k reflect.Kind, a, b interface{}) bool {
	return reflect.DeepEqual(a, b)
}

func NotEqualsComparitor(k reflect.Kind, a, b interface{}) bool {
	return !EqualsComparitor(k, a, b)
}

func LessThanComparitor(k reflect.Kind, a, b interface{}) bool {
	if t, ok := a.(time.Time); ok {
		return t.Before(b.(time.Time))
	}
	return Comparitors[k][LessThan](a, b)
}

func LessThanOrEqualToComparitor(k reflect.Kind, a, b interface{}) bool {
	if t, ok := a.(time.Time); ok {
		bt := b.(time.Time)
		return t.Before(bt) || t == bt
	}
	return EqualsComparitor(k, a, b) || Comparitors[k][LessThan](a, b)
}

func GreaterThanComparitor(k reflect.Kind, a, b interface{}) bool {
	if t, ok := a.(time.Time); ok {
		return t.After(b.(time.Time))
	}
	return !EqualsComparitor(k, a, b) && !Comparitors[k][LessThan](a, b)
}

func GreaterOrEqualToComparitor(k reflect.Kind, a, b interface{}) bool {
	if t, ok := a.(time.Time); ok {
		bt := b.(time.Time)
		return t.After(bt) || t == bt
	}
	return !Comparitors[k][LessThan](a, b)
}
