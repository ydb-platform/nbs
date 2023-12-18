package expect

import (
	"reflect"
)

func SameKind(a, b interface{}) (reflect.Kind, bool) {
	aKind := reflect.ValueOf(a).Kind()
	bKind := reflect.ValueOf(b).Kind()
	if aKind == bKind {
		return aKind, true
	}

	if aKind == reflect.Slice && bKind == reflect.String {
		return reflect.Slice, true
	}
	if aKind == reflect.String && bKind == reflect.Slice {
		return reflect.String, true
	}

	return aKind, false
}

func IsNumeric(a interface{}) bool {
	if IsInt(a) || IsUint(a) {
		return true
	}
	kind := reflect.ValueOf(a).Kind()
	return kind == reflect.Float32 || kind == reflect.Float64
}

func IsNil(a interface{}) bool {
	if a == nil {
		return true
	}
	value := reflect.ValueOf(a)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface:
		fallthrough
	case reflect.Ptr, reflect.Map, reflect.Slice:
		return value.IsNil()
	}
	return false
}

func IsInt(a interface{}) bool {
	kind := reflect.ValueOf(a).Kind()
	return kind == reflect.Int || kind == reflect.Int8 || kind == reflect.Int16 || kind == reflect.Int32 || kind == reflect.Int64
}

func IsUint(a interface{}) bool {
	kind := reflect.ValueOf(a).Kind()
	return kind == reflect.Uint || kind == reflect.Uint8 || kind == reflect.Uint16 || kind == reflect.Uint32 || kind == reflect.Uint64
}

func ToInt64(a, b interface{}) (interface{}, interface{}) {
	return reflect.ValueOf(a).Int(), reflect.ValueOf(b).Int()
}

func ToUint64(a, b interface{}) (interface{}, interface{}) {
	return reflect.ValueOf(a).Uint(), reflect.ValueOf(b).Uint()
}

func IsString(a interface{}) bool {
	return reflect.ValueOf(a).Kind() == reflect.String
}

func IsSlice(a interface{}) bool {
	return reflect.ValueOf(a).Kind() == reflect.Slice
}

func ToString(a interface{}) string {
	return reflect.ValueOf(a).String()
}
