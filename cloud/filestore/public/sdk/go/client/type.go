package client

import (
	"reflect"
)

////////////////////////////////////////////////////////////////////////////////

func underlyingTypeName(obj interface{}) string {
	objType := reflect.TypeOf(obj)
	if objType.Kind() == reflect.Ptr {
		return objType.Elem().Name()
	} else {
		return objType.Name()
	}
}
