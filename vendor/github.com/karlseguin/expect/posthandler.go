package expect

import (
	"fmt"
	"strings"
)

var (
	SuccessHandler        = &SuccessPostHandler{}
	FailureHandlerFactory = func(expected, actual interface{}) PostHandler {
		return &FailurePostHandler{expected, actual}
	}
)

type PostHandler interface {
	Message(format string, args ...interface{})
}

type FailurePostHandler struct {
	expected interface{}
	actual   interface{}
}

func NewFailureHandler(expected, actual interface{}) PostHandler {
	return FailureHandlerFactory(expected, actual)
}

func (h *FailurePostHandler) Message(format string, args ...interface{}) {
	if args == nil {
		s := fmt.Sprintf(format, h.expected, h.actual)
		if strings.Contains(s, "%!(EXTRA") == false {
			runner.ErrorMessage(s)
			return
		}
	}
	runner.ErrorMessage(format, args...)
}

type SuccessPostHandler struct {
}

func (h *SuccessPostHandler) Message(format string, args ...interface{}) {

}
