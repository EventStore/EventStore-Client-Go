package persistent

import (
	"errors"
	"fmt"
)

type ErrorCode string

type Error struct {
	err  error
	code ErrorCode
}

func (r Error) Error() string {
	if r.err != nil {
		return fmt.Sprintf("Error code: %s. Reason %v", r.code, r.err)
	}

	return fmt.Sprintf("Error code: %s", r.code)
}

func (r Error) Code() ErrorCode {
	return r.code
}

func NewErrorCodeMsg(code ErrorCode, msg ...interface{}) Error {
	return Error{code: code, err: errors.New(fmt.Sprint(msg...))}
}

func NewErrorCode(code ErrorCode) Error {
	return Error{
		code: code,
	}
}

func NewError(code ErrorCode, err error) Error {
	return Error{
		code: code,
		err:  err,
	}
}
