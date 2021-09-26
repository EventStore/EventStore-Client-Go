// Code generated by MockGen. DO NOT EDIT.
// Source: reader_helper.go

// Package buffered_async is a generated GoMock package.
package buffered_async

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	errors "github.com/pivonroll/EventStore-Client-Go/errors"
)

// MockreaderHelper is a mock of readerHelper interface.
type MockreaderHelper struct {
	ctrl     *gomock.Controller
	recorder *MockreaderHelperMockRecorder
}

// MockreaderHelperMockRecorder is the mock recorder for MockreaderHelper.
type MockreaderHelperMockRecorder struct {
	mock *MockreaderHelper
}

// NewMockreaderHelper creates a new mock instance.
func NewMockreaderHelper(ctrl *gomock.Controller) *MockreaderHelper {
	mock := &MockreaderHelper{ctrl: ctrl}
	mock.recorder = &MockreaderHelperMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockreaderHelper) EXPECT() *MockreaderHelperMockRecorder {
	return m.recorder
}

// Read mocks base method.
func (m *MockreaderHelper) Read() (interface{}, errors.Error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Read")
	ret0, _ := ret[0].(interface{})
	ret1, _ := ret[1].(errors.Error)
	return ret0, ret1
}

// Read indicates an expected call of Read.
func (mr *MockreaderHelperMockRecorder) Read() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockreaderHelper)(nil).Read))
}