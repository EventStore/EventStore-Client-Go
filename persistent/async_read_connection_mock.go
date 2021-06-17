// Code generated by MockGen. DO NOT EDIT.
// Source: async_read_connection.go

// Package persistent is a generated GoMock package.
package persistent

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
)

// MockAsyncReadConnection is a mock of AsyncReadConnection interface.
type MockAsyncReadConnection struct {
	ctrl     *gomock.Controller
	recorder *MockAsyncReadConnectionMockRecorder
}

// MockAsyncReadConnectionMockRecorder is the mock recorder for MockAsyncReadConnection.
type MockAsyncReadConnectionMockRecorder struct {
	mock *MockAsyncReadConnection
}

// NewMockAsyncReadConnection creates a new mock instance.
func NewMockAsyncReadConnection(ctrl *gomock.Controller) *MockAsyncReadConnection {
	mock := &MockAsyncReadConnection{ctrl: ctrl}
	mock.recorder = &MockAsyncReadConnectionMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockAsyncReadConnection) EXPECT() *MockAsyncReadConnectionMockRecorder {
	return m.recorder
}

// RegisterHandler mocks base method.
func (m *MockAsyncReadConnection) RegisterHandler(handler EventAppearedHandler) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "RegisterHandler", handler)
}

// RegisterHandler indicates an expected call of RegisterHandler.
func (mr *MockAsyncReadConnectionMockRecorder) RegisterHandler(handler interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RegisterHandler", reflect.TypeOf((*MockAsyncReadConnection)(nil).RegisterHandler), handler)
}

// Start mocks base method.
func (m *MockAsyncReadConnection) Start(retryCount uint8) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Start", retryCount)
	ret0, _ := ret[0].(error)
	return ret0
}

// Start indicates an expected call of Start.
func (mr *MockAsyncReadConnectionMockRecorder) Start(retryCount interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Start", reflect.TypeOf((*MockAsyncReadConnection)(nil).Start), retryCount)
}

// Stop mocks base method.
func (m *MockAsyncReadConnection) Stop() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Stop")
}

// Stop indicates an expected call of Stop.
func (mr *MockAsyncReadConnectionMockRecorder) Stop() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stop", reflect.TypeOf((*MockAsyncReadConnection)(nil).Stop))
}