// Code generated by MockGen. DO NOT EDIT.
// Source: event_reader_factory.go

// Package persistent is a generated GoMock package.
package persistent

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	persistent "github.com/pivonroll/EventStore-Client-Go/protos/persistent"
)

// MockeventReaderFactory is a mock of eventReaderFactory interface.
type MockeventReaderFactory struct {
	ctrl     *gomock.Controller
	recorder *MockeventReaderFactoryMockRecorder
}

// MockeventReaderFactoryMockRecorder is the mock recorder for MockeventReaderFactory.
type MockeventReaderFactoryMockRecorder struct {
	mock *MockeventReaderFactory
}

// NewMockeventReaderFactory creates a new mock instance.
func NewMockeventReaderFactory(ctrl *gomock.Controller) *MockeventReaderFactory {
	mock := &MockeventReaderFactory{ctrl: ctrl}
	mock.recorder = &MockeventReaderFactoryMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockeventReaderFactory) EXPECT() *MockeventReaderFactoryMockRecorder {
	return m.recorder
}

// Create mocks base method.
func (m *MockeventReaderFactory) Create(client persistent.PersistentSubscriptions_ReadClient, subscriptionId string, messageAdapter messageAdapter, cancel context.CancelFunc) EventReader {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Create", client, subscriptionId, messageAdapter, cancel)
	ret0, _ := ret[0].(EventReader)
	return ret0
}

// Create indicates an expected call of Create.
func (mr *MockeventReaderFactoryMockRecorder) Create(client, subscriptionId, messageAdapter, cancel interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Create", reflect.TypeOf((*MockeventReaderFactory)(nil).Create), client, subscriptionId, messageAdapter, cancel)
}