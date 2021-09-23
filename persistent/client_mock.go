// Code generated by MockGen. DO NOT EDIT.
// Source: client.go

// Package persistent is a generated GoMock package.
package persistent

import (
	context "context"
	reflect "reflect"

	errors "github.com/EventStore/EventStore-Client-Go/errors"
	gomock "github.com/golang/mock/gomock"
)

// MockClient is a mock of Client interface.
type MockClient struct {
	ctrl     *gomock.Controller
	recorder *MockClientMockRecorder
}

// MockClientMockRecorder is the mock recorder for MockClient.
type MockClientMockRecorder struct {
	mock *MockClient
}

// NewMockClient creates a new mock instance.
func NewMockClient(ctrl *gomock.Controller) *MockClient {
	mock := &MockClient{ctrl: ctrl}
	mock.recorder = &MockClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockClient) EXPECT() *MockClientMockRecorder {
	return m.recorder
}

// CreateAllSubscription mocks base method.
func (m *MockClient) CreateAllSubscription(ctx context.Context, request CreateAllRequest) errors.Error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateAllSubscription", ctx, request)
	ret0, _ := ret[0].(errors.Error)
	return ret0
}

// CreateAllSubscription indicates an expected call of CreateAllSubscription.
func (mr *MockClientMockRecorder) CreateAllSubscription(ctx, request interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateAllSubscription", reflect.TypeOf((*MockClient)(nil).CreateAllSubscription), ctx, request)
}

// CreateStreamSubscription mocks base method.
func (m *MockClient) CreateStreamSubscription(ctx context.Context, request CreateOrUpdateStreamRequest) errors.Error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateStreamSubscription", ctx, request)
	ret0, _ := ret[0].(errors.Error)
	return ret0
}

// CreateStreamSubscription indicates an expected call of CreateStreamSubscription.
func (mr *MockClientMockRecorder) CreateStreamSubscription(ctx, request interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateStreamSubscription", reflect.TypeOf((*MockClient)(nil).CreateStreamSubscription), ctx, request)
}

// DeleteAllSubscription mocks base method.
func (m *MockClient) DeleteAllSubscription(ctx context.Context, groupName string) errors.Error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteAllSubscription", ctx, groupName)
	ret0, _ := ret[0].(errors.Error)
	return ret0
}

// DeleteAllSubscription indicates an expected call of DeleteAllSubscription.
func (mr *MockClientMockRecorder) DeleteAllSubscription(ctx, groupName interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteAllSubscription", reflect.TypeOf((*MockClient)(nil).DeleteAllSubscription), ctx, groupName)
}

// DeleteStreamSubscription mocks base method.
func (m *MockClient) DeleteStreamSubscription(ctx context.Context, deleteOptions DeleteRequest) errors.Error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteStreamSubscription", ctx, deleteOptions)
	ret0, _ := ret[0].(errors.Error)
	return ret0
}

// DeleteStreamSubscription indicates an expected call of DeleteStreamSubscription.
func (mr *MockClientMockRecorder) DeleteStreamSubscription(ctx, deleteOptions interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteStreamSubscription", reflect.TypeOf((*MockClient)(nil).DeleteStreamSubscription), ctx, deleteOptions)
}

// SubscribeToStreamSync mocks base method.
func (m *MockClient) SubscribeToStreamSync(ctx context.Context, bufferSize int32, groupName, streamName string) (SyncReadConnection, errors.Error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SubscribeToStreamSync", ctx, bufferSize, groupName, streamName)
	ret0, _ := ret[0].(SyncReadConnection)
	ret1, _ := ret[1].(errors.Error)
	return ret0, ret1
}

// SubscribeToStreamSync indicates an expected call of SubscribeToStreamSync.
func (mr *MockClientMockRecorder) SubscribeToStreamSync(ctx, bufferSize, groupName, streamName interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SubscribeToStreamSync", reflect.TypeOf((*MockClient)(nil).SubscribeToStreamSync), ctx, bufferSize, groupName, streamName)
}

// UpdateAllSubscription mocks base method.
func (m *MockClient) UpdateAllSubscription(ctx context.Context, request UpdateAllRequest) errors.Error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateAllSubscription", ctx, request)
	ret0, _ := ret[0].(errors.Error)
	return ret0
}

// UpdateAllSubscription indicates an expected call of UpdateAllSubscription.
func (mr *MockClientMockRecorder) UpdateAllSubscription(ctx, request interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateAllSubscription", reflect.TypeOf((*MockClient)(nil).UpdateAllSubscription), ctx, request)
}

// UpdateStreamSubscription mocks base method.
func (m *MockClient) UpdateStreamSubscription(ctx context.Context, request CreateOrUpdateStreamRequest) errors.Error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateStreamSubscription", ctx, request)
	ret0, _ := ret[0].(errors.Error)
	return ret0
}

// UpdateStreamSubscription indicates an expected call of UpdateStreamSubscription.
func (mr *MockClientMockRecorder) UpdateStreamSubscription(ctx, request interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateStreamSubscription", reflect.TypeOf((*MockClient)(nil).UpdateStreamSubscription), ctx, request)
}
