// Code generated by MockGen. DO NOT EDIT.
// Source: persistent_grpc.pb.go

// Package persistent is a generated GoMock package.
package persistent

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	grpc "google.golang.org/grpc"
	metadata "google.golang.org/grpc/metadata"
)

// MockPersistentSubscriptionsClient is a mock of PersistentSubscriptionsClient interface.
type MockPersistentSubscriptionsClient struct {
	ctrl     *gomock.Controller
	recorder *MockPersistentSubscriptionsClientMockRecorder
}

// MockPersistentSubscriptionsClientMockRecorder is the mock recorder for MockPersistentSubscriptionsClient.
type MockPersistentSubscriptionsClientMockRecorder struct {
	mock *MockPersistentSubscriptionsClient
}

// NewMockPersistentSubscriptionsClient creates a new mock instance.
func NewMockPersistentSubscriptionsClient(ctrl *gomock.Controller) *MockPersistentSubscriptionsClient {
	mock := &MockPersistentSubscriptionsClient{ctrl: ctrl}
	mock.recorder = &MockPersistentSubscriptionsClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockPersistentSubscriptionsClient) EXPECT() *MockPersistentSubscriptionsClientMockRecorder {
	return m.recorder
}

// Create mocks base method.
func (m *MockPersistentSubscriptionsClient) Create(ctx context.Context, in *CreateReq, opts ...grpc.CallOption) (*CreateResp, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Create", varargs...)
	ret0, _ := ret[0].(*CreateResp)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Create indicates an expected call of Create.
func (mr *MockPersistentSubscriptionsClientMockRecorder) Create(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Create", reflect.TypeOf((*MockPersistentSubscriptionsClient)(nil).Create), varargs...)
}

// Delete mocks base method.
func (m *MockPersistentSubscriptionsClient) Delete(ctx context.Context, in *DeleteReq, opts ...grpc.CallOption) (*DeleteResp, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Delete", varargs...)
	ret0, _ := ret[0].(*DeleteResp)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Delete indicates an expected call of Delete.
func (mr *MockPersistentSubscriptionsClientMockRecorder) Delete(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockPersistentSubscriptionsClient)(nil).Delete), varargs...)
}

// Read mocks base method.
func (m *MockPersistentSubscriptionsClient) Read(ctx context.Context, opts ...grpc.CallOption) (PersistentSubscriptions_ReadClient, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Read", varargs...)
	ret0, _ := ret[0].(PersistentSubscriptions_ReadClient)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Read indicates an expected call of Read.
func (mr *MockPersistentSubscriptionsClientMockRecorder) Read(ctx interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockPersistentSubscriptionsClient)(nil).Read), varargs...)
}

// Update mocks base method.
func (m *MockPersistentSubscriptionsClient) Update(ctx context.Context, in *UpdateReq, opts ...grpc.CallOption) (*UpdateResp, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{ctx, in}
	for _, a := range opts {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Update", varargs...)
	ret0, _ := ret[0].(*UpdateResp)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Update indicates an expected call of Update.
func (mr *MockPersistentSubscriptionsClientMockRecorder) Update(ctx, in interface{}, opts ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{ctx, in}, opts...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Update", reflect.TypeOf((*MockPersistentSubscriptionsClient)(nil).Update), varargs...)
}

// MockPersistentSubscriptions_ReadClient is a mock of PersistentSubscriptions_ReadClient interface.
type MockPersistentSubscriptions_ReadClient struct {
	ctrl     *gomock.Controller
	recorder *MockPersistentSubscriptions_ReadClientMockRecorder
}

// MockPersistentSubscriptions_ReadClientMockRecorder is the mock recorder for MockPersistentSubscriptions_ReadClient.
type MockPersistentSubscriptions_ReadClientMockRecorder struct {
	mock *MockPersistentSubscriptions_ReadClient
}

// NewMockPersistentSubscriptions_ReadClient creates a new mock instance.
func NewMockPersistentSubscriptions_ReadClient(ctrl *gomock.Controller) *MockPersistentSubscriptions_ReadClient {
	mock := &MockPersistentSubscriptions_ReadClient{ctrl: ctrl}
	mock.recorder = &MockPersistentSubscriptions_ReadClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockPersistentSubscriptions_ReadClient) EXPECT() *MockPersistentSubscriptions_ReadClientMockRecorder {
	return m.recorder
}

// CloseSend mocks base method.
func (m *MockPersistentSubscriptions_ReadClient) CloseSend() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CloseSend")
	ret0, _ := ret[0].(error)
	return ret0
}

// CloseSend indicates an expected call of CloseSend.
func (mr *MockPersistentSubscriptions_ReadClientMockRecorder) CloseSend() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CloseSend", reflect.TypeOf((*MockPersistentSubscriptions_ReadClient)(nil).CloseSend))
}

// Context mocks base method.
func (m *MockPersistentSubscriptions_ReadClient) Context() context.Context {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Context")
	ret0, _ := ret[0].(context.Context)
	return ret0
}

// Context indicates an expected call of Context.
func (mr *MockPersistentSubscriptions_ReadClientMockRecorder) Context() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Context", reflect.TypeOf((*MockPersistentSubscriptions_ReadClient)(nil).Context))
}

// Header mocks base method.
func (m *MockPersistentSubscriptions_ReadClient) Header() (metadata.MD, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Header")
	ret0, _ := ret[0].(metadata.MD)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Header indicates an expected call of Header.
func (mr *MockPersistentSubscriptions_ReadClientMockRecorder) Header() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Header", reflect.TypeOf((*MockPersistentSubscriptions_ReadClient)(nil).Header))
}

// Recv mocks base method.
func (m *MockPersistentSubscriptions_ReadClient) Recv() (*ReadResp, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Recv")
	ret0, _ := ret[0].(*ReadResp)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Recv indicates an expected call of Recv.
func (mr *MockPersistentSubscriptions_ReadClientMockRecorder) Recv() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Recv", reflect.TypeOf((*MockPersistentSubscriptions_ReadClient)(nil).Recv))
}

// RecvMsg mocks base method.
func (m_2 *MockPersistentSubscriptions_ReadClient) RecvMsg(m interface{}) error {
	m_2.ctrl.T.Helper()
	ret := m_2.ctrl.Call(m_2, "RecvMsg", m)
	ret0, _ := ret[0].(error)
	return ret0
}

// RecvMsg indicates an expected call of RecvMsg.
func (mr *MockPersistentSubscriptions_ReadClientMockRecorder) RecvMsg(m interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecvMsg", reflect.TypeOf((*MockPersistentSubscriptions_ReadClient)(nil).RecvMsg), m)
}

// Send mocks base method.
func (m *MockPersistentSubscriptions_ReadClient) Send(arg0 *ReadReq) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Send", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Send indicates an expected call of Send.
func (mr *MockPersistentSubscriptions_ReadClientMockRecorder) Send(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Send", reflect.TypeOf((*MockPersistentSubscriptions_ReadClient)(nil).Send), arg0)
}

// SendMsg mocks base method.
func (m_2 *MockPersistentSubscriptions_ReadClient) SendMsg(m interface{}) error {
	m_2.ctrl.T.Helper()
	ret := m_2.ctrl.Call(m_2, "SendMsg", m)
	ret0, _ := ret[0].(error)
	return ret0
}

// SendMsg indicates an expected call of SendMsg.
func (mr *MockPersistentSubscriptions_ReadClientMockRecorder) SendMsg(m interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendMsg", reflect.TypeOf((*MockPersistentSubscriptions_ReadClient)(nil).SendMsg), m)
}

// Trailer mocks base method.
func (m *MockPersistentSubscriptions_ReadClient) Trailer() metadata.MD {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Trailer")
	ret0, _ := ret[0].(metadata.MD)
	return ret0
}

// Trailer indicates an expected call of Trailer.
func (mr *MockPersistentSubscriptions_ReadClientMockRecorder) Trailer() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Trailer", reflect.TypeOf((*MockPersistentSubscriptions_ReadClient)(nil).Trailer))
}

// MockPersistentSubscriptionsServer is a mock of PersistentSubscriptionsServer interface.
type MockPersistentSubscriptionsServer struct {
	ctrl     *gomock.Controller
	recorder *MockPersistentSubscriptionsServerMockRecorder
}

// MockPersistentSubscriptionsServerMockRecorder is the mock recorder for MockPersistentSubscriptionsServer.
type MockPersistentSubscriptionsServerMockRecorder struct {
	mock *MockPersistentSubscriptionsServer
}

// NewMockPersistentSubscriptionsServer creates a new mock instance.
func NewMockPersistentSubscriptionsServer(ctrl *gomock.Controller) *MockPersistentSubscriptionsServer {
	mock := &MockPersistentSubscriptionsServer{ctrl: ctrl}
	mock.recorder = &MockPersistentSubscriptionsServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockPersistentSubscriptionsServer) EXPECT() *MockPersistentSubscriptionsServerMockRecorder {
	return m.recorder
}

// Create mocks base method.
func (m *MockPersistentSubscriptionsServer) Create(arg0 context.Context, arg1 *CreateReq) (*CreateResp, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Create", arg0, arg1)
	ret0, _ := ret[0].(*CreateResp)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Create indicates an expected call of Create.
func (mr *MockPersistentSubscriptionsServerMockRecorder) Create(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Create", reflect.TypeOf((*MockPersistentSubscriptionsServer)(nil).Create), arg0, arg1)
}

// Delete mocks base method.
func (m *MockPersistentSubscriptionsServer) Delete(arg0 context.Context, arg1 *DeleteReq) (*DeleteResp, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete", arg0, arg1)
	ret0, _ := ret[0].(*DeleteResp)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Delete indicates an expected call of Delete.
func (mr *MockPersistentSubscriptionsServerMockRecorder) Delete(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockPersistentSubscriptionsServer)(nil).Delete), arg0, arg1)
}

// Read mocks base method.
func (m *MockPersistentSubscriptionsServer) Read(arg0 PersistentSubscriptions_ReadServer) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Read", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Read indicates an expected call of Read.
func (mr *MockPersistentSubscriptionsServerMockRecorder) Read(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockPersistentSubscriptionsServer)(nil).Read), arg0)
}

// Update mocks base method.
func (m *MockPersistentSubscriptionsServer) Update(arg0 context.Context, arg1 *UpdateReq) (*UpdateResp, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Update", arg0, arg1)
	ret0, _ := ret[0].(*UpdateResp)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Update indicates an expected call of Update.
func (mr *MockPersistentSubscriptionsServerMockRecorder) Update(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Update", reflect.TypeOf((*MockPersistentSubscriptionsServer)(nil).Update), arg0, arg1)
}

// mustEmbedUnimplementedPersistentSubscriptionsServer mocks base method.
func (m *MockPersistentSubscriptionsServer) mustEmbedUnimplementedPersistentSubscriptionsServer() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "mustEmbedUnimplementedPersistentSubscriptionsServer")
}

// mustEmbedUnimplementedPersistentSubscriptionsServer indicates an expected call of mustEmbedUnimplementedPersistentSubscriptionsServer.
func (mr *MockPersistentSubscriptionsServerMockRecorder) mustEmbedUnimplementedPersistentSubscriptionsServer() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "mustEmbedUnimplementedPersistentSubscriptionsServer", reflect.TypeOf((*MockPersistentSubscriptionsServer)(nil).mustEmbedUnimplementedPersistentSubscriptionsServer))
}

// MockUnsafePersistentSubscriptionsServer is a mock of UnsafePersistentSubscriptionsServer interface.
type MockUnsafePersistentSubscriptionsServer struct {
	ctrl     *gomock.Controller
	recorder *MockUnsafePersistentSubscriptionsServerMockRecorder
}

// MockUnsafePersistentSubscriptionsServerMockRecorder is the mock recorder for MockUnsafePersistentSubscriptionsServer.
type MockUnsafePersistentSubscriptionsServerMockRecorder struct {
	mock *MockUnsafePersistentSubscriptionsServer
}

// NewMockUnsafePersistentSubscriptionsServer creates a new mock instance.
func NewMockUnsafePersistentSubscriptionsServer(ctrl *gomock.Controller) *MockUnsafePersistentSubscriptionsServer {
	mock := &MockUnsafePersistentSubscriptionsServer{ctrl: ctrl}
	mock.recorder = &MockUnsafePersistentSubscriptionsServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockUnsafePersistentSubscriptionsServer) EXPECT() *MockUnsafePersistentSubscriptionsServerMockRecorder {
	return m.recorder
}

// mustEmbedUnimplementedPersistentSubscriptionsServer mocks base method.
func (m *MockUnsafePersistentSubscriptionsServer) mustEmbedUnimplementedPersistentSubscriptionsServer() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "mustEmbedUnimplementedPersistentSubscriptionsServer")
}

// mustEmbedUnimplementedPersistentSubscriptionsServer indicates an expected call of mustEmbedUnimplementedPersistentSubscriptionsServer.
func (mr *MockUnsafePersistentSubscriptionsServerMockRecorder) mustEmbedUnimplementedPersistentSubscriptionsServer() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "mustEmbedUnimplementedPersistentSubscriptionsServer", reflect.TypeOf((*MockUnsafePersistentSubscriptionsServer)(nil).mustEmbedUnimplementedPersistentSubscriptionsServer))
}

// MockPersistentSubscriptions_ReadServer is a mock of PersistentSubscriptions_ReadServer interface.
type MockPersistentSubscriptions_ReadServer struct {
	ctrl     *gomock.Controller
	recorder *MockPersistentSubscriptions_ReadServerMockRecorder
}

// MockPersistentSubscriptions_ReadServerMockRecorder is the mock recorder for MockPersistentSubscriptions_ReadServer.
type MockPersistentSubscriptions_ReadServerMockRecorder struct {
	mock *MockPersistentSubscriptions_ReadServer
}

// NewMockPersistentSubscriptions_ReadServer creates a new mock instance.
func NewMockPersistentSubscriptions_ReadServer(ctrl *gomock.Controller) *MockPersistentSubscriptions_ReadServer {
	mock := &MockPersistentSubscriptions_ReadServer{ctrl: ctrl}
	mock.recorder = &MockPersistentSubscriptions_ReadServerMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockPersistentSubscriptions_ReadServer) EXPECT() *MockPersistentSubscriptions_ReadServerMockRecorder {
	return m.recorder
}

// Context mocks base method.
func (m *MockPersistentSubscriptions_ReadServer) Context() context.Context {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Context")
	ret0, _ := ret[0].(context.Context)
	return ret0
}

// Context indicates an expected call of Context.
func (mr *MockPersistentSubscriptions_ReadServerMockRecorder) Context() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Context", reflect.TypeOf((*MockPersistentSubscriptions_ReadServer)(nil).Context))
}

// Recv mocks base method.
func (m *MockPersistentSubscriptions_ReadServer) Recv() (*ReadReq, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Recv")
	ret0, _ := ret[0].(*ReadReq)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Recv indicates an expected call of Recv.
func (mr *MockPersistentSubscriptions_ReadServerMockRecorder) Recv() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Recv", reflect.TypeOf((*MockPersistentSubscriptions_ReadServer)(nil).Recv))
}

// RecvMsg mocks base method.
func (m_2 *MockPersistentSubscriptions_ReadServer) RecvMsg(m interface{}) error {
	m_2.ctrl.T.Helper()
	ret := m_2.ctrl.Call(m_2, "RecvMsg", m)
	ret0, _ := ret[0].(error)
	return ret0
}

// RecvMsg indicates an expected call of RecvMsg.
func (mr *MockPersistentSubscriptions_ReadServerMockRecorder) RecvMsg(m interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "RecvMsg", reflect.TypeOf((*MockPersistentSubscriptions_ReadServer)(nil).RecvMsg), m)
}

// Send mocks base method.
func (m *MockPersistentSubscriptions_ReadServer) Send(arg0 *ReadResp) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Send", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Send indicates an expected call of Send.
func (mr *MockPersistentSubscriptions_ReadServerMockRecorder) Send(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Send", reflect.TypeOf((*MockPersistentSubscriptions_ReadServer)(nil).Send), arg0)
}

// SendHeader mocks base method.
func (m *MockPersistentSubscriptions_ReadServer) SendHeader(arg0 metadata.MD) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SendHeader", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SendHeader indicates an expected call of SendHeader.
func (mr *MockPersistentSubscriptions_ReadServerMockRecorder) SendHeader(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendHeader", reflect.TypeOf((*MockPersistentSubscriptions_ReadServer)(nil).SendHeader), arg0)
}

// SendMsg mocks base method.
func (m_2 *MockPersistentSubscriptions_ReadServer) SendMsg(m interface{}) error {
	m_2.ctrl.T.Helper()
	ret := m_2.ctrl.Call(m_2, "SendMsg", m)
	ret0, _ := ret[0].(error)
	return ret0
}

// SendMsg indicates an expected call of SendMsg.
func (mr *MockPersistentSubscriptions_ReadServerMockRecorder) SendMsg(m interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SendMsg", reflect.TypeOf((*MockPersistentSubscriptions_ReadServer)(nil).SendMsg), m)
}

// SetHeader mocks base method.
func (m *MockPersistentSubscriptions_ReadServer) SetHeader(arg0 metadata.MD) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SetHeader", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// SetHeader indicates an expected call of SetHeader.
func (mr *MockPersistentSubscriptions_ReadServerMockRecorder) SetHeader(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetHeader", reflect.TypeOf((*MockPersistentSubscriptions_ReadServer)(nil).SetHeader), arg0)
}

// SetTrailer mocks base method.
func (m *MockPersistentSubscriptions_ReadServer) SetTrailer(arg0 metadata.MD) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "SetTrailer", arg0)
}

// SetTrailer indicates an expected call of SetTrailer.
func (mr *MockPersistentSubscriptions_ReadServerMockRecorder) SetTrailer(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SetTrailer", reflect.TypeOf((*MockPersistentSubscriptions_ReadServer)(nil).SetTrailer), arg0)
}
