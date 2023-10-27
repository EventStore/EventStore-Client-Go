// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.31.0
// 	protoc        v3.20.0
// source: shared.proto

package shared

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type UUID struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to Value:
	//
	//	*UUID_Structured_
	//	*UUID_String_
	Value isUUID_Value `protobuf_oneof:"value"`
}

func (x *UUID) Reset() {
	*x = UUID{}
	if protoimpl.UnsafeEnabled {
		mi := &file_shared_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *UUID) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UUID) ProtoMessage() {}

func (x *UUID) ProtoReflect() protoreflect.Message {
	mi := &file_shared_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UUID.ProtoReflect.Descriptor instead.
func (*UUID) Descriptor() ([]byte, []int) {
	return file_shared_proto_rawDescGZIP(), []int{0}
}

func (m *UUID) GetValue() isUUID_Value {
	if m != nil {
		return m.Value
	}
	return nil
}

func (x *UUID) GetStructured() *UUID_Structured {
	if x, ok := x.GetValue().(*UUID_Structured_); ok {
		return x.Structured
	}
	return nil
}

func (x *UUID) GetString_() string {
	if x, ok := x.GetValue().(*UUID_String_); ok {
		return x.String_
	}
	return ""
}

type isUUID_Value interface {
	isUUID_Value()
}

type UUID_Structured_ struct {
	Structured *UUID_Structured `protobuf:"bytes,1,opt,name=structured,proto3,oneof"`
}

type UUID_String_ struct {
	String_ string `protobuf:"bytes,2,opt,name=string,proto3,oneof"`
}

func (*UUID_Structured_) isUUID_Value() {}

func (*UUID_String_) isUUID_Value() {}

type Empty struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *Empty) Reset() {
	*x = Empty{}
	if protoimpl.UnsafeEnabled {
		mi := &file_shared_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Empty) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Empty) ProtoMessage() {}

func (x *Empty) ProtoReflect() protoreflect.Message {
	mi := &file_shared_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Empty.ProtoReflect.Descriptor instead.
func (*Empty) Descriptor() ([]byte, []int) {
	return file_shared_proto_rawDescGZIP(), []int{1}
}

type StreamIdentifier struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	StreamName []byte `protobuf:"bytes,3,opt,name=stream_name,json=streamName,proto3" json:"stream_name,omitempty"`
}

func (x *StreamIdentifier) Reset() {
	*x = StreamIdentifier{}
	if protoimpl.UnsafeEnabled {
		mi := &file_shared_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StreamIdentifier) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StreamIdentifier) ProtoMessage() {}

func (x *StreamIdentifier) ProtoReflect() protoreflect.Message {
	mi := &file_shared_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StreamIdentifier.ProtoReflect.Descriptor instead.
func (*StreamIdentifier) Descriptor() ([]byte, []int) {
	return file_shared_proto_rawDescGZIP(), []int{2}
}

func (x *StreamIdentifier) GetStreamName() []byte {
	if x != nil {
		return x.StreamName
	}
	return nil
}

type AllStreamPosition struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	CommitPosition  uint64 `protobuf:"varint,1,opt,name=commit_position,json=commitPosition,proto3" json:"commit_position,omitempty"`
	PreparePosition uint64 `protobuf:"varint,2,opt,name=prepare_position,json=preparePosition,proto3" json:"prepare_position,omitempty"`
}

func (x *AllStreamPosition) Reset() {
	*x = AllStreamPosition{}
	if protoimpl.UnsafeEnabled {
		mi := &file_shared_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AllStreamPosition) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AllStreamPosition) ProtoMessage() {}

func (x *AllStreamPosition) ProtoReflect() protoreflect.Message {
	mi := &file_shared_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AllStreamPosition.ProtoReflect.Descriptor instead.
func (*AllStreamPosition) Descriptor() ([]byte, []int) {
	return file_shared_proto_rawDescGZIP(), []int{3}
}

func (x *AllStreamPosition) GetCommitPosition() uint64 {
	if x != nil {
		return x.CommitPosition
	}
	return 0
}

func (x *AllStreamPosition) GetPreparePosition() uint64 {
	if x != nil {
		return x.PreparePosition
	}
	return 0
}

type WrongExpectedVersion struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to CurrentStreamRevisionOption:
	//
	//	*WrongExpectedVersion_CurrentStreamRevision
	//	*WrongExpectedVersion_CurrentNoStream
	CurrentStreamRevisionOption isWrongExpectedVersion_CurrentStreamRevisionOption `protobuf_oneof:"current_stream_revision_option"`
	// Types that are assignable to ExpectedStreamPositionOption:
	//
	//	*WrongExpectedVersion_ExpectedStreamPosition
	//	*WrongExpectedVersion_ExpectedAny
	//	*WrongExpectedVersion_ExpectedStreamExists
	//	*WrongExpectedVersion_ExpectedNoStream
	ExpectedStreamPositionOption isWrongExpectedVersion_ExpectedStreamPositionOption `protobuf_oneof:"expected_stream_position_option"`
}

func (x *WrongExpectedVersion) Reset() {
	*x = WrongExpectedVersion{}
	if protoimpl.UnsafeEnabled {
		mi := &file_shared_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *WrongExpectedVersion) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WrongExpectedVersion) ProtoMessage() {}

func (x *WrongExpectedVersion) ProtoReflect() protoreflect.Message {
	mi := &file_shared_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WrongExpectedVersion.ProtoReflect.Descriptor instead.
func (*WrongExpectedVersion) Descriptor() ([]byte, []int) {
	return file_shared_proto_rawDescGZIP(), []int{4}
}

func (m *WrongExpectedVersion) GetCurrentStreamRevisionOption() isWrongExpectedVersion_CurrentStreamRevisionOption {
	if m != nil {
		return m.CurrentStreamRevisionOption
	}
	return nil
}

func (x *WrongExpectedVersion) GetCurrentStreamRevision() uint64 {
	if x, ok := x.GetCurrentStreamRevisionOption().(*WrongExpectedVersion_CurrentStreamRevision); ok {
		return x.CurrentStreamRevision
	}
	return 0
}

func (x *WrongExpectedVersion) GetCurrentNoStream() *emptypb.Empty {
	if x, ok := x.GetCurrentStreamRevisionOption().(*WrongExpectedVersion_CurrentNoStream); ok {
		return x.CurrentNoStream
	}
	return nil
}

func (m *WrongExpectedVersion) GetExpectedStreamPositionOption() isWrongExpectedVersion_ExpectedStreamPositionOption {
	if m != nil {
		return m.ExpectedStreamPositionOption
	}
	return nil
}

func (x *WrongExpectedVersion) GetExpectedStreamPosition() uint64 {
	if x, ok := x.GetExpectedStreamPositionOption().(*WrongExpectedVersion_ExpectedStreamPosition); ok {
		return x.ExpectedStreamPosition
	}
	return 0
}

func (x *WrongExpectedVersion) GetExpectedAny() *emptypb.Empty {
	if x, ok := x.GetExpectedStreamPositionOption().(*WrongExpectedVersion_ExpectedAny); ok {
		return x.ExpectedAny
	}
	return nil
}

func (x *WrongExpectedVersion) GetExpectedStreamExists() *emptypb.Empty {
	if x, ok := x.GetExpectedStreamPositionOption().(*WrongExpectedVersion_ExpectedStreamExists); ok {
		return x.ExpectedStreamExists
	}
	return nil
}

func (x *WrongExpectedVersion) GetExpectedNoStream() *emptypb.Empty {
	if x, ok := x.GetExpectedStreamPositionOption().(*WrongExpectedVersion_ExpectedNoStream); ok {
		return x.ExpectedNoStream
	}
	return nil
}

type isWrongExpectedVersion_CurrentStreamRevisionOption interface {
	isWrongExpectedVersion_CurrentStreamRevisionOption()
}

type WrongExpectedVersion_CurrentStreamRevision struct {
	CurrentStreamRevision uint64 `protobuf:"varint,1,opt,name=current_stream_revision,json=currentStreamRevision,proto3,oneof"`
}

type WrongExpectedVersion_CurrentNoStream struct {
	CurrentNoStream *emptypb.Empty `protobuf:"bytes,2,opt,name=current_no_stream,json=currentNoStream,proto3,oneof"`
}

func (*WrongExpectedVersion_CurrentStreamRevision) isWrongExpectedVersion_CurrentStreamRevisionOption() {
}

func (*WrongExpectedVersion_CurrentNoStream) isWrongExpectedVersion_CurrentStreamRevisionOption() {}

type isWrongExpectedVersion_ExpectedStreamPositionOption interface {
	isWrongExpectedVersion_ExpectedStreamPositionOption()
}

type WrongExpectedVersion_ExpectedStreamPosition struct {
	ExpectedStreamPosition uint64 `protobuf:"varint,3,opt,name=expected_stream_position,json=expectedStreamPosition,proto3,oneof"`
}

type WrongExpectedVersion_ExpectedAny struct {
	ExpectedAny *emptypb.Empty `protobuf:"bytes,4,opt,name=expected_any,json=expectedAny,proto3,oneof"`
}

type WrongExpectedVersion_ExpectedStreamExists struct {
	ExpectedStreamExists *emptypb.Empty `protobuf:"bytes,5,opt,name=expected_stream_exists,json=expectedStreamExists,proto3,oneof"`
}

type WrongExpectedVersion_ExpectedNoStream struct {
	ExpectedNoStream *emptypb.Empty `protobuf:"bytes,6,opt,name=expected_no_stream,json=expectedNoStream,proto3,oneof"`
}

func (*WrongExpectedVersion_ExpectedStreamPosition) isWrongExpectedVersion_ExpectedStreamPositionOption() {
}

func (*WrongExpectedVersion_ExpectedAny) isWrongExpectedVersion_ExpectedStreamPositionOption() {}

func (*WrongExpectedVersion_ExpectedStreamExists) isWrongExpectedVersion_ExpectedStreamPositionOption() {
}

func (*WrongExpectedVersion_ExpectedNoStream) isWrongExpectedVersion_ExpectedStreamPositionOption() {}

type AccessDenied struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *AccessDenied) Reset() {
	*x = AccessDenied{}
	if protoimpl.UnsafeEnabled {
		mi := &file_shared_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *AccessDenied) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*AccessDenied) ProtoMessage() {}

func (x *AccessDenied) ProtoReflect() protoreflect.Message {
	mi := &file_shared_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use AccessDenied.ProtoReflect.Descriptor instead.
func (*AccessDenied) Descriptor() ([]byte, []int) {
	return file_shared_proto_rawDescGZIP(), []int{5}
}

type StreamDeleted struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	StreamIdentifier *StreamIdentifier `protobuf:"bytes,1,opt,name=stream_identifier,json=streamIdentifier,proto3" json:"stream_identifier,omitempty"`
}

func (x *StreamDeleted) Reset() {
	*x = StreamDeleted{}
	if protoimpl.UnsafeEnabled {
		mi := &file_shared_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StreamDeleted) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StreamDeleted) ProtoMessage() {}

func (x *StreamDeleted) ProtoReflect() protoreflect.Message {
	mi := &file_shared_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StreamDeleted.ProtoReflect.Descriptor instead.
func (*StreamDeleted) Descriptor() ([]byte, []int) {
	return file_shared_proto_rawDescGZIP(), []int{6}
}

func (x *StreamDeleted) GetStreamIdentifier() *StreamIdentifier {
	if x != nil {
		return x.StreamIdentifier
	}
	return nil
}

type Timeout struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *Timeout) Reset() {
	*x = Timeout{}
	if protoimpl.UnsafeEnabled {
		mi := &file_shared_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Timeout) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Timeout) ProtoMessage() {}

func (x *Timeout) ProtoReflect() protoreflect.Message {
	mi := &file_shared_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Timeout.ProtoReflect.Descriptor instead.
func (*Timeout) Descriptor() ([]byte, []int) {
	return file_shared_proto_rawDescGZIP(), []int{7}
}

type Unknown struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *Unknown) Reset() {
	*x = Unknown{}
	if protoimpl.UnsafeEnabled {
		mi := &file_shared_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Unknown) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Unknown) ProtoMessage() {}

func (x *Unknown) ProtoReflect() protoreflect.Message {
	mi := &file_shared_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Unknown.ProtoReflect.Descriptor instead.
func (*Unknown) Descriptor() ([]byte, []int) {
	return file_shared_proto_rawDescGZIP(), []int{8}
}

type InvalidTransaction struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *InvalidTransaction) Reset() {
	*x = InvalidTransaction{}
	if protoimpl.UnsafeEnabled {
		mi := &file_shared_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *InvalidTransaction) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*InvalidTransaction) ProtoMessage() {}

func (x *InvalidTransaction) ProtoReflect() protoreflect.Message {
	mi := &file_shared_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use InvalidTransaction.ProtoReflect.Descriptor instead.
func (*InvalidTransaction) Descriptor() ([]byte, []int) {
	return file_shared_proto_rawDescGZIP(), []int{9}
}

type MaximumAppendSizeExceeded struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	MaxAppendSize uint32 `protobuf:"varint,1,opt,name=maxAppendSize,proto3" json:"maxAppendSize,omitempty"`
}

func (x *MaximumAppendSizeExceeded) Reset() {
	*x = MaximumAppendSizeExceeded{}
	if protoimpl.UnsafeEnabled {
		mi := &file_shared_proto_msgTypes[10]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *MaximumAppendSizeExceeded) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*MaximumAppendSizeExceeded) ProtoMessage() {}

func (x *MaximumAppendSizeExceeded) ProtoReflect() protoreflect.Message {
	mi := &file_shared_proto_msgTypes[10]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use MaximumAppendSizeExceeded.ProtoReflect.Descriptor instead.
func (*MaximumAppendSizeExceeded) Descriptor() ([]byte, []int) {
	return file_shared_proto_rawDescGZIP(), []int{10}
}

func (x *MaximumAppendSizeExceeded) GetMaxAppendSize() uint32 {
	if x != nil {
		return x.MaxAppendSize
	}
	return 0
}

type BadRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Message string `protobuf:"bytes,1,opt,name=message,proto3" json:"message,omitempty"`
}

func (x *BadRequest) Reset() {
	*x = BadRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_shared_proto_msgTypes[11]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BadRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BadRequest) ProtoMessage() {}

func (x *BadRequest) ProtoReflect() protoreflect.Message {
	mi := &file_shared_proto_msgTypes[11]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BadRequest.ProtoReflect.Descriptor instead.
func (*BadRequest) Descriptor() ([]byte, []int) {
	return file_shared_proto_rawDescGZIP(), []int{11}
}

func (x *BadRequest) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

type UUID_Structured struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	MostSignificantBits  int64 `protobuf:"varint,1,opt,name=most_significant_bits,json=mostSignificantBits,proto3" json:"most_significant_bits,omitempty"`
	LeastSignificantBits int64 `protobuf:"varint,2,opt,name=least_significant_bits,json=leastSignificantBits,proto3" json:"least_significant_bits,omitempty"`
}

func (x *UUID_Structured) Reset() {
	*x = UUID_Structured{}
	if protoimpl.UnsafeEnabled {
		mi := &file_shared_proto_msgTypes[12]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *UUID_Structured) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UUID_Structured) ProtoMessage() {}

func (x *UUID_Structured) ProtoReflect() protoreflect.Message {
	mi := &file_shared_proto_msgTypes[12]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UUID_Structured.ProtoReflect.Descriptor instead.
func (*UUID_Structured) Descriptor() ([]byte, []int) {
	return file_shared_proto_rawDescGZIP(), []int{0, 0}
}

func (x *UUID_Structured) GetMostSignificantBits() int64 {
	if x != nil {
		return x.MostSignificantBits
	}
	return 0
}

func (x *UUID_Structured) GetLeastSignificantBits() int64 {
	if x != nil {
		return x.LeastSignificantBits
	}
	return 0
}

var File_shared_proto protoreflect.FileDescriptor

var file_shared_proto_rawDesc = []byte{
	0x0a, 0x0c, 0x73, 0x68, 0x61, 0x72, 0x65, 0x64, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x12,
	0x65, 0x76, 0x65, 0x6e, 0x74, 0x5f, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2e, 0x63, 0x6c, 0x69, 0x65,
	0x6e, 0x74, 0x1a, 0x1b, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x75, 0x66, 0x2f, 0x65, 0x6d, 0x70, 0x74, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22,
	0xe8, 0x01, 0x0a, 0x04, 0x55, 0x55, 0x49, 0x44, 0x12, 0x45, 0x0a, 0x0a, 0x73, 0x74, 0x72, 0x75,
	0x63, 0x74, 0x75, 0x72, 0x65, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x23, 0x2e, 0x65,
	0x76, 0x65, 0x6e, 0x74, 0x5f, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x2e, 0x63, 0x6c, 0x69, 0x65, 0x6e,
	0x74, 0x2e, 0x55, 0x55, 0x49, 0x44, 0x2e, 0x53, 0x74, 0x72, 0x75, 0x63, 0x74, 0x75, 0x72, 0x65,
	0x64, 0x48, 0x00, 0x52, 0x0a, 0x73, 0x74, 0x72, 0x75, 0x63, 0x74, 0x75, 0x72, 0x65, 0x64, 0x12,
	0x18, 0x0a, 0x06, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x48,
	0x00, 0x52, 0x06, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x1a, 0x76, 0x0a, 0x0a, 0x53, 0x74, 0x72,
	0x75, 0x63, 0x74, 0x75, 0x72, 0x65, 0x64, 0x12, 0x32, 0x0a, 0x15, 0x6d, 0x6f, 0x73, 0x74, 0x5f,
	0x73, 0x69, 0x67, 0x6e, 0x69, 0x66, 0x69, 0x63, 0x61, 0x6e, 0x74, 0x5f, 0x62, 0x69, 0x74, 0x73,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x13, 0x6d, 0x6f, 0x73, 0x74, 0x53, 0x69, 0x67, 0x6e,
	0x69, 0x66, 0x69, 0x63, 0x61, 0x6e, 0x74, 0x42, 0x69, 0x74, 0x73, 0x12, 0x34, 0x0a, 0x16, 0x6c,
	0x65, 0x61, 0x73, 0x74, 0x5f, 0x73, 0x69, 0x67, 0x6e, 0x69, 0x66, 0x69, 0x63, 0x61, 0x6e, 0x74,
	0x5f, 0x62, 0x69, 0x74, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x03, 0x52, 0x14, 0x6c, 0x65, 0x61,
	0x73, 0x74, 0x53, 0x69, 0x67, 0x6e, 0x69, 0x66, 0x69, 0x63, 0x61, 0x6e, 0x74, 0x42, 0x69, 0x74,
	0x73, 0x42, 0x07, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x07, 0x0a, 0x05, 0x45, 0x6d,
	0x70, 0x74, 0x79, 0x22, 0x39, 0x0a, 0x10, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x64, 0x65,
	0x6e, 0x74, 0x69, 0x66, 0x69, 0x65, 0x72, 0x12, 0x1f, 0x0a, 0x0b, 0x73, 0x74, 0x72, 0x65, 0x61,
	0x6d, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x0a, 0x73, 0x74,
	0x72, 0x65, 0x61, 0x6d, 0x4e, 0x61, 0x6d, 0x65, 0x4a, 0x04, 0x08, 0x01, 0x10, 0x03, 0x22, 0x67,
	0x0a, 0x11, 0x41, 0x6c, 0x6c, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x50, 0x6f, 0x73, 0x69, 0x74,
	0x69, 0x6f, 0x6e, 0x12, 0x27, 0x0a, 0x0f, 0x63, 0x6f, 0x6d, 0x6d, 0x69, 0x74, 0x5f, 0x70, 0x6f,
	0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0e, 0x63, 0x6f,
	0x6d, 0x6d, 0x69, 0x74, 0x50, 0x6f, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x29, 0x0a, 0x10,
	0x70, 0x72, 0x65, 0x70, 0x61, 0x72, 0x65, 0x5f, 0x70, 0x6f, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x0f, 0x70, 0x72, 0x65, 0x70, 0x61, 0x72, 0x65, 0x50,
	0x6f, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0xec, 0x03, 0x0a, 0x14, 0x57, 0x72, 0x6f, 0x6e,
	0x67, 0x45, 0x78, 0x70, 0x65, 0x63, 0x74, 0x65, 0x64, 0x56, 0x65, 0x72, 0x73, 0x69, 0x6f, 0x6e,
	0x12, 0x38, 0x0a, 0x17, 0x63, 0x75, 0x72, 0x72, 0x65, 0x6e, 0x74, 0x5f, 0x73, 0x74, 0x72, 0x65,
	0x61, 0x6d, 0x5f, 0x72, 0x65, 0x76, 0x69, 0x73, 0x69, 0x6f, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x04, 0x48, 0x00, 0x52, 0x15, 0x63, 0x75, 0x72, 0x72, 0x65, 0x6e, 0x74, 0x53, 0x74, 0x72, 0x65,
	0x61, 0x6d, 0x52, 0x65, 0x76, 0x69, 0x73, 0x69, 0x6f, 0x6e, 0x12, 0x44, 0x0a, 0x11, 0x63, 0x75,
	0x72, 0x72, 0x65, 0x6e, 0x74, 0x5f, 0x6e, 0x6f, 0x5f, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x48, 0x00, 0x52,
	0x0f, 0x63, 0x75, 0x72, 0x72, 0x65, 0x6e, 0x74, 0x4e, 0x6f, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d,
	0x12, 0x3a, 0x0a, 0x18, 0x65, 0x78, 0x70, 0x65, 0x63, 0x74, 0x65, 0x64, 0x5f, 0x73, 0x74, 0x72,
	0x65, 0x61, 0x6d, 0x5f, 0x70, 0x6f, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x03, 0x20, 0x01,
	0x28, 0x04, 0x48, 0x01, 0x52, 0x16, 0x65, 0x78, 0x70, 0x65, 0x63, 0x74, 0x65, 0x64, 0x53, 0x74,
	0x72, 0x65, 0x61, 0x6d, 0x50, 0x6f, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x3b, 0x0a, 0x0c,
	0x65, 0x78, 0x70, 0x65, 0x63, 0x74, 0x65, 0x64, 0x5f, 0x61, 0x6e, 0x79, 0x18, 0x04, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x48, 0x01, 0x52, 0x0b, 0x65, 0x78,
	0x70, 0x65, 0x63, 0x74, 0x65, 0x64, 0x41, 0x6e, 0x79, 0x12, 0x4e, 0x0a, 0x16, 0x65, 0x78, 0x70,
	0x65, 0x63, 0x74, 0x65, 0x64, 0x5f, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x5f, 0x65, 0x78, 0x69,
	0x73, 0x74, 0x73, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74,
	0x79, 0x48, 0x01, 0x52, 0x14, 0x65, 0x78, 0x70, 0x65, 0x63, 0x74, 0x65, 0x64, 0x53, 0x74, 0x72,
	0x65, 0x61, 0x6d, 0x45, 0x78, 0x69, 0x73, 0x74, 0x73, 0x12, 0x46, 0x0a, 0x12, 0x65, 0x78, 0x70,
	0x65, 0x63, 0x74, 0x65, 0x64, 0x5f, 0x6e, 0x6f, 0x5f, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x18,
	0x06, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x45, 0x6d, 0x70, 0x74, 0x79, 0x48, 0x01, 0x52,
	0x10, 0x65, 0x78, 0x70, 0x65, 0x63, 0x74, 0x65, 0x64, 0x4e, 0x6f, 0x53, 0x74, 0x72, 0x65, 0x61,
	0x6d, 0x42, 0x20, 0x0a, 0x1e, 0x63, 0x75, 0x72, 0x72, 0x65, 0x6e, 0x74, 0x5f, 0x73, 0x74, 0x72,
	0x65, 0x61, 0x6d, 0x5f, 0x72, 0x65, 0x76, 0x69, 0x73, 0x69, 0x6f, 0x6e, 0x5f, 0x6f, 0x70, 0x74,
	0x69, 0x6f, 0x6e, 0x42, 0x21, 0x0a, 0x1f, 0x65, 0x78, 0x70, 0x65, 0x63, 0x74, 0x65, 0x64, 0x5f,
	0x73, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x5f, 0x70, 0x6f, 0x73, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x5f,
	0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0x0e, 0x0a, 0x0c, 0x41, 0x63, 0x63, 0x65, 0x73, 0x73,
	0x44, 0x65, 0x6e, 0x69, 0x65, 0x64, 0x22, 0x62, 0x0a, 0x0d, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d,
	0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x12, 0x51, 0x0a, 0x11, 0x73, 0x74, 0x72, 0x65, 0x61,
	0x6d, 0x5f, 0x69, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x66, 0x69, 0x65, 0x72, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x24, 0x2e, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x5f, 0x73, 0x74, 0x6f, 0x72, 0x65,
	0x2e, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x2e, 0x53, 0x74, 0x72, 0x65, 0x61, 0x6d, 0x49, 0x64,
	0x65, 0x6e, 0x74, 0x69, 0x66, 0x69, 0x65, 0x72, 0x52, 0x10, 0x73, 0x74, 0x72, 0x65, 0x61, 0x6d,
	0x49, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x66, 0x69, 0x65, 0x72, 0x22, 0x09, 0x0a, 0x07, 0x54, 0x69,
	0x6d, 0x65, 0x6f, 0x75, 0x74, 0x22, 0x09, 0x0a, 0x07, 0x55, 0x6e, 0x6b, 0x6e, 0x6f, 0x77, 0x6e,
	0x22, 0x14, 0x0a, 0x12, 0x49, 0x6e, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x54, 0x72, 0x61, 0x6e, 0x73,
	0x61, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0x41, 0x0a, 0x19, 0x4d, 0x61, 0x78, 0x69, 0x6d, 0x75,
	0x6d, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64, 0x53, 0x69, 0x7a, 0x65, 0x45, 0x78, 0x63, 0x65, 0x65,
	0x64, 0x65, 0x64, 0x12, 0x24, 0x0a, 0x0d, 0x6d, 0x61, 0x78, 0x41, 0x70, 0x70, 0x65, 0x6e, 0x64,
	0x53, 0x69, 0x7a, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x0d, 0x6d, 0x61, 0x78, 0x41,
	0x70, 0x70, 0x65, 0x6e, 0x64, 0x53, 0x69, 0x7a, 0x65, 0x22, 0x26, 0x0a, 0x0a, 0x42, 0x61, 0x64,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x18, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61,
	0x67, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67,
	0x65, 0x42, 0x63, 0x0a, 0x24, 0x63, 0x6f, 0x6d, 0x2e, 0x65, 0x76, 0x65, 0x6e, 0x74, 0x73, 0x74,
	0x6f, 0x72, 0x65, 0x2e, 0x64, 0x62, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x2e, 0x73, 0x68, 0x61, 0x72, 0x65, 0x64, 0x5a, 0x3b, 0x67, 0x69, 0x74, 0x68, 0x75,
	0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x53, 0x74, 0x6f, 0x72, 0x65,
	0x2f, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x53, 0x74, 0x6f, 0x72, 0x65, 0x2d, 0x43, 0x6c, 0x69, 0x65,
	0x6e, 0x74, 0x2d, 0x47, 0x6f, 0x2f, 0x76, 0x33, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x2f,
	0x73, 0x68, 0x61, 0x72, 0x65, 0x64, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_shared_proto_rawDescOnce sync.Once
	file_shared_proto_rawDescData = file_shared_proto_rawDesc
)

func file_shared_proto_rawDescGZIP() []byte {
	file_shared_proto_rawDescOnce.Do(func() {
		file_shared_proto_rawDescData = protoimpl.X.CompressGZIP(file_shared_proto_rawDescData)
	})
	return file_shared_proto_rawDescData
}

var file_shared_proto_msgTypes = make([]protoimpl.MessageInfo, 13)
var file_shared_proto_goTypes = []interface{}{
	(*UUID)(nil),                      // 0: event_store.client.UUID
	(*Empty)(nil),                     // 1: event_store.client.Empty
	(*StreamIdentifier)(nil),          // 2: event_store.client.StreamIdentifier
	(*AllStreamPosition)(nil),         // 3: event_store.client.AllStreamPosition
	(*WrongExpectedVersion)(nil),      // 4: event_store.client.WrongExpectedVersion
	(*AccessDenied)(nil),              // 5: event_store.client.AccessDenied
	(*StreamDeleted)(nil),             // 6: event_store.client.StreamDeleted
	(*Timeout)(nil),                   // 7: event_store.client.Timeout
	(*Unknown)(nil),                   // 8: event_store.client.Unknown
	(*InvalidTransaction)(nil),        // 9: event_store.client.InvalidTransaction
	(*MaximumAppendSizeExceeded)(nil), // 10: event_store.client.MaximumAppendSizeExceeded
	(*BadRequest)(nil),                // 11: event_store.client.BadRequest
	(*UUID_Structured)(nil),           // 12: event_store.client.UUID.Structured
	(*emptypb.Empty)(nil),             // 13: google.protobuf.Empty
}
var file_shared_proto_depIdxs = []int32{
	12, // 0: event_store.client.UUID.structured:type_name -> event_store.client.UUID.Structured
	13, // 1: event_store.client.WrongExpectedVersion.current_no_stream:type_name -> google.protobuf.Empty
	13, // 2: event_store.client.WrongExpectedVersion.expected_any:type_name -> google.protobuf.Empty
	13, // 3: event_store.client.WrongExpectedVersion.expected_stream_exists:type_name -> google.protobuf.Empty
	13, // 4: event_store.client.WrongExpectedVersion.expected_no_stream:type_name -> google.protobuf.Empty
	2,  // 5: event_store.client.StreamDeleted.stream_identifier:type_name -> event_store.client.StreamIdentifier
	6,  // [6:6] is the sub-list for method output_type
	6,  // [6:6] is the sub-list for method input_type
	6,  // [6:6] is the sub-list for extension type_name
	6,  // [6:6] is the sub-list for extension extendee
	0,  // [0:6] is the sub-list for field type_name
}

func init() { file_shared_proto_init() }
func file_shared_proto_init() {
	if File_shared_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_shared_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*UUID); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_shared_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Empty); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_shared_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StreamIdentifier); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_shared_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AllStreamPosition); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_shared_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*WrongExpectedVersion); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_shared_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*AccessDenied); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_shared_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StreamDeleted); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_shared_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Timeout); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_shared_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Unknown); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_shared_proto_msgTypes[9].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*InvalidTransaction); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_shared_proto_msgTypes[10].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*MaximumAppendSizeExceeded); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_shared_proto_msgTypes[11].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BadRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_shared_proto_msgTypes[12].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*UUID_Structured); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_shared_proto_msgTypes[0].OneofWrappers = []interface{}{
		(*UUID_Structured_)(nil),
		(*UUID_String_)(nil),
	}
	file_shared_proto_msgTypes[4].OneofWrappers = []interface{}{
		(*WrongExpectedVersion_CurrentStreamRevision)(nil),
		(*WrongExpectedVersion_CurrentNoStream)(nil),
		(*WrongExpectedVersion_ExpectedStreamPosition)(nil),
		(*WrongExpectedVersion_ExpectedAny)(nil),
		(*WrongExpectedVersion_ExpectedStreamExists)(nil),
		(*WrongExpectedVersion_ExpectedNoStream)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_shared_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   13,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_shared_proto_goTypes,
		DependencyIndexes: file_shared_proto_depIdxs,
		MessageInfos:      file_shared_proto_msgTypes,
	}.Build()
	File_shared_proto = out.File
	file_shared_proto_rawDesc = nil
	file_shared_proto_goTypes = nil
	file_shared_proto_depIdxs = nil
}
