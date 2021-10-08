package event_streams

import (
	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/protobuf_uuid"

	"github.com/golang/protobuf/ptypes/any"
	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

// BatchAppendResponse is a response returned by EventStoreDB after an entire batch of events (all chunks) were appended
// to a stream in EventStoreDB.
type BatchAppendResponse struct {
	correlationId uuid.UUID
	// batchAppendResponseResultSuccessPosition
	// batchAppendResponseResultSuccessNoPosition
	position isBatchAppendResponseResultSuccessPosition
	streamId string
	// batchAppendResponseResultSuccessCurrentRevision
	// batchAppendResponseResultSuccessCurrentRevisionNoStream
	currentRevisionOption isBatchAppendResponseResultSuccessCurrentRevision
	// Types that are assignable to ExpectedStreamPosition:
	//	batchAppendResponseExpectedStreamPosition
	//	batchAppendResponseExpectedStreamPositionNoStream
	//	batchAppendResponseExpectedStreamPositionAny
	//	batchAppendResponseExpectedStreamPositionStreamExists
	expectedStreamPosition isBatchAppendResponseExpectedStreamPosition
}

// GetCorrelationId returns a correlation id the client has sent along with a
// chunk of events to append to EventStoreDB stream.
func (response BatchAppendResponse) GetCorrelationId() uuid.UUID {
	return response.correlationId
}

// IsCurrentRevisionNoStream returns true if current revision of a stream returned by EventStore is NoStream.
func (response BatchAppendResponse) IsCurrentRevisionNoStream() bool {
	_, ok := response.currentRevisionOption.(batchAppendResponseResultSuccessCurrentRevisionNoStream)
	return ok
}

// GetCurrentRevision returns a current revision of a stream after events were appended to a stream in EventStoreDB.
func (response BatchAppendResponse) GetCurrentRevision() uint64 {
	if revision, ok := response.currentRevisionOption.(batchAppendResponseResultSuccessCurrentRevision); ok {
		return revision.CurrentRevision
	}

	return 0
}

// GetPosition returns a position of the last appended event to a stream along with a boolean which
// indicates if EventStoreDB has returned a position or if it has not returned a position.
// If EventStoreDB has not returned a position, GetPosition returns a zero initialized Position and false.
func (response BatchAppendResponse) GetPosition() (Position, bool) {
	if position, ok := response.position.(batchAppendResponseResultSuccessPosition); ok {
		return Position{
			CommitPosition:  position.CommitPosition,
			PreparePosition: position.PreparePosition,
		}, true
	}

	return Position{}, false
}

// HasExpectedStreamPosition returns true is response contains expected stream position.
func (response BatchAppendResponse) HasExpectedStreamPosition() bool {
	return response.expectedStreamPosition != nil
}

// GetExpectedStreamPosition returns true if response contains finite expected stream position.
func (response BatchAppendResponse) GetExpectedStreamPosition() (uint64, bool) {
	if response.expectedStreamPosition != nil {
		if position, ok := response.expectedStreamPosition.(batchAppendResponseExpectedStreamPosition); ok {
			return position.StreamPosition, true
		}
	}

	return 0, false
}

// IsExpectedStreamPositionNoStream returns true if expected stream position is NoStream.
func (response BatchAppendResponse) IsExpectedStreamPositionNoStream() bool {
	_, ok := response.expectedStreamPosition.(batchAppendResponseExpectedStreamPositionNoStream)
	return ok
}

// IsExpectedStreamPositionAny returns true if expected stream position is Any.
func (response BatchAppendResponse) IsExpectedStreamPositionAny() bool {
	_, ok := response.expectedStreamPosition.(batchAppendResponseExpectedStreamPositionAny)
	return ok
}

// IsExpectedStreamPositionStreamExists returns true if expected stream position is StreamExists.
func (response BatchAppendResponse) IsExpectedStreamPositionStreamExists() bool {
	_, ok := response.expectedStreamPosition.(batchAppendResponseExpectedStreamPositionStreamExists)
	return ok
}

// BatchError is an error which can occur when chunks of events are appended to a stream in EventStoreDB.
type BatchError struct {
	err           errors.Error
	ProtoCode     int32          // an error code returned by EventStoreDB
	Message       string         // informative message of an error returned by EventStoreDB
	Details       []ErrorDetails // various details about an error
	CorrelationId uuid.UUID      // correlation id sent by a client when it tried to append a batch of events to a stream at EventStoreDB
	StreamId      string         // stream to which we wanted to append events to
}

func newBatchError(errorCode errors.ErrorCode) BatchError {
	return BatchError{
		err: errors.NewErrorCode(errorCode),
	}
}

// Error returns a string representation of an error.
func (b BatchError) Error() string {
	return b.err.Error()
}

// Code returns a code of an error.
func (b BatchError) Code() errors.ErrorCode {
	return b.err.Code()
}

// ErrorDetails represents various details about an error EventStore might sent to us.
type ErrorDetails struct {
	TypeUrl string
	Value   []byte
}

type batchAppendResponseResultSuccess struct {
	// batchAppendResponseResultSuccessCurrentRevision
	// batchAppendResponseResultSuccessCurrentRevisionNoStream
	CurrentRevisionOption isBatchAppendResponseResultSuccessCurrentRevision
	// batchAppendResponseResultSuccessPosition
	// batchAppendResponseResultSuccessNoPosition
	Position isBatchAppendResponseResultSuccessPosition
}

type isBatchAppendResponseResultSuccessCurrentRevision interface {
	isBatchAppendResponseResultSuccessCurrentRevision()
}

type batchAppendResponseResultSuccessCurrentRevision struct {
	CurrentRevision uint64
}

func (this batchAppendResponseResultSuccessCurrentRevision) isBatchAppendResponseResultSuccessCurrentRevision() {
}

type batchAppendResponseResultSuccessCurrentRevisionNoStream struct{}

func (this batchAppendResponseResultSuccessCurrentRevisionNoStream) isBatchAppendResponseResultSuccessCurrentRevision() {
}

type isBatchAppendResponseResultSuccessPosition interface {
	isBatchAppendResponseResultSuccessPosition()
}

type batchAppendResponseResultSuccessPosition struct {
	CommitPosition  uint64
	PreparePosition uint64
}

func (this batchAppendResponseResultSuccessPosition) isBatchAppendResponseResultSuccessPosition() {
}

type isBatchAppendResponseExpectedStreamPosition interface {
	isBatchAppendResponseExpectedStreamPosition()
}

type batchAppendResponseExpectedStreamPosition struct {
	StreamPosition uint64
}

func (this batchAppendResponseExpectedStreamPosition) isBatchAppendResponseExpectedStreamPosition() {
}

type batchAppendResponseExpectedStreamPositionNoStream struct{}

func (this batchAppendResponseExpectedStreamPositionNoStream) isBatchAppendResponseExpectedStreamPosition() {
}

type batchAppendResponseExpectedStreamPositionAny struct{}

func (this batchAppendResponseExpectedStreamPositionAny) isBatchAppendResponseExpectedStreamPosition() {
}

type batchAppendResponseExpectedStreamPositionStreamExists struct{}

func (this batchAppendResponseExpectedStreamPositionStreamExists) isBatchAppendResponseExpectedStreamPosition() {
}

type batchResponseAdapter interface {
	createResponseWithError(protoResponse *streams2.BatchAppendResp) (BatchAppendResponse, errors.Error)
}

type batchResponseAdapterImpl struct{}

const (
	protoWrongExpectedVersion = "WrongExpectedVersion"
	protoAccessDenied         = "AccessDenied"
	protoTimeout              = "Timeout"
	protoBadRequest           = "BadRequest"
	protoStreamDeleted        = "StreamDeleted"
)

func (this batchResponseAdapterImpl) createResponseWithError(
	protoResponse *streams2.BatchAppendResp) (BatchAppendResponse, errors.Error) {

	if protoResponse.Result != nil {
		if protoResponse.GetError() != nil {
			return BatchAppendResponse{}, this.buildBatchError(protoResponse)
		} else {
			return this.buildSuccess(protoResponse), nil
		}
	}

	panic("Unsupported result type")
}

func (this batchResponseAdapterImpl) buildBatchError(
	protoResponse *streams2.BatchAppendResp) BatchError {
	protoError := protoResponse.Result.(*streams2.BatchAppendResp_Error)
	errorCode := errors.UnknownErr
	if protoError.Error.Message == protoWrongExpectedVersion {
		errorCode = errors.WrongExpectedStreamRevisionErr
	} else if protoError.Error.Message == protoAccessDenied {
		errorCode = errors.PermissionDeniedErr
	} else if protoError.Error.Message == protoTimeout {
		errorCode = errors.DeadlineExceededErr
	} else if protoError.Error.Message == protoBadRequest {
		errorCode = errors.InvalidArgumentErr
	} else if protoError.Error.Message == protoStreamDeleted {
		errorCode = errors.StreamDeletedErr
	}

	errorResult := newBatchError(errorCode)
	errorResult.ProtoCode = protoError.Error.Code
	errorResult.Message = protoError.Error.Message
	errorResult.Details = buildErrorDetails(protoError.Error.Details)
	errorResult.CorrelationId = protobuf_uuid.GetUUID(protoResponse.GetCorrelationId())
	errorResult.StreamId = string(protoResponse.GetStreamIdentifier().GetStreamName())

	return errorResult
}

func (this batchResponseAdapterImpl) buildSuccess(
	protoResponse *streams2.BatchAppendResp) BatchAppendResponse {
	result := BatchAppendResponse{
		correlationId: protobuf_uuid.GetUUID(protoResponse.GetCorrelationId()),
		streamId:      string(protoResponse.StreamIdentifier.StreamName),
	}

	result.expectedStreamPosition = this.buildExpectedStreamPosition(protoResponse)
	protoSuccess := protoResponse.Result.(*streams2.BatchAppendResp_Success_)
	revisionAndPosition := this.buildSuccessRevisionAndPosition(protoSuccess)
	result.currentRevisionOption = revisionAndPosition.CurrentRevisionOption
	result.position = revisionAndPosition.Position

	return result
}

func buildErrorDetails(protoDetails []*any.Any) []ErrorDetails {
	var result []ErrorDetails

	for _, item := range protoDetails {
		result = append(result, ErrorDetails{
			TypeUrl: item.TypeUrl,
			Value:   item.Value,
		})
	}

	return result
}

func (this batchResponseAdapterImpl) buildSuccessRevisionAndPosition(
	protoSuccess *streams2.BatchAppendResp_Success_) batchAppendResponseResultSuccess {
	result := batchAppendResponseResultSuccess{}

	switch protoSuccess.Success.CurrentRevisionOption.(type) {
	case *streams2.BatchAppendResp_Success_CurrentRevision:
		protoCurrentRevision := protoSuccess.Success.CurrentRevisionOption.(*streams2.BatchAppendResp_Success_CurrentRevision)
		result.CurrentRevisionOption = batchAppendResponseResultSuccessCurrentRevision{
			CurrentRevision: protoCurrentRevision.CurrentRevision,
		}
	case *streams2.BatchAppendResp_Success_NoStream:
		result.CurrentRevisionOption = batchAppendResponseResultSuccessCurrentRevisionNoStream{}
	}

	switch protoSuccess.Success.PositionOption.(type) {
	case *streams2.BatchAppendResp_Success_Position:
		protoPosition := protoSuccess.Success.PositionOption.(*streams2.BatchAppendResp_Success_Position)
		result.Position = batchAppendResponseResultSuccessPosition{
			CommitPosition:  protoPosition.Position.CommitPosition,
			PreparePosition: protoPosition.Position.PreparePosition,
		}
	}

	return result
}

func (this batchResponseAdapterImpl) buildExpectedStreamPosition(
	protoResponse *streams2.BatchAppendResp) isBatchAppendResponseExpectedStreamPosition {

	if protoResponse.ExpectedStreamPosition == nil {
		return nil
	}

	switch protoResponse.ExpectedStreamPosition.(type) {
	case *streams2.BatchAppendResp_StreamPosition:
		protoPosition := protoResponse.ExpectedStreamPosition.(*streams2.BatchAppendResp_StreamPosition)
		return batchAppendResponseExpectedStreamPosition{
			StreamPosition: protoPosition.StreamPosition,
		}
	case *streams2.BatchAppendResp_NoStream:
		return batchAppendResponseExpectedStreamPositionNoStream{}
	case *streams2.BatchAppendResp_Any:
		return batchAppendResponseExpectedStreamPositionAny{}
	case *streams2.BatchAppendResp_StreamExists:
		return batchAppendResponseExpectedStreamPositionStreamExists{}
	default:
		panic("unsupported type")
	}
}
