package event_streams

import (
	"github.com/google/uuid"
	"github.com/pivonroll/EventStore-Client-Go/protobuf_uuid"

	"github.com/golang/protobuf/ptypes/any"
	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

type BatchAppendResponse struct {
	CorrelationId uuid.UUID
	// BatchAppendResponseResultSuccessPosition
	// BatchAppendResponseResultSuccessNoPosition
	Position         isBatchAppendResponseResultSuccessPosition
	StreamIdentifier string
	// BatchAppendResponseResultSuccessCurrentRevision
	// BatchAppendResponseResultSuccessCurrentRevisionNoStream
	CurrentRevisionOption isBatchAppendResponseResultSuccessCurrentRevision
	// Types that are assignable to ExpectedStreamPosition:
	//	BatchAppendResponseExpectedStreamPosition
	//	BatchAppendResponseExpectedStreamPositionNoStream
	//	BatchAppendResponseExpectedStreamPositionAny
	//	BatchAppendResponseExpectedStreamPositionStreamExists
	ExpectedStreamPosition isBatchAppendResponseExpectedStreamPosition
}

func (response BatchAppendResponse) GetRevisionNoStream() bool {
	_, ok := response.CurrentRevisionOption.(BatchAppendResponseResultSuccessCurrentRevisionNoStream)
	return ok
}

func (response BatchAppendResponse) GetRevision() uint64 {
	if revision, ok := response.CurrentRevisionOption.(BatchAppendResponseResultSuccessCurrentRevision); ok {
		return revision.CurrentRevision
	}

	return 0
}

func (response BatchAppendResponse) GetPosition() (BatchAppendResponseResultSuccessPosition, bool) {
	if position, ok := response.Position.(BatchAppendResponseResultSuccessPosition); ok {
		return position, true
	}

	return BatchAppendResponseResultSuccessPosition{}, false
}

func (response BatchAppendResponse) IsExpectedStreamPositionNil() bool {
	return response.ExpectedStreamPosition == nil
}

func (response BatchAppendResponse) GetExpectedPosition() (uint64, bool) {
	if response.ExpectedStreamPosition != nil {
		if position, ok := response.ExpectedStreamPosition.(BatchAppendResponseExpectedStreamPosition); ok {
			return position.StreamPosition, true
		}
	}

	return 0, false
}

func (response BatchAppendResponse) IsExpectedNoStreamPosition() bool {
	_, ok := response.ExpectedStreamPosition.(BatchAppendResponseExpectedStreamPositionNoStream)
	return ok
}

func (response BatchAppendResponse) IsExpectedAnyPosition() bool {
	_, ok := response.ExpectedStreamPosition.(BatchAppendResponseExpectedStreamPositionAny)
	return ok
}

func (response BatchAppendResponse) IsExpectedStreamExistsPosition() bool {
	_, ok := response.ExpectedStreamPosition.(BatchAppendResponseExpectedStreamPositionStreamExists)
	return ok
}

type BatchError struct {
	err           errors.Error
	ProtoCode     int32
	Message       string
	Details       []ErrorDetails
	CorrelationId string
	StreamId      string
}

func newBatchError(errorCode errors.ErrorCode) BatchError {
	return BatchError{
		err: errors.NewErrorCode(errorCode),
	}
}

func (b BatchError) Error() string {
	return b.err.Error()
}

func (b BatchError) Code() errors.ErrorCode {
	return b.err.Code()
}

type ErrorDetails struct {
	TypeUrl string
	Value   []byte
}

type batchAppendResponseResultSuccess struct {
	// BatchAppendResponseResultSuccessCurrentRevision
	// BatchAppendResponseResultSuccessCurrentRevisionNoStream
	CurrentRevisionOption isBatchAppendResponseResultSuccessCurrentRevision
	// BatchAppendResponseResultSuccessPosition
	// BatchAppendResponseResultSuccessNoPosition
	Position isBatchAppendResponseResultSuccessPosition
}

type isBatchAppendResponseResultSuccessCurrentRevision interface {
	isBatchAppendResponseResultSuccessCurrentRevision()
}

type BatchAppendResponseResultSuccessCurrentRevision struct {
	CurrentRevision uint64
}

func (this BatchAppendResponseResultSuccessCurrentRevision) isBatchAppendResponseResultSuccessCurrentRevision() {
}

type BatchAppendResponseResultSuccessCurrentRevisionNoStream struct{}

func (this BatchAppendResponseResultSuccessCurrentRevisionNoStream) isBatchAppendResponseResultSuccessCurrentRevision() {
}

type isBatchAppendResponseResultSuccessPosition interface {
	isBatchAppendResponseResultSuccessPosition()
}

type BatchAppendResponseResultSuccessPosition struct {
	CommitPosition  uint64
	PreparePosition uint64
}

func (this BatchAppendResponseResultSuccessPosition) isBatchAppendResponseResultSuccessPosition() {
}

type BatchAppendResponseResultSuccessNoPosition struct{}

func (this BatchAppendResponseResultSuccessNoPosition) isBatchAppendResponseResultSuccessPosition() {
}

type isBatchAppendResponseExpectedStreamPosition interface {
	isBatchAppendResponseExpectedStreamPosition()
}

type BatchAppendResponseExpectedStreamPosition struct {
	StreamPosition uint64
}

func (this BatchAppendResponseExpectedStreamPosition) isBatchAppendResponseExpectedStreamPosition() {
}

type BatchAppendResponseExpectedStreamPositionNoStream struct{}

func (this BatchAppendResponseExpectedStreamPositionNoStream) isBatchAppendResponseExpectedStreamPosition() {
}

type BatchAppendResponseExpectedStreamPositionAny struct{}

func (this BatchAppendResponseExpectedStreamPositionAny) isBatchAppendResponseExpectedStreamPosition() {
}

type BatchAppendResponseExpectedStreamPositionStreamExists struct{}

func (this BatchAppendResponseExpectedStreamPositionStreamExists) isBatchAppendResponseExpectedStreamPosition() {
}

type batchResponseAdapter interface {
	CreateResponse(protoResponse *streams2.BatchAppendResp) BatchAppendResponse
	CreateResponseWithError(protoResponse *streams2.BatchAppendResp) (BatchAppendResponse, errors.Error)
}

type batchResponseAdapterImpl struct{}

func (this batchResponseAdapterImpl) CreateResponse(
	protoResponse *streams2.BatchAppendResp) BatchAppendResponse {
	return this.buildSuccess(protoResponse)
}

const (
	protoWrongExpectedVersion = "WrongExpectedVersion"
	protoAccessDenied         = "AccessDenied"
	protoTimeout              = "Timeout"
	protoBadRequest           = "BadRequest"
	protoStreamDeleted        = "StreamDeleted"
)

func (this batchResponseAdapterImpl) CreateResponseWithError(
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
	errorResult.CorrelationId = protobuf_uuid.GetUUID(protoResponse.GetCorrelationId()).String()
	errorResult.StreamId = string(protoResponse.GetStreamIdentifier().GetStreamName())

	return errorResult
}

func (this batchResponseAdapterImpl) buildSuccess(
	protoResponse *streams2.BatchAppendResp) BatchAppendResponse {
	result := BatchAppendResponse{
		CorrelationId:    protobuf_uuid.GetUUID(protoResponse.GetCorrelationId()),
		StreamIdentifier: string(protoResponse.StreamIdentifier.StreamName),
	}

	result.ExpectedStreamPosition = this.buildExpectedStreamPosition(protoResponse)
	protoSuccess := protoResponse.Result.(*streams2.BatchAppendResp_Success_)
	revisionAndPosition := this.buildSuccessRevisionAndPosition(protoSuccess)
	result.CurrentRevisionOption = revisionAndPosition.CurrentRevisionOption
	result.Position = revisionAndPosition.Position

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
		result.CurrentRevisionOption = BatchAppendResponseResultSuccessCurrentRevision{
			CurrentRevision: protoCurrentRevision.CurrentRevision,
		}
	case *streams2.BatchAppendResp_Success_NoStream:
		result.CurrentRevisionOption = BatchAppendResponseResultSuccessCurrentRevisionNoStream{}
	}

	switch protoSuccess.Success.PositionOption.(type) {
	case *streams2.BatchAppendResp_Success_Position:
		protoPosition := protoSuccess.Success.PositionOption.(*streams2.BatchAppendResp_Success_Position)
		result.Position = BatchAppendResponseResultSuccessPosition{
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
		return BatchAppendResponseExpectedStreamPosition{
			StreamPosition: protoPosition.StreamPosition,
		}
	case *streams2.BatchAppendResp_NoStream:
		return BatchAppendResponseExpectedStreamPositionNoStream{}
	case *streams2.BatchAppendResp_Any:
		return BatchAppendResponseExpectedStreamPositionAny{}
	case *streams2.BatchAppendResp_StreamExists:
		return BatchAppendResponseExpectedStreamPositionStreamExists{}
	default:
		panic("unsupported type")
	}
}
