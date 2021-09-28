package event_streams

import (
	"github.com/golang/protobuf/ptypes/any"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

type BatchAppendResponse struct {
	CorrelationId string
	// Types that are assignable to Result:
	//	BatchAppendResponseResultError
	//	BatchAppendResponseResultSuccess
	Result           isBatchAppendResponseResult
	StreamIdentifier string
	// Types that are assignable to ExpectedStreamPosition:
	//	BatchAppendResponseExpectedStreamPosition
	//	BatchAppendResponseExpectedStreamPositionNoStream
	//	BatchAppendResponseExpectedStreamPositionAny
	//	BatchAppendResponseExpectedStreamPositionStreamExists
	ExpectedStreamPosition isBatchAppendResponseExpectedStreamPosition
}

func (response BatchAppendResponse) GetError() (BatchAppendResponseResultError, bool) {
	err, ok := response.Result.(BatchAppendResponseResultError)
	return err, ok
}

func (response BatchAppendResponse) GetSuccess() (BatchAppendResponseResultSuccess, bool) {
	success, ok := response.Result.(BatchAppendResponseResultSuccess)
	return success, ok
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

type isBatchAppendResponseResult interface {
	isBatchAppendResponseResult()
}

type BatchAppendResponseResultError struct {
	Code    int32
	Message string
	Details []ErrorDetails
}

func (this BatchAppendResponseResultError) isBatchAppendResponseResult() {
}

type ErrorDetails struct {
	TypeUrl string
	Value   []byte
}

type BatchAppendResponseResultSuccess struct {
	// BatchAppendResponseResultSuccessCurrentRevision
	// BatchAppendResponseResultSuccessCurrentRevisionNoStream
	CurrentRevisionOption isBatchAppendResponseResultSuccessCurrentRevision
	// BatchAppendResponseResultSuccessPosition
	// BatchAppendResponseResultSuccessNoPosition
	Position isBatchAppendResponseResultSuccessPosition
}

func (success BatchAppendResponseResultSuccess) GetRevisionNoStream() bool {
	_, ok := success.CurrentRevisionOption.(BatchAppendResponseResultSuccessCurrentRevisionNoStream)
	return ok
}

func (success BatchAppendResponseResultSuccess) GetRevision() uint64 {
	if revision, ok := success.CurrentRevisionOption.(BatchAppendResponseResultSuccessCurrentRevision); ok {
		return revision.CurrentRevision
	}

	return 0
}

func (success BatchAppendResponseResultSuccess) GetPosition() (BatchAppendResponseResultSuccessPosition, bool) {
	if position, ok := success.Position.(BatchAppendResponseResultSuccessPosition); ok {
		return position, true
	}

	return BatchAppendResponseResultSuccessPosition{}, false
}

func (this BatchAppendResponseResultSuccess) isBatchAppendResponseResult() {
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
}

type batchResponseAdapterImpl struct{}

func (this batchResponseAdapterImpl) CreateResponse(
	protoResponse *streams2.BatchAppendResp) BatchAppendResponse {
	correlationId := protoResponse.GetCorrelationId()
	correlationIdString := correlationId.GetString_()

	result := BatchAppendResponse{
		CorrelationId:    correlationIdString,
		StreamIdentifier: string(protoResponse.StreamIdentifier.StreamName),
	}

	result.Result = this.buildResult(protoResponse)
	result.ExpectedStreamPosition = this.buildExpectedStreamPosition(protoResponse)

	return result
}

func (this batchResponseAdapterImpl) buildResult(
	response *streams2.BatchAppendResp) isBatchAppendResponseResult {
	switch response.Result.(type) {
	case *streams2.BatchAppendResp_Error:
		protoError := response.GetError()
		return BatchAppendResponseResultError{
			Code:    protoError.Code,
			Message: protoError.Message,
			Details: buildErrorDetails(protoError.Details),
		}
	case *streams2.BatchAppendResp_Success_:
		protoSuccess := response.Result.(*streams2.BatchAppendResp_Success_)
		return this.buildResultSuccess(protoSuccess)
	default:
		panic("unsupported type received")
	}
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

func (this batchResponseAdapterImpl) buildResultSuccess(
	protoSuccess *streams2.BatchAppendResp_Success_) BatchAppendResponseResultSuccess {
	result := BatchAppendResponseResultSuccess{}

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
