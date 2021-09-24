package event_streams

import (
	"github.com/gofrs/uuid"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

type BatchAppendResponse struct {
	CorrelationId uuid.UUID
	// Types that are assignable to Result:
	//	BatchAppendResponseResultError
	//	BatchAppendResponseResultSuccess
	Result           isBatchAppendResponseResult
	StreamIdentifier []byte
	// Types that are assignable to ExpectedStreamPosition:
	//	BatchAppendResponseExpectedStreamPosition
	//	BatchAppendResponseExpectedStreamPositionNoStream
	//	BatchAppendResponseExpectedStreamPositionAny
	//	BatchAppendResponseExpectedStreamPositionStreamExists
	ExpectedStreamPosition isBatchAppendResponseExpectedStreamPosition
}

type isBatchAppendResponseResult interface {
	isBatchAppendResponseResult()
}

type BatchAppendResponseResultError struct{}

func (this BatchAppendResponseResultError) isBatchAppendResponseResult() {
}

type BatchAppendResponseResultSuccess struct {
	// BatchAppendResponseResultSuccessCurrentRevision
	// BatchAppendResponseResultSuccessCurrentRevisionNoStream
	CurrentRevisionOption isBatchAppendResponseResultSuccessCurrentRevision
	// BatchAppendResponseResultSuccessPosition
	// BatchAppendResponseResultSuccessNoPosition
	Position isBatchAppendResponseResultSuccessPosition
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
		CorrelationId:    uuid.FromStringOrNil(correlationIdString),
		StreamIdentifier: protoResponse.StreamIdentifier.StreamName,
	}

	result.Result = this.buildResult(protoResponse)
	result.ExpectedStreamPosition = this.buildExpectedStreamPosition(protoResponse)

	return result
}

func (this batchResponseAdapterImpl) buildResult(
	response *streams2.BatchAppendResp) isBatchAppendResponseResult {
	switch response.Result.(type) {
	case *streams2.BatchAppendResp_Error:
		return BatchAppendResponseResultError{}
	case *streams2.BatchAppendResp_Success_:
		protoSuccess := response.Result.(*streams2.BatchAppendResp_Success_)
		return this.buildResultSuccess(protoSuccess)
	default:
		panic("unsupported type received")
	}
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
