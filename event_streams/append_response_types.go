package event_streams

import (
	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

type AppendResponse struct {
	// Types that are assignable to CurrentRevisionOption:
	//	AppendResponseSuccessCurrentRevision
	//	AppendResponseSuccessCurrentRevisionNoStream
	CurrentRevision isAppendResponseSuccessCurrentRevision
	// Types that are assignable to PositionOption:
	//	AppendResponseSuccessPosition
	//	AppendResponseSuccessNoPosition
	Position isAppendResponseSuccessPosition
}

func (this AppendResponse) GetCurrentRevisionNoStream() bool {
	if _, ok := this.CurrentRevision.(AppendResponseSuccessCurrentRevisionNoStream); ok {
		return true
	}
	return false
}

func (this AppendResponse) GetCurrentRevision() uint64 {
	if revision, ok := this.CurrentRevision.(AppendResponseSuccessCurrentRevision); ok {
		return revision.CurrentRevision
	}
	return 0
}

func (this AppendResponse) GetPosition() (Position, bool) {
	if revision, ok := this.Position.(AppendResponseSuccessPosition); ok {
		return Position{
			CommitPosition:  revision.CommitPosition,
			PreparePosition: revision.PreparePosition,
		}, true
	}
	return Position{}, false
}

type isAppendResponseSuccessPosition interface {
	isAppendResponseSuccessPosition()
}

type AppendResponseSuccessPosition struct {
	CommitPosition  uint64
	PreparePosition uint64
}

func (this AppendResponseSuccessPosition) isAppendResponseSuccessPosition() {
}

type AppendResponseSuccessNoPosition struct{}

func (this AppendResponseSuccessNoPosition) isAppendResponseSuccessPosition() {
}

type isAppendResponseSuccessCurrentRevision interface {
	isAppendResponseSuccessCurrentRevision()
}

type AppendResponseSuccessCurrentRevision struct {
	CurrentRevision uint64
}

func (this AppendResponseSuccessCurrentRevision) isAppendResponseSuccessCurrentRevision() {
}

type AppendResponseSuccessCurrentRevisionNoStream struct{}

func (this AppendResponseSuccessCurrentRevisionNoStream) isAppendResponseSuccessCurrentRevision() {
}

type isAppendResponseWrongCurrentRevision_20_6_0 interface {
	isAppendResponseWrongCurrentRevision_20_6_0()
}

type AppendResponseWrongCurrentRevision_20_6_0 struct {
	CurrentRevision uint64
}

func (this AppendResponseWrongCurrentRevision_20_6_0) isAppendResponseWrongCurrentRevision_20_6_0() {
}

type AppendResponseWrongCurrentRevisionNoStream_20_6_0 struct{}

func (this AppendResponseWrongCurrentRevisionNoStream_20_6_0) isAppendResponseWrongCurrentRevision_20_6_0() {
}

type isAppendResponseWrongExpectedRevision_20_6_0 interface {
	isAppendResponseWrongExpectedRevision_20_6_0()
}

type AppendResponseWrongExpectedRevision_20_6_0 struct {
	ExpectedRevision uint64
}

func (this AppendResponseWrongExpectedRevision_20_6_0) isAppendResponseWrongExpectedRevision_20_6_0() {
}

type AppendResponseWrongExpectedRevisionAny_20_6_0 struct{}

func (this AppendResponseWrongExpectedRevisionAny_20_6_0) isAppendResponseWrongExpectedRevision_20_6_0() {
}

type AppendResponseWrongExpectedRevisionStreamExists_20_6_0 struct{}

func (this AppendResponseWrongExpectedRevisionStreamExists_20_6_0) isAppendResponseWrongExpectedRevision_20_6_0() {
}

type isAppendResponseWrongCurrentRevision interface {
	isAppendResponseCurrentRevision()
}

type AppendResponseWrongCurrentRevision struct {
	CurrentRevision uint64
}

func (this AppendResponseWrongCurrentRevision) isAppendResponseCurrentRevision() {
}

type AppendResponseWrongCurrentRevisionNoStream struct{}

func (this AppendResponseWrongCurrentRevisionNoStream) isAppendResponseCurrentRevision() {
}

type isAppendResponseWrongExpectedRevision interface {
	isAppendResponseWrongExpectedRevision()
}

type AppendResponseWrongExpectedRevision struct {
	ExpectedRevision uint64
}

func (this AppendResponseWrongExpectedRevision) isAppendResponseWrongExpectedRevision() {
}

type AppendResponseWrongExpectedRevisionAny struct{}

func (this AppendResponseWrongExpectedRevisionAny) isAppendResponseWrongExpectedRevision() {
}

type AppendResponseWrongExpectedRevisionStreamExists struct{}

func (this AppendResponseWrongExpectedRevisionStreamExists) isAppendResponseWrongExpectedRevision() {
}

type AppendResponseWrongExpectedRevisionNoStream struct{}

func (this AppendResponseWrongExpectedRevisionNoStream) isAppendResponseWrongExpectedRevision() {
}

type appendResponseAdapter interface {
	CreateResponseWithError(
		protoResponse *streams2.AppendResp) (AppendResponse, errors.Error)
}

type appendResponseAdapterImpl struct{}

func (this appendResponseAdapterImpl) CreateResponseWithError(
	protoResponse *streams2.AppendResp) (AppendResponse, errors.Error) {

	switch protoResponse.Result.(type) {
	case *streams2.AppendResp_WrongExpectedVersion_:
		wrongExpectedVersionProto := protoResponse.Result.(*streams2.AppendResp_WrongExpectedVersion_)
		return AppendResponse{}, this.buildWrongExpectedVersionError(wrongExpectedVersionProto)
	case *streams2.AppendResp_Success_:
		successProtoResult := protoResponse.Result.(*streams2.AppendResp_Success_)
		return this.buildSuccessResponse(successProtoResult), nil
	}

	return AppendResponse{}, nil
}

func (this appendResponseAdapterImpl) buildSuccessResponse(
	protoSuccessResult *streams2.AppendResp_Success_) AppendResponse {
	result := AppendResponse{}

	switch protoSuccessResult.Success.CurrentRevisionOption.(type) {
	case *streams2.AppendResp_Success_CurrentRevision:
		protoCurrentRevision := protoSuccessResult.Success.CurrentRevisionOption.(*streams2.AppendResp_Success_CurrentRevision)
		result.CurrentRevision = AppendResponseSuccessCurrentRevision{
			CurrentRevision: protoCurrentRevision.CurrentRevision,
		}

	case *streams2.AppendResp_Success_NoStream:
		result.CurrentRevision = AppendResponseSuccessCurrentRevisionNoStream{}
	}

	switch protoSuccessResult.Success.PositionOption.(type) {
	case *streams2.AppendResp_Success_Position:
		protoPosition := protoSuccessResult.Success.PositionOption.(*streams2.AppendResp_Success_Position)
		result.Position = AppendResponseSuccessPosition{
			CommitPosition:  protoPosition.Position.CommitPosition,
			PreparePosition: protoPosition.Position.PreparePosition,
		}
	case *streams2.AppendResp_Success_NoPosition:
		result.Position = AppendResponseSuccessNoPosition{}
	}

	return result
}

func (this appendResponseAdapterImpl) buildWrongExpectedVersionError(
	proto *streams2.AppendResp_WrongExpectedVersion_) WrongExpectedVersion {
	result := newWrongExpectedVersionError()

	result.CurrentRevision_20_6_0 = this.buildCurrentRevision2060(proto.WrongExpectedVersion)
	result.ExpectedRevision_20_6_0 = this.buildExpectedRevision2060(proto.WrongExpectedVersion)
	result.CurrentRevision = this.buildCurrentRevision(proto.WrongExpectedVersion)
	result.ExpectedRevision = this.buildExpectedRevision(proto.WrongExpectedVersion)

	return result
}

func (this appendResponseAdapterImpl) buildCurrentRevision2060(
	proto *streams2.AppendResp_WrongExpectedVersion) isAppendResponseWrongCurrentRevision_20_6_0 {

	if proto.CurrentRevisionOption_20_6_0 != nil {
		switch proto.CurrentRevisionOption_20_6_0.(type) {
		case *streams2.AppendResp_WrongExpectedVersion_CurrentRevision_20_6_0:
			protoWrongCurrentRevision := proto.CurrentRevisionOption_20_6_0.(*streams2.AppendResp_WrongExpectedVersion_CurrentRevision_20_6_0)
			return AppendResponseWrongCurrentRevision_20_6_0{
				CurrentRevision: protoWrongCurrentRevision.CurrentRevision_20_6_0,
			}
		case *streams2.AppendResp_WrongExpectedVersion_NoStream_20_6_0:
			return AppendResponseWrongCurrentRevisionNoStream_20_6_0{}
		}
	}

	return nil
}

func (this appendResponseAdapterImpl) buildExpectedRevision2060(
	proto *streams2.AppendResp_WrongExpectedVersion) isAppendResponseWrongExpectedRevision_20_6_0 {

	if proto.ExpectedRevisionOption_20_6_0 != nil {
		switch proto.ExpectedRevisionOption_20_6_0.(type) {
		case *streams2.AppendResp_WrongExpectedVersion_ExpectedRevision_20_6_0:
			protoExpectedRevision2060 := proto.ExpectedRevisionOption_20_6_0.(*streams2.AppendResp_WrongExpectedVersion_ExpectedRevision_20_6_0)
			return AppendResponseWrongExpectedRevision_20_6_0{
				ExpectedRevision: protoExpectedRevision2060.ExpectedRevision_20_6_0,
			}
		case *streams2.AppendResp_WrongExpectedVersion_Any_20_6_0:
			return AppendResponseWrongExpectedRevisionAny_20_6_0{}
		case *streams2.AppendResp_WrongExpectedVersion_StreamExists_20_6_0:
			return AppendResponseWrongExpectedRevisionStreamExists_20_6_0{}
		}
	}

	return nil
}

func (this appendResponseAdapterImpl) buildCurrentRevision(
	proto *streams2.AppendResp_WrongExpectedVersion) isAppendResponseWrongCurrentRevision {

	if proto.CurrentRevisionOption != nil {
		switch proto.CurrentRevisionOption.(type) {
		case *streams2.AppendResp_WrongExpectedVersion_CurrentRevision:
			protoWrongCurrentRevision := proto.CurrentRevisionOption.(*streams2.AppendResp_WrongExpectedVersion_CurrentRevision)
			return AppendResponseWrongCurrentRevision{
				CurrentRevision: protoWrongCurrentRevision.CurrentRevision,
			}
		case *streams2.AppendResp_WrongExpectedVersion_CurrentNoStream:
			return AppendResponseWrongCurrentRevisionNoStream{}
		}
	}

	return nil
}

func (this appendResponseAdapterImpl) buildExpectedRevision(
	proto *streams2.AppendResp_WrongExpectedVersion) isAppendResponseWrongExpectedRevision {

	if proto.ExpectedRevisionOption != nil {
		switch proto.ExpectedRevisionOption.(type) {
		case *streams2.AppendResp_WrongExpectedVersion_ExpectedRevision:
			protoExpectedRevision := proto.ExpectedRevisionOption.(*streams2.AppendResp_WrongExpectedVersion_ExpectedRevision)
			return AppendResponseWrongExpectedRevision{
				ExpectedRevision: protoExpectedRevision.ExpectedRevision,
			}
		case *streams2.AppendResp_WrongExpectedVersion_ExpectedAny:
			return AppendResponseWrongExpectedRevisionAny{}
		case *streams2.AppendResp_WrongExpectedVersion_ExpectedStreamExists:
			return AppendResponseWrongExpectedRevisionStreamExists{}
		case *streams2.AppendResp_WrongExpectedVersion_ExpectedNoStream:
			return AppendResponseWrongExpectedRevisionNoStream{}
		}
	}

	return nil
}
