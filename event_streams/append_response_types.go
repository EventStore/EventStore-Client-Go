package event_streams

import "github.com/pivonroll/EventStore-Client-Go/protos/streams2"

type AppendResponse struct {
	// AppendResponseSuccess
	// AppendResponseWrongExpectedVersion
	Result isAppendResponseResult
}

func (this AppendResponse) GetSuccess() (AppendResponseSuccess, bool) {
	if response, ok := this.Result.(AppendResponseSuccess); ok {
		return response, true
	}
	return AppendResponseSuccess{}, false
}

func (this AppendResponse) GetCurrentRevision() (uint64, bool) {
	success, isSuccess := this.GetSuccess()

	if !isSuccess {
		return 0, false
	}

	return success.GetCurrentRevision(), true
}

func (this AppendResponse) IsCurrentRevisionNoStream() bool {
	wrongExpectedVersion, isWrongExpectedRevision := this.GetWrongExpectedVersion()

	if !isWrongExpectedRevision {
		return false
	}

	return wrongExpectedVersion.IsCurrentRevisionNoStream()
}

func (this AppendResponse) GetWrongExpectedVersion() (AppendResponseWrongExpectedVersion, bool) {
	if response, ok := this.Result.(AppendResponseWrongExpectedVersion); ok {
		return response, true
	}
	return AppendResponseWrongExpectedVersion{}, false
}

func (this AppendResponse) GetWrongCurrentRevision() (uint64, bool) {
	wrongExpectedVersion, isWrongExpectedVersion := this.GetWrongExpectedVersion()

	if !isWrongExpectedVersion {
		return 0, false
	}

	return wrongExpectedVersion.GetCurrentRevision()
}

func (this AppendResponse) GetWrongExpectedRevision() (uint64, bool) {
	wrongExpectedVersion, isWrongExpectedVersion := this.GetWrongExpectedVersion()

	if !isWrongExpectedVersion {
		return 0, false
	}

	return wrongExpectedVersion.GetExpectedRevision()
}

func (this AppendResponse) GetPosition() (Position, bool) {
	success, isSuccess := this.GetSuccess()

	if !isSuccess {
		return Position{}, false
	}

	position, isPosition := success.GetPosition()

	if !isPosition {
		return Position{}, false
	}

	return Position{
		CommitPosition:  position.CommitPosition,
		PreparePosition: position.PreparePosition,
	}, true
}

type isAppendResponseResult interface {
	isAppendResponseResult()
}

type AppendResponseSuccess struct {
	// Types that are assignable to CurrentRevisionOption:
	//	AppendResponseSuccessCurrentRevision
	//	AppendResponseSuccessCurrentRevisionNoStream
	CurrentRevision isAppendResponseSuccessCurrentRevision
	// Types that are assignable to PositionOption:
	//	AppendResponseSuccessPosition
	//	AppendResponseSuccessNoPosition
	Position isAppendResponseSuccessPosition
}

func (this AppendResponseSuccess) isAppendResponseResult() {
}

func (this AppendResponseSuccess) GetCurrentRevisionNoStream() bool {
	if _, ok := this.CurrentRevision.(AppendResponseSuccessCurrentRevisionNoStream); ok {
		return true
	}
	return false
}

func (this AppendResponseSuccess) GetCurrentRevision() uint64 {
	if revision, ok := this.CurrentRevision.(AppendResponseSuccessCurrentRevision); ok {
		return revision.CurrentRevision
	}
	return 0
}

func (this AppendResponseSuccess) GetPosition() (AppendResponseSuccessPosition, bool) {
	if revision, ok := this.Position.(AppendResponseSuccessPosition); ok {
		return revision, true
	}
	return AppendResponseSuccessPosition{}, false
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

type AppendResponseWrongExpectedVersion struct {
	// Types that are assignable to CurrentRevisionOption_20_6_0:
	//	AppendResponseWrongCurrentRevision_20_6_0
	//	AppendResponseWrongCurrentRevisionNoStream_20_6_0
	CurrentRevision_20_6_0 isAppendResponseWrongCurrentRevision_20_6_0
	// Types that are assignable to ExpectedRevisionOption_20_6_0:
	//	AppendResponseWrongExpectedRevision_20_6_0
	//	AppendResponseWrongExpectedRevisionAny_20_6_0
	//	AppendResponseWrongExpectedRevisionStreamExists_20_6_0
	ExpectedRevision_20_6_0 isAppendResponseWrongExpectedRevision_20_6_0
	// Types that are assignable to CurrentRevisionOption:
	//	AppendResponseWrongCurrentRevision
	//	AppendResponseWrongCurrentRevisionNoStream
	CurrentRevision isAppendResponseWrongCurrentRevision
	// Types that are assignable to ExpectedRevisionOption:
	//	AppendResponseWrongExpectedRevision
	//	AppendResponseWrongExpectedRevisionAny
	//	AppendResponseWrongExpectedRevisionStreamExists
	//	AppendResponseWrongExpectedRevisionNoStream
	ExpectedRevision isAppendResponseWrongExpectedRevision
}

func (this AppendResponseWrongExpectedVersion) isAppendResponseResult() {
}

func (this AppendResponseWrongExpectedVersion) GetExpectedRevision() (uint64, bool) {
	if revision, ok := this.ExpectedRevision.(AppendResponseWrongExpectedRevision); ok {
		return revision.ExpectedRevision, true
	} else if revision, ok := this.ExpectedRevision_20_6_0.(AppendResponseWrongExpectedRevision_20_6_0); ok {
		return revision.ExpectedRevision, true
	}

	return 0, false
}

func (this AppendResponseWrongExpectedVersion) IsExpectedRevisionNoStream() bool {
	if _, ok := this.ExpectedRevision.(AppendResponseWrongExpectedRevisionNoStream); ok {
		return true
	}

	return false
}

func (this AppendResponseWrongExpectedVersion) IsCurrentRevisionNoStream() bool {
	if _, ok := this.CurrentRevision.(AppendResponseWrongCurrentRevisionNoStream); ok {
		return true
	} else if _, ok := this.CurrentRevision_20_6_0.(AppendResponseWrongCurrentRevisionNoStream_20_6_0); ok {
		return true
	}

	return false
}

func (this AppendResponseWrongExpectedVersion) GetCurrentRevision() (uint64, bool) {
	if revision, ok := this.CurrentRevision.(AppendResponseWrongCurrentRevision); ok {
		return revision.CurrentRevision, true
	} else if revision, ok := this.CurrentRevision_20_6_0.(AppendResponseWrongCurrentRevision_20_6_0); ok {
		return revision.CurrentRevision, true
	}

	return 0, false
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
	CreateResponse(protoResponse *streams2.AppendResp) AppendResponse
}

type appendResponseAdapterImpl struct{}

func (this appendResponseAdapterImpl) CreateResponse(protoResponse *streams2.AppendResp) AppendResponse {
	result := AppendResponse{}

	switch protoResponse.Result.(type) {
	case *streams2.AppendResp_WrongExpectedVersion_:
		wrongExpectedVersionProto := protoResponse.Result.(*streams2.AppendResp_WrongExpectedVersion_)
		result.Result = this.buildWrongExpectedVersionResponse(wrongExpectedVersionProto)
	case *streams2.AppendResp_Success_:
		successProtoResult := protoResponse.Result.(*streams2.AppendResp_Success_)
		result.Result = this.buildSuccessResponse(successProtoResult)
	}

	return result
}

func (this appendResponseAdapterImpl) buildSuccessResponse(
	protoSuccessResult *streams2.AppendResp_Success_) isAppendResponseResult {
	result := AppendResponseSuccess{}

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

func (this appendResponseAdapterImpl) buildWrongExpectedVersionResponse(
	proto *streams2.AppendResp_WrongExpectedVersion_) isAppendResponseResult {
	result := AppendResponseWrongExpectedVersion{}

	switch proto.WrongExpectedVersion.CurrentRevisionOption_20_6_0.(type) {
	case *streams2.AppendResp_WrongExpectedVersion_CurrentRevision_20_6_0:
		protoWrongCurrentRevision := proto.WrongExpectedVersion.CurrentRevisionOption_20_6_0.(*streams2.AppendResp_WrongExpectedVersion_CurrentRevision_20_6_0)
		result.CurrentRevision_20_6_0 = AppendResponseWrongCurrentRevision_20_6_0{
			CurrentRevision: protoWrongCurrentRevision.CurrentRevision_20_6_0,
		}
	case *streams2.AppendResp_WrongExpectedVersion_NoStream_20_6_0:
		result.CurrentRevision_20_6_0 = AppendResponseWrongCurrentRevisionNoStream_20_6_0{}
	}

	switch proto.WrongExpectedVersion.ExpectedRevisionOption_20_6_0.(type) {
	case *streams2.AppendResp_WrongExpectedVersion_ExpectedRevision_20_6_0:
		protoExpectedRevision_20_6_0 := proto.WrongExpectedVersion.ExpectedRevisionOption_20_6_0.(*streams2.AppendResp_WrongExpectedVersion_ExpectedRevision_20_6_0)
		result.ExpectedRevision_20_6_0 = AppendResponseWrongExpectedRevision_20_6_0{
			ExpectedRevision: protoExpectedRevision_20_6_0.ExpectedRevision_20_6_0,
		}
	case *streams2.AppendResp_WrongExpectedVersion_Any_20_6_0:
		result.ExpectedRevision_20_6_0 = AppendResponseWrongExpectedRevisionAny_20_6_0{}
	case *streams2.AppendResp_WrongExpectedVersion_StreamExists_20_6_0:
		result.ExpectedRevision_20_6_0 = AppendResponseWrongExpectedRevisionStreamExists_20_6_0{}
	}

	switch proto.WrongExpectedVersion.CurrentRevisionOption.(type) {
	case *streams2.AppendResp_WrongExpectedVersion_CurrentRevision:
		protoCurrentRevision := proto.WrongExpectedVersion.CurrentRevisionOption.(*streams2.AppendResp_WrongExpectedVersion_CurrentRevision)
		result.CurrentRevision = AppendResponseWrongCurrentRevision{
			CurrentRevision: protoCurrentRevision.CurrentRevision,
		}
	case *streams2.AppendResp_WrongExpectedVersion_CurrentNoStream:
		result.CurrentRevision = AppendResponseWrongCurrentRevisionNoStream{}
	}

	switch proto.WrongExpectedVersion.ExpectedRevisionOption.(type) {
	case *streams2.AppendResp_WrongExpectedVersion_ExpectedRevision:
		protoExpectedVersion := proto.WrongExpectedVersion.ExpectedRevisionOption.(*streams2.AppendResp_WrongExpectedVersion_ExpectedRevision)
		result.ExpectedRevision = AppendResponseWrongExpectedRevision{
			ExpectedRevision: protoExpectedVersion.ExpectedRevision,
		}
	case *streams2.AppendResp_WrongExpectedVersion_ExpectedAny:
		result.ExpectedRevision = AppendResponseWrongExpectedRevisionAny{}
	case *streams2.AppendResp_WrongExpectedVersion_ExpectedStreamExists:
		result.ExpectedRevision = AppendResponseWrongExpectedRevisionStreamExists{}
	case *streams2.AppendResp_WrongExpectedVersion_ExpectedNoStream:
		result.ExpectedRevision = AppendResponseWrongExpectedRevisionNoStream{}
	}

	return result
}
