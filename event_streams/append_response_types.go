package event_streams

import (
	"github.com/pivonroll/EventStore-Client-Go/errors"
	"github.com/pivonroll/EventStore-Client-Go/protos/streams2"
)

// AppendResponse is returned from Client.AppendToStream when events are written successfully.
type AppendResponse struct {
	currentRevision isAppendResponseSuccessCurrentRevision
	position        isAppendResponseSuccessPosition
}

// IsCurrentRevisionNoStream returns true if current revision in append response was set to NoStream.
// Current revision in response can be NoStream if no events are appended to a non-existing stream.
func (this AppendResponse) IsCurrentRevisionNoStream() bool {
	if _, ok := this.currentRevision.(appendResponseSuccessCurrentRevisionNoStream); ok {
		return true
	}
	return false
}

// GetCurrentRevision returns stream's current revision if current revision is not NoStream.
// If currentRevision is NoStream, it returns 0.
// Note that stream can have a valid revision 0 if it contains only one event.
// Use IsCurrentRevisionNoStream to check if current revision of a stream is NoStream.
func (this AppendResponse) GetCurrentRevision() uint64 {
	if revision, ok := this.currentRevision.(appendResponseSuccessCurrentRevision); ok {
		return revision.CurrentRevision
	}
	return 0
}

// GetPosition returns a position of last appended event in a stream and a boolean value
// which indicates if position for last written event was received.
// If no position was received a zero initialized Position and a false will be returned.
func (this AppendResponse) GetPosition() (Position, bool) {
	if revision, ok := this.position.(appendResponseSuccessPosition); ok {
		return Position{
			CommitPosition:  revision.commitPosition,
			PreparePosition: revision.preparePosition,
		}, true
	}
	return Position{}, false
}

type isAppendResponseSuccessPosition interface {
	isAppendResponseSuccessPosition()
}

type appendResponseSuccessPosition struct {
	commitPosition  uint64
	preparePosition uint64
}

func (this appendResponseSuccessPosition) isAppendResponseSuccessPosition() {
}

type appendResponseSuccessNoPosition struct{}

func (this appendResponseSuccessNoPosition) isAppendResponseSuccessPosition() {
}

type isAppendResponseSuccessCurrentRevision interface {
	isAppendResponseSuccessCurrentRevision()
}

type appendResponseSuccessCurrentRevision struct {
	CurrentRevision uint64
}

func (this appendResponseSuccessCurrentRevision) isAppendResponseSuccessCurrentRevision() {
}

type appendResponseSuccessCurrentRevisionNoStream struct{}

func (this appendResponseSuccessCurrentRevisionNoStream) isAppendResponseSuccessCurrentRevision() {
}

type isAppendResponseWrongCurrentRevision2060 interface {
	isAppendResponseWrongCurrentRevision2060()
}

type appendResponseWrongCurrentRevision2060 struct {
	currentRevision uint64
}

func (this appendResponseWrongCurrentRevision2060) isAppendResponseWrongCurrentRevision2060() {
}

type appendResponseWrongCurrentRevisionNoStream2060 struct{}

func (this appendResponseWrongCurrentRevisionNoStream2060) isAppendResponseWrongCurrentRevision2060() {
}

type isAppendResponseWrongExpectedRevision2060 interface {
	isAppendResponseWrongExpectedRevision2060()
}

type appendResponseWrongExpectedRevision2060 struct {
	expectedRevision uint64
}

func (this appendResponseWrongExpectedRevision2060) isAppendResponseWrongExpectedRevision2060() {
}

type appendResponseWrongExpectedRevisionAny2060 struct{}

func (this appendResponseWrongExpectedRevisionAny2060) isAppendResponseWrongExpectedRevision2060() {
}

type appendResponseWrongExpectedRevisionStreamExists2060 struct{}

func (this appendResponseWrongExpectedRevisionStreamExists2060) isAppendResponseWrongExpectedRevision2060() {
}

type isAppendResponseWrongCurrentRevision interface {
	isAppendResponseCurrentRevision()
}

type appendResponseWrongCurrentRevision struct {
	currentRevision uint64
}

func (this appendResponseWrongCurrentRevision) isAppendResponseCurrentRevision() {
}

type appendResponseWrongCurrentRevisionNoStream struct{}

func (this appendResponseWrongCurrentRevisionNoStream) isAppendResponseCurrentRevision() {
}

type isAppendResponseWrongExpectedRevision interface {
	isAppendResponseWrongExpectedRevision()
}

type appendResponseWrongExpectedRevision struct {
	expectedRevision uint64
}

func (this appendResponseWrongExpectedRevision) isAppendResponseWrongExpectedRevision() {
}

type appendResponseWrongExpectedRevisionAny struct{}

func (this appendResponseWrongExpectedRevisionAny) isAppendResponseWrongExpectedRevision() {
}

type appendResponseWrongExpectedRevisionStreamExists struct{}

func (this appendResponseWrongExpectedRevisionStreamExists) isAppendResponseWrongExpectedRevision() {
}

type appendResponseWrongExpectedRevisionNoStream struct{}

func (this appendResponseWrongExpectedRevisionNoStream) isAppendResponseWrongExpectedRevision() {
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
		result.currentRevision = appendResponseSuccessCurrentRevision{
			CurrentRevision: protoCurrentRevision.CurrentRevision,
		}

	case *streams2.AppendResp_Success_NoStream:
		result.currentRevision = appendResponseSuccessCurrentRevisionNoStream{}
	}

	switch protoSuccessResult.Success.PositionOption.(type) {
	case *streams2.AppendResp_Success_Position:
		protoPosition := protoSuccessResult.Success.PositionOption.(*streams2.AppendResp_Success_Position)
		result.position = appendResponseSuccessPosition{
			commitPosition:  protoPosition.Position.CommitPosition,
			preparePosition: protoPosition.Position.PreparePosition,
		}
	case *streams2.AppendResp_Success_NoPosition:
		result.position = appendResponseSuccessNoPosition{}
	}

	return result
}

func (this appendResponseAdapterImpl) buildWrongExpectedVersionError(
	proto *streams2.AppendResp_WrongExpectedVersion_) WrongExpectedVersion {
	result := newWrongExpectedVersionError()

	result.currentRevision2060 = this.buildCurrentRevision2060(proto.WrongExpectedVersion)
	result.expectedRevision2060 = this.buildExpectedRevision2060(proto.WrongExpectedVersion)
	result.currentRevision = this.buildCurrentRevision(proto.WrongExpectedVersion)
	result.expectedRevision = this.buildExpectedRevision(proto.WrongExpectedVersion)

	return result
}

func (this appendResponseAdapterImpl) buildCurrentRevision2060(
	proto *streams2.AppendResp_WrongExpectedVersion) isAppendResponseWrongCurrentRevision2060 {

	if proto.CurrentRevisionOption_20_6_0 != nil {
		switch proto.CurrentRevisionOption_20_6_0.(type) {
		case *streams2.AppendResp_WrongExpectedVersion_CurrentRevision_20_6_0:
			protoWrongCurrentRevision := proto.CurrentRevisionOption_20_6_0.(*streams2.AppendResp_WrongExpectedVersion_CurrentRevision_20_6_0)
			return appendResponseWrongCurrentRevision2060{
				currentRevision: protoWrongCurrentRevision.CurrentRevision_20_6_0,
			}
		case *streams2.AppendResp_WrongExpectedVersion_NoStream_20_6_0:
			return appendResponseWrongCurrentRevisionNoStream2060{}
		}
	}

	return nil
}

func (this appendResponseAdapterImpl) buildExpectedRevision2060(
	proto *streams2.AppendResp_WrongExpectedVersion) isAppendResponseWrongExpectedRevision2060 {

	if proto.ExpectedRevisionOption_20_6_0 != nil {
		switch proto.ExpectedRevisionOption_20_6_0.(type) {
		case *streams2.AppendResp_WrongExpectedVersion_ExpectedRevision_20_6_0:
			protoExpectedRevision2060 := proto.ExpectedRevisionOption_20_6_0.(*streams2.AppendResp_WrongExpectedVersion_ExpectedRevision_20_6_0)
			return appendResponseWrongExpectedRevision2060{
				expectedRevision: protoExpectedRevision2060.ExpectedRevision_20_6_0,
			}
		case *streams2.AppendResp_WrongExpectedVersion_Any_20_6_0:
			return appendResponseWrongExpectedRevisionAny2060{}
		case *streams2.AppendResp_WrongExpectedVersion_StreamExists_20_6_0:
			return appendResponseWrongExpectedRevisionStreamExists2060{}
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
			return appendResponseWrongCurrentRevision{
				currentRevision: protoWrongCurrentRevision.CurrentRevision,
			}
		case *streams2.AppendResp_WrongExpectedVersion_CurrentNoStream:
			return appendResponseWrongCurrentRevisionNoStream{}
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
			return appendResponseWrongExpectedRevision{
				expectedRevision: protoExpectedRevision.ExpectedRevision,
			}
		case *streams2.AppendResp_WrongExpectedVersion_ExpectedAny:
			return appendResponseWrongExpectedRevisionAny{}
		case *streams2.AppendResp_WrongExpectedVersion_ExpectedStreamExists:
			return appendResponseWrongExpectedRevisionStreamExists{}
		case *streams2.AppendResp_WrongExpectedVersion_ExpectedNoStream:
			return appendResponseWrongExpectedRevisionNoStream{}
		}
	}

	return nil
}
