package projections

import (
	"encoding/json"

	"github.com/pivonroll/EventStore-Client-Go/protos/projections"
	"google.golang.org/protobuf/types/known/structpb"
)

type StateResponseType string

const (
	StateResponseNullType   StateResponseType = "StateResponseNullType"
	StateResponseNumberType StateResponseType = "StateResponseNumberType"
	StateResponseStringType StateResponseType = "StateResponseStringType"
	StateResponseBoolType   StateResponseType = "StateResponseBoolType"
	StateResponseStructType StateResponseType = "StateResponseStructType"
	StateResponseListType   StateResponseType = "StateResponseListType"
)

type StateResponse interface {
	GetType() StateResponseType
}

type StateResponseBool struct {
	value bool
}

func (s *StateResponseBool) GetType() StateResponseType {
	return StateResponseBoolType
}

func (s *StateResponseBool) Value() bool {
	return s.value
}

type StateResponseNull struct{}

func (s *StateResponseNull) GetType() StateResponseType {
	return StateResponseNullType
}

type StateResponseNumber struct {
	value float64
}

func (s *StateResponseNumber) GetType() StateResponseType {
	return StateResponseNumberType
}

func (s *StateResponseNumber) Value() float64 {
	return s.value
}

const (
	StateAbortedOrStopped = "Aborted/Stopped"
	StateRunning          = "Running"
	StateStopped          = "Stopped"
)

type StateResponseString struct {
	value string
}

func (s *StateResponseString) GetType() StateResponseType {
	return StateResponseStringType
}

func (s *StateResponseString) Value() string {
	return s.value
}

type StateResponseStruct struct {
	value JSONBytes
}

func (s *StateResponseStruct) GetType() StateResponseType {
	return StateResponseStructType
}

func (s *StateResponseStruct) Value() JSONBytes {
	return s.value
}

type StateResponseList struct {
	value []JSONBytes
}

func (s *StateResponseList) GetType() StateResponseType {
	return StateResponseListType
}

func (s *StateResponseList) Value() []JSONBytes {
	return s.value
}

func newStateResponse(state *projections.StateResp) StateResponse {
	_, ok := state.State.Kind.(*structpb.Value_NullValue)

	if ok {
		return &StateResponseNull{}
	}

	numberValue, ok := state.State.Kind.(*structpb.Value_NumberValue)

	if ok {
		return &StateResponseNumber{
			value: numberValue.NumberValue,
		}
	}

	stringValue, ok := state.State.Kind.(*structpb.Value_StringValue)

	if ok {
		return &StateResponseString{
			value: stringValue.StringValue,
		}
	}

	boolValue, ok := state.State.Kind.(*structpb.Value_BoolValue)

	if ok {
		return &StateResponseBool{
			value: boolValue.BoolValue,
		}
	}

	structValue, ok := state.State.Kind.(*structpb.Value_StructValue)

	if ok {
		return &StateResponseStruct{
			value: newStateResponseStructMap(structValue.StructValue.Fields),
		}
	}

	listValue, ok := state.State.Kind.(*structpb.Value_ListValue)

	if ok {
		return &StateResponseList{
			value: newStateResponseList(listValue.ListValue.Values),
		}
	}

	return nil
}

func newStateResponseStructMap(grpcValue map[string]*structpb.Value) JSONBytes {
	result, err := json.Marshal(grpcValue)
	if err != nil {
		panic(err)
	}

	return result
}

func newStateResponseList(grpcValue []*structpb.Value) []JSONBytes {
	var result []JSONBytes

	for _, value := range grpcValue {
		byteValue, err := value.MarshalJSON()
		if err != nil {
			panic(err)
		}
		result = append(result, byteValue)
	}

	return result
}
