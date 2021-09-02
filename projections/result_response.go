package projections

import (
	"encoding/json"

	"github.com/EventStore/EventStore-Client-Go/protos/projections"
	"google.golang.org/protobuf/types/known/structpb"
)

type ResultResponseType string

const (
	ResultResponseNullType   ResultResponseType = "ResultResponseNullType"
	ResultResponseNumberType ResultResponseType = "ResultResponseNumberType"
	ResultResponseStringType ResultResponseType = "ResultResponseStringType"
	ResultResponseBoolType   ResultResponseType = "ResultResponseBoolType"
	ResultResponseStructType ResultResponseType = "ResultResponseStructType"
	ResultResponseListType   ResultResponseType = "ResultResponseListType"
)

type ResultResponse interface {
	GetType() ResultResponseType
}

type ResultResponseBool struct {
	value bool
}

func (s *ResultResponseBool) GetType() ResultResponseType {
	return ResultResponseBoolType
}

func (s *ResultResponseBool) Value() bool {
	return s.value
}

type ResultResponseNull struct{}

func (s *ResultResponseNull) GetType() ResultResponseType {
	return ResultResponseNullType
}

type ResultResponseNumber struct {
	value float64
}

func (s *ResultResponseNumber) GetType() ResultResponseType {
	return ResultResponseNumberType
}

func (s *ResultResponseNumber) Value() float64 {
	return s.value
}

type ResultResponseString struct {
	value string
}

func (s *ResultResponseString) GetType() ResultResponseType {
	return ResultResponseStringType
}

func (s *ResultResponseString) Value() string {
	return s.value
}

type ResultResponseStruct struct {
	value JSONBytes
}

func (s *ResultResponseStruct) GetType() ResultResponseType {
	return ResultResponseStructType
}

func (s *ResultResponseStruct) Value() JSONBytes {
	return s.value
}

type ResultResponseList struct {
	value []JSONBytes
}

func (s *ResultResponseList) GetType() ResultResponseType {
	return ResultResponseListType
}

func (s *ResultResponseList) Value() []JSONBytes {
	return s.value
}

func newResultResponse(state *projections.ResultResp) ResultResponse {
	_, ok := state.Result.Kind.(*structpb.Value_NullValue)

	if ok {
		return &ResultResponseNull{}
	}

	numberValue, ok := state.Result.Kind.(*structpb.Value_NumberValue)

	if ok {
		return &ResultResponseNumber{
			value: numberValue.NumberValue,
		}
	}

	stringValue, ok := state.Result.Kind.(*structpb.Value_StringValue)

	if ok {
		return &ResultResponseString{
			value: stringValue.StringValue,
		}
	}

	boolValue, ok := state.Result.Kind.(*structpb.Value_BoolValue)

	if ok {
		return &ResultResponseBool{
			value: boolValue.BoolValue,
		}
	}

	structValue, ok := state.Result.Kind.(*structpb.Value_StructValue)

	if ok {
		return &ResultResponseStruct{
			value: newResultResponseStructMap(structValue.StructValue.Fields),
		}
	}

	listValue, ok := state.Result.Kind.(*structpb.Value_ListValue)

	if ok {
		return &ResultResponseList{
			value: newResultResponseList(listValue.ListValue.Values),
		}
	}

	return nil
}

func newResultResponseStructMap(grpcValue map[string]*structpb.Value) JSONBytes {
	jsonValue, err := json.Marshal(grpcValue)
	if err != nil {
		panic(err)
	}

	return jsonValue
}

func newResultResponseList(grpcValue []*structpb.Value) []JSONBytes {
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
