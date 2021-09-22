package ptr

import "time"

func Int(value int) *int {
	return &value
}

func Int32(value int32) *int32 {
	return &value
}

func Int64(value int64) *int64 {
	return &value
}

func UInt(value uint) *uint {
	return &value
}

func UInt32(value uint32) *uint32 {
	return &value
}

func UInt64(value uint64) *uint64 {
	return &value
}

func Float32(value float32) *float32 {
	return &value
}

func Float64(value float64) *float64 {
	return &value
}

func String(value string) *string {
	return &value
}

func TimeDuration(value time.Duration) *time.Duration {
	return &value
}
