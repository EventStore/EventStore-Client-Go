package esdb

import (
	"fmt"
	"log"
	"strings"
)

type LogLevel = string

const (
	LogDebug LogLevel = "debug"
	LogInfo  LogLevel = "info"
	LogWarn  LogLevel = "warn"
	LogError LogLevel = "error"
)

type LoggingFunc = func(level LogLevel, format string, args ...interface{})

func ConsoleLogging() LoggingFunc {
	return func(level LogLevel, format string, args ...interface{}) {
		scoped := fmt.Sprintf("[%s]", level)
		format = strings.Join([]string{scoped, format}, " ")
		log.Printf(format, args...)
	}
}

func NoopLogging() LoggingFunc {
	return func(scope string, format string, args ...interface{}) {

	}
}

type logger struct {
	callback LoggingFunc
}

func (log *logger) error(format string, args ...interface{}) {
	if log.callback != nil {
		log.callback(LogError, format, args...)
	}
}

func (log *logger) warn(format string, args ...interface{}) {
	if log.callback != nil {
		log.callback(LogWarn, format, args...)
	}
}

func (log *logger) debug(format string, args ...interface{}) {
	if log.callback != nil {
		log.callback(LogDebug, format, args...)
	}
}

func (log *logger) info(format string, args ...interface{}) {
	if log.callback != nil {
		log.callback(LogInfo, format, args...)
	}
}
