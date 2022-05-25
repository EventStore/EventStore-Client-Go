package esdb

import (
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// EndPoint is database node endpoint.
type EndPoint struct {
	// Endpoint's hostname.
	Host string
	// Endpoint's port.
	Port uint16
}

// String Endpoint string representation.
func (e *EndPoint) String() string {
	return fmt.Sprintf("%s:%d", e.Host, e.Port)
}

// ParseEndPoint parses an endpoint string representation. For example: "{hostname}:{port}"
func ParseEndPoint(s string) (*EndPoint, error) {
	if strings.TrimSpace(s) == "" {
		return nil, fmt.Errorf("an empty host is specified")
	}

	endpoint := &EndPoint{}
	if strings.Contains(s, ":") {
		tokens := strings.Split(s, ":")
		if len(tokens) != 2 {
			return nil, fmt.Errorf("too many colons specified in host, expecting {host}:{port}")
		}

		ip := net.ParseIP(tokens[0])
		if ip == nil {
			_, err := url.Parse(tokens[0])

			if err != nil {
				return nil, fmt.Errorf("invalid hostname [%s]", tokens[0])
			}
		}

		port, err := strconv.Atoi(tokens[1])
		if err != nil || !(port >= 1 && port <= 65_535) {
			return nil, fmt.Errorf("invalid port specified, expecting an integer value [%s]", tokens[1])
		}

		endpoint.Host = tokens[0]
		endpoint.Port = uint16(port)
	} else {
		ip := net.ParseIP(s)
		if ip == nil {
			_, err := url.Parse(s)

			if err != nil {
				return nil, fmt.Errorf("invalid hostname [%s]", s)
			}
		}

		endpoint.Host = s
		endpoint.Port = 2_113
	}

	return endpoint, nil
}

func newGrpcClient(config Configuration) *grpcClient {
	channel := make(chan msg)
	closeFlag := new(int32)
	logger := logger{
		callback: config.Logger,
	}

	atomic.StoreInt32(closeFlag, 0)

	go connectionStateMachine(config, closeFlag, channel, &logger)

	return &grpcClient{
		channel:   channel,
		closeFlag: closeFlag,
		once:      new(sync.Once),
		logger:    &logger,
	}
}
